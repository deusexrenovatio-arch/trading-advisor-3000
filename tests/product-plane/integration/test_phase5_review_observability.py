from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.runtime.analytics import run_system_shadow_replay
from trading_advisor_3000.product_plane.runtime.analytics.review import (
    build_loki_event_lines,
    build_phase5_review_report,
    export_prometheus_metrics,
)


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "research" / "canonical_bars_sample.jsonl"


def _load_bars(path: Path) -> list[CanonicalBar]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [CanonicalBar.from_dict(row) for row in rows]


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_phase5_replay_exports_review_and_observability_artifacts(tmp_path: Path) -> None:
    bars = _load_bars(SOURCE_FIXTURE)
    report = run_system_shadow_replay(
        bars=bars,
        instrument_by_contract={"BR-6.26": "BR", "Si-6.26": "Si"},
        strategy_version_id="trend-follow-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path,
        telegram_channel="@ta3000_signals",
        horizon_bars=2,
        runtime_allowed_contracts={"BR-6.26"},
    )

    assert report["runtime_signal_candidates"] > 0
    assert report["phase5_report"]["summary"]["strategy_rows"] == len(report["strategy_rows"])
    assert report["phase5_report"]["summary"]["instrument_rows"] == len(report["instrument_rows"])
    assert report["phase5_report"]["summary"]["latency_rows"] == len(report["latency_rows"])
    assert report["phase5_report"]["summary"]["latency_status_counts"] == {"ok": report["runtime_signal_candidates"]}

    required_delta_keys = {
        "analytics_strategy_metrics_daily",
        "analytics_instrument_metrics_daily",
        "observability_latency_metrics",
    }
    assert required_delta_keys <= set(report["delta_manifest"])

    required_output_paths = {
        "analytics_strategy_metrics_daily",
        "analytics_instrument_metrics_daily",
        "observability_latency_metrics",
        "observability_prometheus_metrics",
        "observability_loki_events",
    }
    for key in required_output_paths:
        path = Path(str(report["output_paths"][key]))
        assert path.exists()

    prometheus_text = Path(str(report["output_paths"]["observability_prometheus_metrics"])).read_text(
        encoding="utf-8"
    )
    assert "ta3000_strategy_signals_total" in prometheus_text
    assert "ta3000_latency_quantile_ms" in prometheus_text

    loki_rows = _load_jsonl(Path(str(report["output_paths"]["observability_loki_events"])))
    assert {str(row["stream"]) for row in loki_rows} >= {"latency", "strategy_dashboard"}


def test_phase5_non_happy_latency_status_is_visible_in_metrics_and_logs(tmp_path: Path) -> None:
    bars = _load_bars(SOURCE_FIXTURE)
    report = run_system_shadow_replay(
        bars=bars,
        instrument_by_contract={"BR-6.26": "BR", "Si-6.26": "Si"},
        strategy_version_id="trend-follow-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path,
        telegram_channel="@ta3000_signals",
        horizon_bars=2,
        runtime_allowed_contracts={"BR-6.26"},
    )

    assert report["runtime_signal_ids"]
    degraded_signal = str(report["runtime_signal_ids"][0])
    degraded_events = [
        row
        for row in report["signal_events"]
        if not (
            str(row.get("signal_id")) == degraded_signal
            and str(row.get("event_type")) == "signal_activated"
        )
    ]

    degraded_report = build_phase5_review_report(
        outcomes=report["analytics_rows"],
        signal_events=degraded_events,
    )
    status_by_signal = {row.signal_id: row.latency_status for row in degraded_report.latency_metrics}
    assert status_by_signal[degraded_signal] == "missing_activation"

    prometheus_text = export_prometheus_metrics(degraded_report)
    assert 'status="missing_activation"' in prometheus_text

    loki_lines = [json.loads(line) for line in build_loki_event_lines(degraded_report)]
    assert any(
        row.get("stream") == "latency"
        and row.get("signal_id") == degraded_signal
        and row.get("status") == "missing_activation"
        for row in loki_lines
    )
