from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.runtime.analytics import run_system_shadow_replay
from trading_advisor_3000.product_plane.runtime.analytics.review import (
    build_loki_event_lines,
    build_review_observability_report,
    export_prometheus_metrics,
)


def _instrument_map() -> dict[str, str]:
    return {"BR-6.26": "BR", "Si-6.26": "Si"}


def _build_bars(*, bars_per_contract: int = 72) -> list[CanonicalBar]:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    specs = (
        ("BR-6.26", "BR", 82.0, 0.22),
        ("Si-6.26", "Si", 91_800.0, 55.0),
    )
    bars: list[CanonicalBar] = []
    for contract_id, instrument_id, base_close, step in specs:
        for index in range(bars_per_contract):
            ts = (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")
            if index < bars_per_contract // 3:
                close = base_close + (index * step)
            elif index < (2 * bars_per_contract) // 3:
                close = base_close + ((bars_per_contract // 3) * step) - ((index - (bars_per_contract // 3)) * step * 1.15)
            else:
                close = (
                    base_close
                    + ((bars_per_contract // 3) * step)
                    - ((bars_per_contract // 3) * step * 1.15)
                    + ((index - ((2 * bars_per_contract) // 3)) * step * 1.35)
                )
            open_price = close - (0.35 * step)
            high = max(open_price, close) + (0.75 * step)
            low = min(open_price, close) - (0.85 * step)
            volume = 1_000 + (index * 20) + (120 if index % 7 == 0 else 0)
            if instrument_id == "Si":
                volume += 300
            bars.append(
                CanonicalBar.from_dict(
                    {
                        "contract_id": contract_id,
                        "instrument_id": instrument_id,
                        "timeframe": "15m",
                        "ts": ts,
                        "open": round(open_price, 6),
                        "high": round(high, 6),
                        "low": round(low, 6),
                        "close": round(close, 6),
                        "volume": int(volume),
                        "open_interest": 20_000 + index,
                    }
                )
            )
    return sorted(bars, key=lambda item: (item.contract_id, item.timeframe.value, item.ts))


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_replay_exports_review_and_observability_artifacts(tmp_path: Path) -> None:
    bars = _build_bars()
    report = run_system_shadow_replay(
        bars=bars,
        instrument_by_contract=_instrument_map(),
        strategy_version_id="ma-cross-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path,
        telegram_channel="@ta3000_signals",
        horizon_bars=2,
        runtime_allowed_contracts={"BR-6.26"},
    )

    assert report["runtime_signal_candidates"] > 0
    assert report["review_report"]["summary"]["strategy_rows"] == len(report["strategy_rows"])
    assert report["review_report"]["summary"]["instrument_rows"] == len(report["instrument_rows"])
    assert report["review_report"]["summary"]["latency_rows"] == len(report["latency_rows"])
    assert report["review_report"]["summary"]["latency_status_counts"] == {"ok": report["runtime_signal_candidates"]}

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


def test_non_happy_latency_status_is_visible_in_metrics_and_logs(tmp_path: Path) -> None:
    bars = _build_bars()
    report = run_system_shadow_replay(
        bars=bars,
        instrument_by_contract=_instrument_map(),
        strategy_version_id="ma-cross-v1",
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

    degraded_report = build_review_observability_report(
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

