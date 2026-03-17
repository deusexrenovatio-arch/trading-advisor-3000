from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.app.contracts import CanonicalBar
from trading_advisor_3000.app.runtime.analytics import run_system_shadow_replay


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "research" / "canonical_bars_sample.jsonl"


def _load_bars(path: Path) -> list[CanonicalBar]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [CanonicalBar.from_dict(row) for row in rows]


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_phase3_integrated_replay_produces_traceable_runtime_bound_outcomes(tmp_path: Path) -> None:
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

    assert report["signal_candidates"] > 0
    assert report["runtime_signal_candidates"] > 0
    assert report["runtime_signal_candidates"] < report["signal_candidates"]
    assert report["runtime_report"]["accepted_unique_signals"] == report["runtime_signal_candidates"]
    assert report["analytics_outcomes"] == report["forward_observations"]
    assert report["forward_observations"] == report["runtime_signal_candidates"]

    assert report["runtime_payload"]["publications"]
    assert all(row["mode"] == "shadow" for row in report["analytics_rows"])
    assert "analytics_signal_outcomes" in report["delta_manifest"]
    assert "research_forward_observations" in report["delta_manifest"]
    assert set(report["runtime_signal_ids"]) == {row["signal_id"] for row in report["runtime_payload"]["publications"]}

    research_rows = _load_jsonl(Path(str(report["output_paths"]["signal_candidates"])))
    forward_rows = _load_jsonl(Path(str(report["output_paths"]["research_forward_observations"])))
    research_candidate_ids_by_signal = {
        str(row["signal_contract_json"]["signal_id"]): str(row["candidate_id"]) for row in research_rows
    }
    expected_candidate_ids = {research_candidate_ids_by_signal[signal_id] for signal_id in report["runtime_signal_ids"]}
    actual_forward_candidate_ids = {str(row["candidate_id"]) for row in forward_rows}
    assert actual_forward_candidate_ids == expected_candidate_ids

    for path_text in report["output_paths"].values():
        assert Path(path_text).exists()
