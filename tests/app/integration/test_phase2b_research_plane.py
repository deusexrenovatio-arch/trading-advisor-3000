from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.app.contracts import CanonicalBar, DecisionCandidate
from trading_advisor_3000.app.research import run_research_from_bars


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "research" / "canonical_bars_sample.jsonl"


def _load_bars(path: Path) -> list[CanonicalBar]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [CanonicalBar.from_dict(row) for row in rows]


def test_phase2b_backtest_is_reproducible_and_writes_outputs(tmp_path: Path) -> None:
    bars = _load_bars(SOURCE_FIXTURE)
    instrument_map = {"BR-6.26": "BR", "Si-6.26": "Si"}

    run_a = run_research_from_bars(
        bars=bars,
        instrument_by_contract=instrument_map,
        strategy_version_id="trend-follow-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path / "run-a",
    )
    run_b = run_research_from_bars(
        bars=bars,
        instrument_by_contract=instrument_map,
        strategy_version_id="trend-follow-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path / "run-b",
    )

    assert run_a["backtest_run"]["backtest_run_id"] == run_b["backtest_run"]["backtest_run_id"]
    assert run_a["signal_contract_rows"] == run_b["signal_contract_rows"]
    assert run_a["delta_manifest"]["feature_snapshots"]["format"] == "delta"

    for path_text in run_a["output_paths"].values():
        assert Path(path_text).exists()

    assert run_a["signal_contracts"] > 0
    for row in run_a["signal_contract_rows"]:
        contract = DecisionCandidate.from_dict(row)
        assert contract.to_dict() == row
