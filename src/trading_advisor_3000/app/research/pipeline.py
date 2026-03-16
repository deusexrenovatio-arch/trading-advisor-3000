from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.app.contracts import CanonicalBar

from .backtest import run_backtest
from .features import build_feature_snapshots


def run_research_from_bars(
    *,
    bars: list[CanonicalBar],
    instrument_by_contract: dict[str, str],
    strategy_version_id: str,
    dataset_version: str,
    output_dir: Path,
) -> dict[str, object]:
    snapshots = build_feature_snapshots(
        bars,
        feature_set_version="feature-set-v1",
        instrument_by_contract=instrument_by_contract,
    )
    result = run_backtest(
        snapshots,
        strategy_version_id=strategy_version_id,
        dataset_version=dataset_version,
        output_dir=output_dir,
    )
    return {
        "bars_processed": len(bars),
        "feature_snapshots": len(snapshots),
        "signal_contracts": len(result["signal_contracts"]),
        "research_candidates": len(result["research_candidates"]),
        "backtest_run": result["backtest_run"],
        "delta_manifest": result["delta_manifest"],
        "output_paths": result["output_paths"],
        "signal_contract_rows": result["signal_contracts"],
    }
