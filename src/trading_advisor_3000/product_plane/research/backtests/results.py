from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BacktestBatchArtifact:
    backtest_batch_id: str
    dataset_version: str
    strategy_catalog_version: str
    combination_count: int


@dataclass(frozen=True)
class BacktestRunArtifact:
    backtest_run_id: str
    backtest_batch_id: str
    strategy_version: str
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str

