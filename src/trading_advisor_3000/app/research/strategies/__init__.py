from __future__ import annotations

from trading_advisor_3000.app.contracts import TradeSide

from .production import (
    PHASE9_FEATURE_SET_VERSION,
    PHASE9_PRODUCTION_STRATEGY_ID,
    ProductionStrategySpec,
    StrategyRiskTemplate,
    assess_phase9_production_pilot_readiness,
    evaluate_production_strategy,
    phase9_production_backtest_config,
    phase9_production_strategy_spec,
    production_strategy_ids,
)
from .sample import evaluate_strategy as evaluate_sample_strategy
from .sample import sample_strategy_ids


def evaluate_strategy(*, strategy_version_id: str, snapshot) -> TradeSide:
    if strategy_version_id in sample_strategy_ids():
        return evaluate_sample_strategy(strategy_version_id=strategy_version_id, snapshot=snapshot)
    if strategy_version_id in production_strategy_ids():
        return evaluate_production_strategy(strategy_version_id=strategy_version_id, snapshot=snapshot)
    raise ValueError(f"unsupported strategy_version_id: {strategy_version_id}")


__all__ = [
    "PHASE9_FEATURE_SET_VERSION",
    "PHASE9_PRODUCTION_STRATEGY_ID",
    "ProductionStrategySpec",
    "StrategyRiskTemplate",
    "assess_phase9_production_pilot_readiness",
    "evaluate_strategy",
    "phase9_production_backtest_config",
    "phase9_production_strategy_spec",
    "production_strategy_ids",
    "sample_strategy_ids",
]
