from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def ma_cross_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="ma-cross-v1",
        family="ma_cross",
        description="Trend-following moving-average crossover on materialized indicators and trend states.",
        required_columns=("close", "ema_10", "ema_20", "ema_50", "atr_14", "trend_state_fast_slow_code", "ma_stack_state_code"),
        parameter_grid=(
            StrategyParameter("fast_window", (10, 20)),
            StrategyParameter("slow_window", (20, 50)),
        ),
        signal_builder_key="ma_cross",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "sharpe", "max_drawdown"),
            tags=("trend", "signals"),
        ),
    )
