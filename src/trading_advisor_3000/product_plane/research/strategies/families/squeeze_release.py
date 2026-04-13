from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def squeeze_release_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="squeeze-release-v1",
        family="squeeze_release",
        description="Event-driven squeeze release strategy executed through vectorbt order callbacks.",
        required_columns=("close", "atr_14", "squeeze_on_code", "breakout_ready_state_code", "trend_state_fast_slow_code"),
        parameter_grid=(
            StrategyParameter("min_squeeze_bars", (3, 5, 8)),
            StrategyParameter("atr_target_multiple", (1.5, 2.0, 2.5)),
            StrategyParameter("release_confirmation", (1, 2)),
        ),
        signal_builder_key="squeeze_release",
        execution_mode="order_func",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("profit_factor", "calmar", "max_drawdown"),
            tags=("volatility", "order-func"),
        ),
    )
