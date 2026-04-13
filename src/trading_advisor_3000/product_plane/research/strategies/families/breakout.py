from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def breakout_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="breakout-v1",
        family="breakout",
        description="Breakout continuation strategy over prebuilt range and breakout readiness features.",
        required_columns=(
            "close",
            "high",
            "low",
            "rolling_high_20",
            "rolling_low_20",
            "adx_14",
            "atr_14",
            "breakout_ready_state_code",
            "trend_state_fast_slow_code",
        ),
        parameter_grid=(
            StrategyParameter("breakout_window", (20, 30, 55)),
            StrategyParameter("min_adx", (18, 22, 25)),
            StrategyParameter("entry_buffer_atr", (0.0, 0.25)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.25, target_atr_multiple=2.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "profit_factor", "max_drawdown"),
            tags=("breakout", "signals"),
        ),
    )
