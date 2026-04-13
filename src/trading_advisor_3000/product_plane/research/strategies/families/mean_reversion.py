from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def mean_reversion_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="mean-reversion-v1",
        family="mean_reversion",
        description="Mean-reversion strategy around session VWAP and RSI extremes.",
        required_columns=("close", "rsi_14", "atr_14", "distance_to_session_vwap", "regime_state_code"),
        parameter_grid=(
            StrategyParameter("entry_rsi", (20, 25, 30)),
            StrategyParameter("exit_rsi", (45, 50, 55)),
            StrategyParameter("entry_distance_atr", (0.5, 0.75, 1.0)),
        ),
        signal_builder_key="mean_reversion",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=0.9, target_atr_multiple=1.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("sortino", "profit_factor", "win_rate"),
            tags=("reversion", "signals"),
        ),
    )
