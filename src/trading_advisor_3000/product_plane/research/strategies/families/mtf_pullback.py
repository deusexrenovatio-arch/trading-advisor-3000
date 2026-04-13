from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def mtf_pullback_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="mtf-pullback-v1",
        family="mtf_pullback",
        description="Higher-timeframe pullback continuation strategy using MTF alignment overlays.",
        required_columns=(
            "close",
            "atr_14",
            "ema_20",
            "ema_50",
            "distance_to_session_vwap",
            "trend_state_fast_slow_code",
            "htf_trend_state_code",
            "htf_rsi_14",
        ),
        parameter_grid=(
            StrategyParameter("pullback_depth", (0.25, 0.5, 0.75)),
            StrategyParameter("confirmation_bars", (1, 2, 3)),
            StrategyParameter("min_htf_rsi", (50, 55)),
        ),
        signal_builder_key="mtf_pullback",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.1, target_atr_multiple=2.2),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "calmar", "sharpe"),
            tags=("mtf", "signals"),
        ),
    )
