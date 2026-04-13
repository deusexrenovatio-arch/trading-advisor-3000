from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import StrategyParameter, StrategySpec


def mtf_pullback_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="mtf-pullback-v1",
        family="mtf_pullback",
        required_columns=("htf_trend_state_code", "htf_rsi_14", "ema_20", "ema_50"),
        parameter_grid=(
            StrategyParameter("pullback_depth", (0.25, 0.5, 0.75)),
            StrategyParameter("confirmation_bars", (1, 2, 3)),
        ),
    )

