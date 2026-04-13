from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import StrategyParameter, StrategySpec


def ma_cross_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="ma-cross-v1",
        family="ma_cross",
        required_columns=("ema_10", "ema_20", "ema_50", "trend_state_code"),
        parameter_grid=(
            StrategyParameter("fast_window", (10, 20)),
            StrategyParameter("slow_window", (50, 100)),
        ),
    )

