from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import StrategyParameter, StrategySpec


def breakout_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="breakout-v1",
        family="breakout",
        required_columns=("rolling_high_20", "rolling_low_20", "adx_14", "breakout_ready_flag"),
        parameter_grid=(
            StrategyParameter("breakout_window", (20, 30, 55)),
            StrategyParameter("min_adx", (18, 22, 25)),
        ),
    )

