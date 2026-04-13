from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import StrategyParameter, StrategySpec


def squeeze_release_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="squeeze-release-v1",
        family="squeeze_release",
        required_columns=("squeeze_state_code", "kc_upper_20_1_5", "kc_lower_20_1_5", "atr_14"),
        parameter_grid=(
            StrategyParameter("min_squeeze_bars", (3, 5, 8)),
            StrategyParameter("atr_target_multiple", (1.5, 2.0, 2.5)),
        ),
        execution_mode="order_func",
    )

