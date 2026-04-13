from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import StrategyParameter, StrategySpec


def mean_reversion_strategy_spec() -> StrategySpec:
    return StrategySpec(
        version="mean-reversion-v1",
        family="mean_reversion",
        required_columns=("rsi_14", "distance_to_session_vwap", "reversion_ready_flag"),
        parameter_grid=(
            StrategyParameter("entry_rsi", (20, 25, 30)),
            StrategyParameter("exit_rsi", (45, 50, 55)),
        ),
    )

