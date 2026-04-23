from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter

from .breakout import breakout_family_adapter, breakout_strategy_spec
from .ma_cross import ma_cross_family_adapter, ma_cross_strategy_spec
from .mean_reversion import mean_reversion_family_adapter, mean_reversion_strategy_spec
from .mtf_pullback import mtf_pullback_family_adapter, mtf_pullback_strategy_spec
from .squeeze_release import squeeze_release_family_adapter, squeeze_release_strategy_spec


def phase_stg02_family_adapters() -> tuple[StrategyFamilyAdapter, ...]:
    return (
        ma_cross_family_adapter(),
        breakout_family_adapter(),
        mean_reversion_family_adapter(),
        mtf_pullback_family_adapter(),
        squeeze_release_family_adapter(),
    )

__all__ = [
    "breakout_family_adapter",
    "breakout_strategy_spec",
    "ma_cross_family_adapter",
    "ma_cross_strategy_spec",
    "mean_reversion_family_adapter",
    "mean_reversion_strategy_spec",
    "mtf_pullback_family_adapter",
    "mtf_pullback_strategy_spec",
    "phase_stg02_family_adapters",
    "squeeze_release_family_adapter",
    "squeeze_release_strategy_spec",
]
