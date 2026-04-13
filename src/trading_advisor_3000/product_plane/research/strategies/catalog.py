from __future__ import annotations

from dataclasses import dataclass

from .families.breakout import breakout_strategy_spec
from .families.ma_cross import ma_cross_strategy_spec
from .families.mean_reversion import mean_reversion_strategy_spec
from .families.mtf_pullback import mtf_pullback_strategy_spec
from .families.squeeze_release import squeeze_release_strategy_spec
from .spec import StrategySpec


@dataclass(frozen=True)
class StrategyCatalog:
    version: str
    strategies: tuple[StrategySpec, ...]


def phase1_strategy_catalog() -> StrategyCatalog:
    return StrategyCatalog(
        version="research-strategy-catalog-v1",
        strategies=(
            ma_cross_strategy_spec(),
            breakout_strategy_spec(),
            mean_reversion_strategy_spec(),
            mtf_pullback_strategy_spec(),
            squeeze_release_strategy_spec(),
        ),
    )
