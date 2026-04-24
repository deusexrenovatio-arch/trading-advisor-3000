from __future__ import annotations

from dataclasses import dataclass

from .families import phase_stg02_family_adapters
from .spec import StrategySpec


@dataclass(frozen=True)
class StrategyCatalog:
    version: str
    strategies: tuple[StrategySpec, ...]


def default_strategy_catalog() -> StrategyCatalog:
    adapters = phase_stg02_family_adapters()
    return StrategyCatalog(
        version="research-strategy-catalog-v1",
        strategies=tuple(adapter.strategy_spec for adapter in adapters),
    )
