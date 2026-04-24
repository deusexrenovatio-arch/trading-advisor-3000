from __future__ import annotations

from dataclasses import dataclass

from .catalog import StrategyCatalog, phase1_strategy_catalog
from .spec import StrategySpec


@dataclass(frozen=True)
class StrategyRegistry:
    catalog: StrategyCatalog

    def get(self, version: str) -> StrategySpec:
        for spec in self.catalog.strategies:
            if spec.version == version:
                return spec
        raise KeyError(f"unknown strategy version: {version}")

    def strategy_versions(self) -> tuple[str, ...]:
        return tuple(spec.version for spec in self.catalog.strategies)

    def parameter_combinations(self, version: str) -> tuple[dict[str, object], ...]:
        return self.get(version).parameter_combinations()

    def catalog_version(self) -> str:
        return self.catalog.version


def build_phase1_strategy_registry() -> StrategyRegistry:
    return StrategyRegistry(catalog=phase1_strategy_catalog())
