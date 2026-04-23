from __future__ import annotations

from dataclasses import dataclass, field
from itertools import product
from typing import Literal


StrategyExecutionMode = Literal["signals", "order_func"]
StrategyDirectionMode = Literal["long_only", "short_only", "long_short"]


@dataclass(frozen=True)
class StrategyParameter:
    name: str
    values: tuple[object, ...]

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("parameter name must be non-empty")
        if not self.values:
            raise ValueError("parameter values must not be empty")


@dataclass(frozen=True)
class StrategyRiskPolicy:
    stop_atr_multiple: float
    target_atr_multiple: float

    def __post_init__(self) -> None:
        if self.stop_atr_multiple <= 0:
            raise ValueError("stop_atr_multiple must be positive")
        if self.target_atr_multiple <= 0:
            raise ValueError("target_atr_multiple must be positive")


@dataclass(frozen=True)
class StrategyRankingMetadata:
    preferred_metrics: tuple[str, ...] = ("total_return", "sharpe", "max_drawdown")
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.preferred_metrics:
            raise ValueError("preferred_metrics must not be empty")


@dataclass(frozen=True)
class StrategySpec:
    version: str
    family: str
    description: str
    required_columns: tuple[str, ...]
    parameter_grid: tuple[StrategyParameter, ...]
    signal_builder_key: str
    execution_mode: StrategyExecutionMode = "signals"
    direction_mode: StrategyDirectionMode = "long_short"
    risk_policy: StrategyRiskPolicy = field(
        default_factory=lambda: StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0)
    )
    ranking_metadata: StrategyRankingMetadata = field(default_factory=StrategyRankingMetadata)

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("version must be non-empty")
        if not self.family.strip():
            raise ValueError("family must be non-empty")
        if not self.description.strip():
            raise ValueError("description must be non-empty")
        if not self.required_columns:
            raise ValueError("required_columns must not be empty")
        if not self.signal_builder_key.strip():
            raise ValueError("signal_builder_key must be non-empty")

    def parameter_names(self) -> tuple[str, ...]:
        return tuple(parameter.name for parameter in self.parameter_grid)

    def parameter_combinations(self) -> tuple[dict[str, object], ...]:
        if not self.parameter_grid:
            return ({},)
        names = self.parameter_names()
        value_lists = [parameter.values for parameter in self.parameter_grid]
        return tuple(
            dict(zip(names, combo, strict=True))
            for combo in product(*value_lists)
        )
