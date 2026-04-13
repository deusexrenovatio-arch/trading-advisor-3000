from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


StrategyExecutionMode = Literal["signals", "order_func"]


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
class StrategySpec:
    version: str
    family: str
    required_columns: tuple[str, ...]
    parameter_grid: tuple[StrategyParameter, ...]
    execution_mode: StrategyExecutionMode = "signals"

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("version must be non-empty")
        if not self.family.strip():
            raise ValueError("family must be non-empty")
        if not self.required_columns:
            raise ValueError("required_columns must not be empty")

