from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MultiTimeframeBinding:
    source_timeframe: str
    target_timeframe: str
    carried_columns: tuple[str, ...]
    lag_bars: int = 1

    def __post_init__(self) -> None:
        if self.lag_bars <= 0:
            raise ValueError("lag_bars must be positive")
        if not self.carried_columns:
            raise ValueError("carried_columns must not be empty")

