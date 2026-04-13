from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResamplePlan:
    source_timeframe: str
    target_timeframes: tuple[str, ...]
    require_session_alignment: bool = True
    preserve_point_in_time_safety: bool = True

    def __post_init__(self) -> None:
        if not self.source_timeframe.strip():
            raise ValueError("source_timeframe must be non-empty")
        if not self.target_timeframes:
            raise ValueError("target_timeframes must not be empty")

