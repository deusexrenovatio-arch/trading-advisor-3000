from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LevelFeatureSpec:
    column_name: str
    description: str


def phase1_level_specs() -> tuple[LevelFeatureSpec, ...]:
    return (
        LevelFeatureSpec("rolling_high_20", "Local rolling 20-bar high used for breakout and structure checks."),
        LevelFeatureSpec("rolling_low_20", "Local rolling 20-bar low used for breakout and structure checks."),
        LevelFeatureSpec("session_vwap", "Session VWAP reference for distance and bias filters."),
        LevelFeatureSpec("opening_range_high", "Session opening-range high boundary."),
        LevelFeatureSpec("opening_range_low", "Session opening-range low boundary."),
        LevelFeatureSpec("distance_to_session_vwap", "Normalized distance between price and session VWAP."),
    )

