from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegimeFeatureSpec:
    column_name: str
    description: str


def phase1_regime_specs() -> tuple[RegimeFeatureSpec, ...]:
    return (
        RegimeFeatureSpec("volatility_regime_code", "Categorical regime code driven by ATR/NATR and band width."),
        RegimeFeatureSpec("volume_regime_code", "Categorical regime code driven by RVOL and volume z-score."),
        RegimeFeatureSpec("squeeze_state_code", "Categorical state describing compression vs release."),
    )

