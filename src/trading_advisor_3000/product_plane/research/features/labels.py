from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LabelFeatureSpec:
    column_name: str
    description: str


def default_label_specs() -> tuple[LabelFeatureSpec, ...]:
    return (
        LabelFeatureSpec("breakout_ready_flag", "Marks bars that are eligible for breakout-family triggers."),
        LabelFeatureSpec("reversion_ready_flag", "Marks bars that are eligible for mean-reversion-family triggers."),
        LabelFeatureSpec("atr_stop_ref_1x", "Reference stop distance based on 1x ATR."),
        LabelFeatureSpec("atr_target_ref_2x", "Reference target distance based on 2x ATR."),
    )

