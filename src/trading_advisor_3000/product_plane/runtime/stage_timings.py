"""Compatibility imports for shared product-plane timing utilities."""

from trading_advisor_3000.product_plane.common.stage_timings import (
    StageTimings,
    record_skipped_stage,
    record_stage_timing,
    stage_timer,
    timed_stage,
    timed_step,
)

__all__ = [
    "StageTimings",
    "record_skipped_stage",
    "record_stage_timing",
    "stage_timer",
    "timed_stage",
    "timed_step",
]
