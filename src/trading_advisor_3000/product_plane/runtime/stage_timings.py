from __future__ import annotations

from time import perf_counter

StageTimings = dict[str, dict[str, object]]


def stage_timer() -> float:
    return perf_counter()


def record_stage_timing(
    stage_timings: StageTimings,
    stage: str,
    started_at: float,
    *,
    status: str = "PASS",
    **metrics: object,
) -> None:
    payload: dict[str, object] = {
        "status": status,
        "elapsed_seconds": round(max(perf_counter() - started_at, 0.0), 6),
    }
    payload.update(metrics)
    stage_timings[stage] = payload


def record_skipped_stage(stage_timings: StageTimings, stage: str, *, reason: str) -> None:
    stage_timings[stage] = {
        "status": "SKIPPED",
        "elapsed_seconds": 0.0,
        "reason": reason,
    }
