from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import wraps
from inspect import signature
from time import perf_counter
from typing import ParamSpec, TypeVar
from uuid import uuid4

StageTimings = dict[str, dict[str, object]]
P = ParamSpec("P")
R = TypeVar("R")
LOGGER = logging.getLogger("ta3000.pipeline_timing")
# A dedicated sink keeps progress visible in Spark subprocess stderr and Docker logs.
# Do not alter root logging or duplicate events through Dagster/root handlers.
if not LOGGER.handlers:
    LOGGER.addHandler(logging.StreamHandler())
    LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False


@dataclass
class _StepContext:
    step: str
    run_id: str
    dataset_version: object = None
    step_id: str = field(default_factory=lambda: uuid4().hex)
    parent_step_id: str | None = None
    pending: list[tuple[float, str]] = field(default_factory=list)


_CURRENT_STEP: ContextVar[_StepContext | None] = ContextVar("pipeline_timing_step", default=None)


def _emit(event_name: str, /, **payload: object) -> None:
    """Telemetry must never change a job result or mask its original exception."""
    try:
        context = _CURRENT_STEP.get()
        row = {
            **payload,
            "schema_version": "pipeline_timing.v1",
            "event": event_name,
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "pid": os.getpid(),
            "step": context.step if context else None,
            "step_id": context.step_id if context else None,
            "parent_step_id": context.parent_step_id if context else None,
            "run_id": context.run_id if context else None,
            "dataset_version": context.dataset_version if context else None,
        }
        LOGGER.log(logging.INFO, json.dumps(row, ensure_ascii=True, default=str))
    except Exception:
        pass


def stage_timer(stage: str | None = None) -> float:
    started = perf_counter()
    if stage is not None:
        context = _CURRENT_STEP.get()
        if context is not None:
            context.pending.append((started, stage))
        _emit("stage_started", stage=stage, status="RUNNING")
    return started


def record_stage_timing(
    stage_timings: StageTimings,
    stage: str,
    started_at: float,
    *,
    status: str = "PASS",
    **metrics: object,
) -> None:
    _record_stage_timing(stage_timings, stage, started_at, metrics, status=status)


def _record_stage_timing(
    stage_timings: StageTimings,
    stage: str,
    started_at: float,
    metrics: Mapping[str, object],
    *,
    status: str = "PASS",
) -> None:
    payload: dict[str, object] = {
        "status": status,
        "elapsed_seconds": round(max(perf_counter() - started_at, 0.0), 6),
    }
    payload.update(metrics)
    stage_timings[stage] = payload
    context = _CURRENT_STEP.get()
    if context is not None:
        try:
            context.pending.remove((started_at, stage))
        except ValueError:
            pass
    _emit("stage_finished", **{**payload, "stage": stage})


def record_skipped_stage(stage_timings: StageTimings, stage: str, *, reason: str) -> None:
    context = _CURRENT_STEP.get()
    if context is not None:
        context.pending[:] = [
            (started, pending_stage)
            for started, pending_stage in context.pending
            if pending_stage != stage
        ]
    stage_timings[stage] = {
        "status": "SKIPPED",
        "elapsed_seconds": 0.0,
        "reason": reason,
    }
    _emit("stage_finished", stage=stage, **stage_timings[stage])


@contextmanager
def timed_stage(
    stage_timings: StageTimings, stage: str, **metrics: object
) -> Iterator[dict[str, object]]:
    """Measure existing work only; metrics must not trigger extra Spark actions."""
    started = stage_timer(stage)
    try:
        yield metrics
    except BaseException as exc:
        _record_stage_timing(
            stage_timings,
            stage,
            started,
            {**metrics, "status": "FAIL", "error_type": type(exc).__name__},
        )
        raise
    else:
        _record_stage_timing(stage_timings, stage, started, metrics)


def timed_step(step: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Log a whole synchronous job, including cleanup, without changing its report."""

    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        function_signature = signature(function)

        @wraps(function)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            bound = function_signature.bind(*args, **kwargs)
            bound.apply_defaults()
            arguments = bound.arguments
            parent = _CURRENT_STEP.get()
            run_id = (
                arguments.get("run_id")
                or arguments.get("build_run_id")
                or getattr(arguments.get("context"), "run_id", None)
                or (parent.run_id if parent else None)
                or os.environ.get("DAGSTER_RUN_ID")
                or uuid4().hex
            )
            context = _StepContext(
                step=step,
                run_id=str(run_id),
                dataset_version=arguments.get("dataset_version")
                or (parent.dataset_version if parent else None),
                parent_step_id=parent.step_id if parent else None,
            )
            token = _CURRENT_STEP.set(context)
            started = perf_counter()
            _emit(
                "step_started", status="RUNNING", dataset_version=arguments.get("dataset_version")
            )
            try:
                result = function(*args, **kwargs)
            except BaseException as exc:
                for stage_started, stage in tuple(context.pending):
                    record_stage_timing(
                        {}, stage, stage_started, status="FAIL", error_type=type(exc).__name__
                    )
                _emit(
                    "step_finished",
                    status="FAIL",
                    error_type=type(exc).__name__,
                    elapsed_seconds=round(max(perf_counter() - started, 0.0), 6),
                )
                raise
            else:
                status = "PASS"
                if isinstance(result, Mapping):
                    status = str(
                        result.get("status")
                        or ("FAIL" if result.get("success") is False else "PASS")
                    )
                for stage_started, stage in tuple(context.pending):
                    record_stage_timing({}, stage, stage_started, status="INCOMPLETE")
                _emit(
                    "step_finished",
                    status=status,
                    elapsed_seconds=round(max(perf_counter() - started, 0.0), 6),
                )
                return result
            finally:
                _CURRENT_STEP.reset(token)

        return wrapped

    return decorate
