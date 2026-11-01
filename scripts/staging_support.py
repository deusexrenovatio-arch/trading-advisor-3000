"""Host-only persistent attempt accounting; reuse native Dagster storage."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

LEGACY_TASK = "staging-admission-guardrails-20261025"


class CleanupUncertain(OSError):
    """Native session termination was not confirmed; keep the worker lock."""


@contextmanager
def worker_lock(home):
    path = Path(home) / "staging-worker.lock"
    with path.open("x", encoding="utf-8") as handle:
        handle.write("Active staging operation; stale locks require explicit recovery.\n")
    release = True
    try:
        yield
    except CleanupUncertain:
        release = False
        raise
    finally:
        if release:
            path.unlink()


def initialize_budget(instance, task, limits, spent):
    """Explicit setup only. Never called by branch, CI, retry or SHA changes."""
    from dagster import RunsFilter
    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus

    if instance.get_runs(filters=RunsFilter(job_name="staging_budget", tags={"task": task})):
        raise ValueError("Task already initialized; no reset")
    if set(limits) != {"branch", "acceptance"} or set(spent) != set(limits):
        raise ValueError("Explicit branch/acceptance history required")
    if any(type(n) is not int or n < 0 for n in [*limits.values(), *spent.values()]):
        raise ValueError("Invalid budget")
    if limits["acceptance"] != 1 or limits["branch"] > (3 if task == LEGACY_TASK else 2):
        raise ValueError("Budget exceeds accepted limits")
    if any(spent[k] > limits[k] for k in limits):
        raise ValueError("Prior spend exceeds budget")
    if task == LEGACY_TASK and spent != {"branch": 3, "acceptance": 0}:
        raise ValueError("Preserve previous small3/3 and acceptance0/1")
    instance.add_run(
        DagsterRun(
            job_name="staging_budget",
            status=DagsterRunStatus.CANCELED,
            tags={"task": task},
            run_config={"limits": limits, "spent": spent},
        )
    )


def budget_remaining(instance, task, stage):
    from dagster import RunsFilter

    records = instance.get_runs(
        filters=RunsFilter(job_name="staging_budget", tags={"task": task}), limit=2
    )
    if len(records) != 1 or not instance.is_persistent:
        raise ValueError(
            "Persistent task budget is uninitialized; import history before activation"
        )
    policy = records[0].run_config
    used = policy["spent"][stage] + instance.get_runs_count(
        filters=RunsFilter(job_name="staging_attempt", tags={"task": task, "stage": stage})
    )
    if used >= policy["limits"][stage]:
        raise ValueError("Attempt limit exhausted; no automatic extension")
    return used, policy["limits"][stage]


def reserve(instance, task, stage, run_id, binding):
    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus

    used, limit = budget_remaining(instance, task, stage)
    instance.add_run(
        DagsterRun(
            job_name="staging_attempt",
            run_id=run_id,
            status=DagsterRunStatus.CANCELED,
            tags={"task": task, "stage": stage},
            run_config=json.loads(json.dumps(binding)),
        )
    )
    return {"task": task, "used": used + 1, "limit": limit}
