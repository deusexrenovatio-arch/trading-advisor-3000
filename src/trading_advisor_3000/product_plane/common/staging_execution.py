"""Small native launcher fence, installed only for an isolated CI proof session."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


def bind_run(instance, run, plan, seen=()):
    """Admit the exact root or a native successful upstream, once per job."""
    from dagster._core.errors import DagsterRunAlreadyExists
    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus

    jobs = plan["jobs"]
    if run is None or run.run_id in seen or run.parent_run_id or run.job_name not in jobs:
        raise ValueError("Run outside staging graph or retry/resume")
    index = jobs.index(run.job_name)
    if index == 0:
        if run.run_id != plan["root_run_id"] or run.run_config != plan["run_config"]:
            raise ValueError("Root identity/config differs from frozen plan")
    else:
        parent = instance.get_run_by_id(run.tags.get("ta3000/upstream_run_id", ""))
        if parent is None or parent.status.value != "SUCCESS" or parent.job_name != jobs[index - 1]:
            raise ValueError("Actual successful native upstream is missing")
        # Parent was already launched; inspect its real ancestry without reserving twice.
        chain = parent
        for expected in reversed(jobs[:index]):
            if chain is None or chain.job_name != expected or chain.status.value != "SUCCESS":
                raise ValueError("Broken native run lineage")
            if expected == jobs[0]:
                if chain.run_id != plan["root_run_id"]:
                    raise ValueError("Foreign root")
            else:
                chain = instance.get_run_by_id(chain.tags.get("ta3000/upstream_run_id", ""))
    tags = {"ta3000/staging_root": plan["root_run_id"]}
    claim_id = hashlib.sha256(
        f"staging-launch:{plan['root_run_id']}:{run.job_name}".encode()
    ).hexdigest()
    try:
        instance.add_run(
            DagsterRun(
                job_name="staging_launch_claim",
                run_id=claim_id,
                status=DagsterRunStatus.CANCELED,
                tags=tags,
                run_config={"native_run_id": run.run_id},
            )
        )
    except DagsterRunAlreadyExists as exc:
        raise ValueError("Staging job launch already spent; no automatic retry") from exc
    instance.add_run_tags(run.run_id, tags)


def __getattr__(name):
    if name != "StagingRunLauncher":
        raise AttributeError(name)
    from dagster._core.launcher.default_run_launcher import DefaultRunLauncher

    class StagingRunLauncher(DefaultRunLauncher):
        def launch_run(self, context):
            plan = json.loads(Path(os.environ["TA3000_STAGING_PLAN"]).read_text())
            run = self._instance.get_run_by_id(context.dagster_run.run_id)
            bind_run(self._instance, run, plan)
            super().launch_run(context)

    StagingRunLauncher.__module__ = __name__
    StagingRunLauncher.__qualname__ = name
    globals()[name] = StagingRunLauncher
    return StagingRunLauncher
