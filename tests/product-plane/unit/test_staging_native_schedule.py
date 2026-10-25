"""Native scheduler metadata only; producer/RPC boundaries are never executed."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import dagster._daemon  # noqa: F401 -- native daemon bootstrap avoids scheduler import cycle
import pytest
from dagster import ScheduleDefinition, build_schedule_context, job, op
from dagster._core.execution.api import create_execution_plan
from dagster._core.launcher.default_run_launcher import DefaultRunLauncher
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteInstigatorOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    InstigatorType,
    ScheduleInstigatorData,
    TickData,
    TickStatus,
)
from dagster._core.snap.execution_plan_snapshot import snapshot_from_execution_plan
from dagster._core.storage.tags import SCHEDULED_EXECUTION_TIME_TAG
from dagster._scheduler import scheduler
from dagster._utils.schedules import cron_string_iterator
from test_staging_execution import staging as staging

from trading_advisor_3000.product_plane.common import staging_execution as guard


def test_native_scheduler_submit_precedes_tick_commit_and_budgeted_launch(staging, monkeypatch):
    instant = next(
        cron_string_iterator(
            datetime.now(UTC).timestamp(),
            "0 2 * * *",
            "Europe/Moscow",
            ascending=False,
        )
    )
    staging.plan.update(stage="acceptance", attempt_kind="acceptance", require_scheduled=True)
    staging.plan["source"] = {"native_schedule": {"logical_scheduled_at_utc": instant.isoformat()}}
    staging.save()
    instance = staging.instance
    origin = RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("staging"), "repo")
    state = InstigatorState(
        RemoteInstigatorOrigin(origin, "moex_baseline_daily_update_schedule"),
        InstigatorType.SCHEDULE,
        InstigatorStatus.RUNNING,
        ScheduleInstigatorData("0 2 * * *"),
    )
    instance.add_instigator_state(state)
    tick = instance.create_tick(
        TickData(
            state.instigator_origin_id,
            state.name,
            InstigatorType.SCHEDULE,
            TickStatus.STARTED,
            instant.timestamp(),
            selector_id=state.selector_id,
        )
    )

    @op
    def forbidden_producer():
        raise AssertionError("A metadata characterization cannot execute a producer")

    @job(name="source")
    def metadata_job():
        forbidden_producer()

    definition = ScheduleDefinition(
        name=state.name,
        job=metadata_job,
        cron_schedule="0 2 * * *",
        execution_timezone="Europe/Moscow",
    )
    schedule = SimpleNamespace(
        name=state.name,
        job_name="source",
        op_selection=None,
        selector_id=state.selector_id,
        handle=SimpleNamespace(repository_handle=SimpleNamespace(), repository_name="repo"),
        get_remote_origin=lambda: state.origin,
        get_remote_origin_id=lambda: state.instigator_origin_id,
    )
    remote_job = SimpleNamespace(
        name="source",
        run_tags={},
        resolved_op_selection=None,
        op_selection=None,
        job_snapshot=metadata_job.get_job_snapshot(),
        parent_job_snapshot=None,
        get_remote_origin=lambda: RemoteJobOrigin(origin, "source"),
        get_python_origin=lambda: None,
        repository_handle=SimpleNamespace(repository_name="repo"),
    )
    location = SimpleNamespace(
        name="staging",
        get_job=lambda selector: remote_job,
        get_execution_plan=lambda *args, **kwargs: SimpleNamespace(
            execution_plan_snapshot=snapshot_from_execution_plan(
                create_execution_plan(metadata_job), metadata_job.get_job_snapshot_id()
            )
        ),
        get_repository=lambda name: SimpleNamespace(
            asset_graph=metadata_job.asset_layer.asset_graph
        ),
        get_schedule_execution_data=lambda **kwargs: definition.evaluate_tick(
            build_schedule_context(instance=instance, scheduled_execution_time=instant)
        ),
    )
    workspace = SimpleNamespace(
        instance=instance,
        create_request_context=lambda: SimpleNamespace(get_code_location=lambda name: location),
    )
    coordinator = guard.StagingRunCoordinator()
    coordinator.register_instance(instance)
    observed = []

    def submit(run_id, request_context):
        actual = instance.get_run_by_id(run_id)
        assert SCHEDULED_EXECUTION_TIME_TAG in actual.tags
        assert "dagster/scheduled_execution_time" not in actual.tags
        current = instance.get_ticks(state.instigator_origin_id, state.selector_id, limit=1)[0]
        assert current.status == TickStatus.STARTED and run_id not in current.run_ids
        coordinator.submit_run(SimpleNamespace(dagster_run=actual))
        assert coordinator.get_run_queue_config().max_concurrent_runs == 0
        with guard._quota(staging.plan) as quota:
            assert (
                quota.get_run_by_id(guard._key("staging-root-v1", "quota-test", "acceptance", 0))
                is None
            )
        observed.append(run_id)

    monkeypatch.setattr(instance, "submit_run", submit)
    monkeypatch.setattr(scheduler, "log_action", lambda *args, **kwargs: None)
    with scheduler._ScheduleLaunchContext(
        remote_schedule=schedule,
        tick=tick,
        instance=instance,
        logger=logging.getLogger(__name__),
        tick_retention_settings={},
    ) as context:
        assert not any(
            scheduler._schedule_runs_at_time(
                workspace,
                logging.getLogger(__name__),
                schedule,
                instant,
                "Europe/Moscow",
                context,
                submit_threadpool_executor=None,
            )
        )
    assert len(observed) == 1
    run = instance.get_run_by_id(observed[0])
    assert guard._reference_tick(staging.plan, instance, run).status == TickStatus.SUCCESS
    assert coordinator.get_run_queue_config().max_concurrent_runs == 1
    calls = []

    def rpc_stub(launcher, context):
        with guard._quota(staging.plan) as quota:
            assert (
                quota.get_run_by_id(guard._key("staging-root-v1", "quota-test", "acceptance", 0))
                is not None
            )
        calls.append(context.dagster_run.run_id)

    monkeypatch.setattr(DefaultRunLauncher, "launch_run", rpc_stub)
    launcher = guard.StagingRunLauncher()
    launcher.register_instance(instance)
    monkeypatch.setattr(instance, "_run_launcher", launcher)
    staging.save()
    instance.launch_run(run.run_id, workspace=None)
    assert calls == observed


@pytest.mark.parametrize("kind", ["future", "unreachable", "wrong_tick_time"])
def test_future_or_unreachable_native_schedule_cannot_spend_quota(staging, kind):
    staging.plan.update(stage="acceptance", attempt_kind="acceptance", require_scheduled=True)
    staging.save()
    instant = datetime.now(UTC) + timedelta(hours=1 if kind == "future" else -1)
    context = staging.context(
        "forged",
        tags={
            "dagster/schedule_name": "moex_baseline_daily_update_schedule",
            SCHEDULED_EXECUTION_TIME_TAG: instant.isoformat(),
        },
    )
    run = staging.instance.get_run_by_id(context.run_id)
    if kind != "unreachable":
        state = InstigatorState(
            RemoteInstigatorOrigin(
                run.remote_job_origin.repository_origin, "moex_baseline_daily_update_schedule"
            ),
            InstigatorType.SCHEDULE,
            InstigatorStatus.RUNNING,
            ScheduleInstigatorData("0 2 * * *"),
        )
        staging.instance.add_instigator_state(state)
        staging.instance.create_tick(
            TickData(
                state.instigator_origin_id,
                state.name,
                InstigatorType.SCHEDULE,
                TickStatus.SUCCESS,
                instant.timestamp() + (1 if kind == "wrong_tick_time" else 0),
                run_ids=[run.run_id],
                selector_id=state.selector_id,
            )
        )
    with pytest.raises(guard.StagingExecutionError, match="future|persisted schedule tick"):
        guard._bind_run(staging.plan, staging.instance, run)
    with guard._quota(staging.plan) as quota:
        assert (
            quota.get_run_by_id(guard._key("staging-root-v1", "quota-test", "acceptance", 0))
            is None
        )
