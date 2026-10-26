"""Tiny native metadata only; these fixtures are never schedule/market evidence."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import DagsterInstance
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteInstigatorOrigin,
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
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus
from dagster._utils.schedules import cron_string_iterator

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import initialize_staging_schedule as initializer  # noqa: E402

from trading_advisor_3000.product_plane.common import staging_execution as guard  # noqa: E402


def plan_and_state(instance):
    now = datetime.now(UTC)
    selected = next(
        cron_string_iterator(
            now.timestamp(),
            "0 2 * * *",
            "Europe/Moscow",
            ascending=False,
        )
    )
    plan = {
        "task_id": "metadata-fixture:one-time-checkpoint",
        "deadline_utc": (now + timedelta(hours=1)).isoformat(),
        "stage": "acceptance",
        "attempt_kind": "acceptance",
        "root_job": "moex_baseline_update_job",
        "require_scheduled": True,
        "jobs": {
            job: {}
            for job in (
                "moex_baseline_update_job",
                "moex_cf_catch_up_job",
                "research_data_prep_job",
            )
        },
        "source": {
            "native_schedule": {
                "cron": "0 2 * * *",
                "timezone": "Europe/Moscow",
                "logical_scheduled_at_utc": selected.isoformat(),
            }
        },
    }
    repo = RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("metadata-fixture"), "fixture")
    state = InstigatorState(
        RemoteInstigatorOrigin(repo, initializer.SCHEDULE),
        InstigatorType.SCHEDULE,
        InstigatorStatus.DECLARED_IN_CODE,
        ScheduleInstigatorData("0 2 * * *", now.timestamp(), now.timestamp()),
    )
    instance.add_instigator_state(state)
    return plan, state, selected.timestamp()


def test_checkpoint_dry_run_and_one_time_apply_preserve_native_identity(tmp_path):
    home, quota_home = tmp_path / "home", tmp_path / "quota"
    home.mkdir()
    quota_home.mkdir()
    with (
        DagsterInstance.local_temp(str(home)) as instance,
        DagsterInstance.local_temp(str(quota_home)) as quota,
    ):
        plan, state, selected = plan_and_state(instance)
        other = InstigatorState(
            RemoteInstigatorOrigin(state.origin.repository_origin, "unrelated-schedule"),
            InstigatorType.SCHEDULE,
            InstigatorStatus.STOPPED,
            ScheduleInstigatorData("0 3 * * *"),
        )
        instance.add_instigator_state(other)
        assert initializer.initialize(instance, quota, plan)["status"] == "DRY_RUN"
        assert instance.get_instigator_state(state.instigator_origin_id, state.selector_id) == state
        assert not quota.get_runs()
        context = {"expires_at_utc": (datetime.now(UTC) + timedelta(seconds=30)).isoformat()}
        result = initializer.initialize(instance, quota, plan, apply=True, context=context)
        actual = instance.get_instigator_state(state.instigator_origin_id, state.selector_id)
        assert result["status"] == "INITIALIZED_NATIVE_CHECKPOINT"
        assert actual.origin == state.origin and actual.status == state.status
        assert actual.instigator_data.cron_schedule == state.instigator_data.cron_schedule
        assert actual.instigator_data.start_timestamp == selected - 1
        assert actual.instigator_data.last_iteration_timestamp == selected - 1
        assert instance.get_instigator_state(other.instigator_origin_id, other.selector_id) == other
        assert not instance.get_ticks(state.instigator_origin_id, state.selector_id)
        assert not instance.get_runs()
        fresh = tmp_path / "recreated-home"
        fresh.mkdir()
        with DagsterInstance.local_temp(str(fresh)) as recreated:
            fresh_plan, _, _ = plan_and_state(recreated)
            fresh_plan["unrelated_candidate_sha"] = "new SHA cannot reset initialization"
            with pytest.raises(ValueError, match="already claimed"):
                initializer.initialize(recreated, quota, fresh_plan, apply=True, context=context)


@pytest.mark.parametrize("existing", ["run", "tick", "task_attempt", "active_other_job"])
def test_nonfresh_state_refuses_without_checkpoint_or_ledger_writes(tmp_path, existing):
    home, quota_home = tmp_path / "home", tmp_path / "quota"
    home.mkdir()
    quota_home.mkdir()
    with (
        DagsterInstance.local_temp(str(home)) as instance,
        DagsterInstance.local_temp(str(quota_home)) as quota,
    ):
        plan, state, selected = plan_and_state(instance)
        if existing == "tick":
            instance.create_tick(
                TickData(
                    instigator_origin_id=state.instigator_origin_id,
                    instigator_name=state.name,
                    instigator_type=InstigatorType.SCHEDULE,
                    status=TickStatus.SUCCESS,
                    timestamp=selected,
                    run_ids=[],
                    selector_id=state.selector_id,
                )
            )
        elif existing == "task_attempt":
            guard._add(
                quota,
                guard._key("staging-root-v1", plan["task_id"], "acceptance", 0),
                {
                    "task_id": plan["task_id"],
                    "root_run_id": "metadata-only-fixture",
                },
            )
        else:
            instance.add_run(
                DagsterRun(
                    job_name="moex_baseline_update_job" if existing == "run" else "other-job",
                    run_id="metadata-only-fixture",
                    status=(
                        DagsterRunStatus.SUCCESS if existing == "run" else DagsterRunStatus.STARTED
                    ),
                )
            )
        before = len(quota.get_runs())
        with pytest.raises(ValueError):
            initializer.initialize(instance, quota, plan, apply=True, context={})
        assert instance.get_instigator_state(state.instigator_origin_id, state.selector_id) == state
        assert len(quota.get_runs()) == before


def test_unrelated_quota_history_does_not_block_but_each_canonical_task_root_does(tmp_path):
    home = tmp_path / "home"
    home.mkdir()

    class Quota:
        roots = {}
        history_queries = 0
        unrelated = [SimpleNamespace(run_config={"task_id": "other", "root_run_id": "old"})] * 1001

        def get_runs(self, *, filters, limit):
            self.history_queries += 1
            return self.unrelated[:limit]

        def get_run_by_id(self, run_id):
            return self.roots.get(run_id)

    quota = Quota()
    with DagsterInstance.local_temp(str(home)) as instance:
        plan, state, _ = plan_and_state(instance)
        assert initializer.initialize(instance, quota, plan)["status"] == "DRY_RUN"
        assert quota.history_queries == 0
        for kind in ("small", "acceptance", "rebuild"):
            quota.roots = {guard._key("staging-root-v1", plan["task_id"], kind, 0): object()}
            with pytest.raises(ValueError, match="Claimed task attempts"):
                initializer.initialize(instance, quota, plan)
        assert instance.get_instigator_state(state.instigator_origin_id, state.selector_id) == state
        assert not instance.get_runs()


def test_expired_host_inspection_refuses_before_native_state_access(tmp_path):
    control, host_state, home = tmp_path / "control", tmp_path / "host-state", tmp_path / "home"
    for path in (control, host_state, home):
        path.mkdir()
    plan = {"candidate": {"image_digest": "sha256:" + "a" * 64}}
    plan_path, context_path = control / "plan.json", control / "context.json"
    binding = guard.digest(plan)
    (control / "root-marker.json").write_text(
        json.dumps(
            {
                "schema_version": "staging_root.v1",
                "plan_path": str(plan_path),
                "plan_digest": binding,
            }
        )
    )
    lock = {
        "schema_version": "staging_host_lock.v1",
        "project": "ta3000-acceptance",
        "plan_sha256": binding,
        "lease_nonce": "a" * 32,
    }
    (host_state / "heavy-compute.lock").write_text(json.dumps(lock))
    past = datetime.now(UTC) - timedelta(minutes=1)
    context_path.write_text(
        json.dumps(
            {
                **lock,
                "schema_version": "staging_schedule_initialization_context.v1",
                "daemon_stopped": True,
                "home_volume": "ta3000-acceptance_staging-home",
                "home_path": str(home),
                "image_digest": plan["candidate"]["image_digest"],
                "verified_at_utc": past.isoformat(),
                "expires_at_utc": (past + timedelta(seconds=30)).isoformat(),
            }
        )
    )
    with pytest.raises(ValueError, match="expired"):
        initializer.verify_host_context(plan, plan_path, context_path, host_state, home)


def test_future_metadata_proposal_cannot_initialize_an_unelapsed_tick(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    with DagsterInstance.local_temp(str(home)) as instance:
        plan, state, _ = plan_and_state(instance)
        future = next(
            cron_string_iterator(
                datetime.now(UTC).timestamp(),
                "0 2 * * *",
                "Europe/Moscow",
            )
        )
        plan["source"]["native_schedule"]["logical_scheduled_at_utc"] = future.isoformat()
        with pytest.raises(ValueError, match="elapsed"):
            initializer.selected_instant(plan)
        assert instance.get_instigator_state(state.instigator_origin_id, state.selector_id) == state
        assert not instance.get_runs()
