"""Real Dagster metadata checks; snapshot fixtures are not route acceptance proof."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from dagster import DagsterEvent, DagsterEventType, DagsterInstance
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.definitions.run_status_sensor_definition import RunStatusSensorCursor
from dagster._core.execution.plan.objects import StepSuccessData
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteInstigatorOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    ScheduleInstigatorData,
    SensorInstigatorData,
    TickData,
    TickStatus,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.storage.event_log.base import RunStatusChangeRecordsFilter
from dagster_shared.record import copy as copy_record

from trading_advisor_3000.product_plane.common.staging_contracts import digest
from trading_advisor_3000.product_plane.common.staging_route_proof import JOBS, STAGES
from trading_advisor_3000.product_plane.interfaces import staging_observation as owner

SCHEDULE = "moex_baseline_daily_update_schedule"
CF_SENSOR = "moex_cf_catch_up_after_moex_baseline_sensor"
RESEARCH_SENSOR = "research_data_prep_after_moex_cf_catch_up_sensor"


@pytest.fixture
def recorded_cycle(tmp_path, monkeypatch):
    home, data = tmp_path / "instance", tmp_path / "data"
    home.mkdir()
    data.mkdir()
    plan = {
        "mode": "reference",
        "stage": "acceptance",
        "root_job": JOBS[0],
        "data_root": str(data),
        "candidate": {"head_sha": "a" * 40},
        "deadline_utc": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        "max_runtime_seconds": 600,
    }
    inputs = {
        "snapshot_id": "d" * 32,
        "manifest_sha256": "1" * 64,
        "tables": {"canonical_bars": {"delta_version": 1}},
        "fixture_metadata": True,
    }
    outputs = {
        "snapshot_id": "e" * 32,
        "data_snapshot_id": "d" * 32,
        "manifest_sha256": "2" * 64,
        "fixture_metadata": True,
    }
    monkeypatch.setattr(owner, "_snapshots", lambda *args: (inputs, outputs))
    repo = RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("staging"), "repo")
    with DagsterInstance.local_temp(str(home)) as instance:

        def record(job, run_id, *, upstream=None, steps=(), failure=False):
            tags = {
                "ta3000/staging_plan_sha256": digest(plan),
                "ta3000/candidate_head_sha": plan["candidate"]["head_sha"],
                "ta3000/staging_root_run_id": "root",
                "ta3000/reference_authorization": "fixture-authorization",
            }
            if upstream:
                previous = instance.get_run_by_id(upstream)
                tags.update(
                    {"ta3000/upstream_run_id": upstream, "ta3000/upstream_job": previous.job_name}
                )
            else:
                tags["dagster/schedule_name"] = SCHEDULE
            instance.add_run(
                DagsterRun(
                    job_name=job,
                    run_id=run_id,
                    tags=tags,
                    run_config={
                        "ops": {"moex_baseline_update": {"config": {"run_id": "logical-root"}}}
                    },
                    remote_job_origin=RemoteJobOrigin(repo, job),
                )
            )
            instance.report_dagster_event(DagsterEvent("PIPELINE_START", job), run_id=run_id)
            for step in steps:
                instance.report_dagster_event(
                    DagsterEvent(
                        "STEP_SUCCESS", job, step_key=step, event_specific_data=StepSuccessData(1.0)
                    ),
                    run_id=run_id,
                )
            event = "PIPELINE_FAILURE" if failure else "PIPELINE_SUCCESS"
            instance.report_dagster_event(DagsterEvent(event, job), run_id=run_id)

        def instigator(name, *, stopped=False, schedule=False):
            kind = InstigatorType.SCHEDULE if schedule else InstigatorType.SENSOR
            state = InstigatorState(
                RemoteInstigatorOrigin(repo, name),
                kind,
                InstigatorStatus.STOPPED if stopped else InstigatorStatus.RUNNING,
                ScheduleInstigatorData("0 0 * * *") if schedule else SensorInstigatorData(),
            )
            instance.add_instigator_state(state)
            return state

        def sensor_tick(name, upstream, *, reason=None, children=(), stopped=False):
            state = instigator(name, stopped=stopped)
            events = instance.fetch_run_status_changes(
                RunStatusChangeRecordsFilter(DagsterEventType.RUN_SUCCESS), limit=100
            ).records
            event = next(item for item in events if item.run_id == upstream)
            return instance.create_tick(
                TickData(
                    state.instigator_origin_id,
                    name,
                    InstigatorType.SENSOR,
                    TickStatus.SKIPPED if reason else TickStatus.SUCCESS,
                    time.time(),
                    run_ids=list(children),
                    skip_reason=reason,
                    cursor=RunStatusSensorCursor(record_id=event.storage_id).to_json(),
                    selector_id=state.selector_id,
                )
            )

        state = instigator(SCHEDULE, schedule=True)
        instance.create_tick(
            TickData(
                state.instigator_origin_id,
                SCHEDULE,
                InstigatorType.SCHEDULE,
                TickStatus.SUCCESS,
                time.time() - 1,
                run_ids=["root"],
                selector_id=state.selector_id,
            )
        )
        yield SimpleNamespace(
            plan=plan,
            data=data,
            instance=instance,
            record=record,
            sensor_tick=sensor_tick,
            instigator=instigator,
            observe=lambda: owner.observe_reference_cycle(plan, "root", instance, data),
        )


def test_missing_child_without_processed_sensor_tick_is_pending(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", steps=(STAGES[0],))
    cycle.instigator(CF_SENSOR)
    assert cycle.observe()["status"] == "PENDING"


def test_explicit_noop_tick_is_bound_to_exact_upstream_event(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", steps=(STAGES[0],))
    cycle.sensor_tick(CF_SENSOR, "root", reason="NOOP: proven_no_changes; admission=" + "a" * 64)
    result = cycle.observe()
    assert result["status"] == "PASS-NOOP"
    assert result["inputs"]["fixture_metadata"] is True
    assert result["outputs"]["snapshot_id"] == "e" * 32


def test_unrelated_run_cursor_cannot_turn_missing_child_into_noop(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", steps=(STAGES[0],))
    cycle.record(JOBS[0], "unrelated", steps=(STAGES[0],))
    cycle.sensor_tick(
        CF_SENSOR, "unrelated", reason="NOOP: proven_no_changes; admission=" + "a" * 64
    )
    assert cycle.observe()["status"] == "PENDING"


@pytest.mark.parametrize("reason", ["BLOCKED: missing_data_manifest", "BLOCKED: unknown_scope"])
def test_skipped_but_blocked_sensor_is_not_healthy_noop(recorded_cycle, reason):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", steps=(STAGES[0],))
    tick = cycle.sensor_tick(CF_SENSOR, "root", reason=reason)
    result = cycle.observe()
    assert result["status"] == "BLOCKED"
    edge = result["sensor_edges"][0]
    assert edge["tick_id"] == str(tick.tick_id)
    assert edge["status"] == "SKIPPED"
    assert edge["skip_reason"] == reason


def test_stopped_sensor_blocks_reference_chain(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", steps=(STAGES[0],))
    cycle.instigator(CF_SENSOR, stopped=True)
    assert cycle.observe()["status"] == "BLOCKED"


def test_failed_root_is_blocked(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", failure=True)
    assert cycle.observe()["status"] == "BLOCKED"


def test_missing_root_start_cannot_certify_a_completed_noop(recorded_cycle, monkeypatch):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", steps=(STAGES[0],))
    cycle.sensor_tick(CF_SENSOR, "root", reason="NOOP: proven_no_changes; admission=" + "a" * 64)
    native_stats = cycle.instance.get_run_stats

    def delayed_start(run_id):
        stats = native_stats(run_id)
        return copy_record(stats, start_time=None) if run_id == "root" else stats

    monkeypatch.setattr(cycle.instance, "get_run_stats", delayed_start)
    result = cycle.observe()
    assert result["status"] == "PENDING", result


@pytest.mark.parametrize("omit_stage", [None, "research_data_snapshot"])
def test_full_chain_requires_real_successful_steps_and_sensor_edges(recorded_cycle, omit_stage):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root", steps=(STAGES[0],))
    cycle.record(JOBS[1], "cf", upstream="root", steps=(STAGES[1],))
    cycle.record(
        JOBS[2],
        "research",
        upstream="cf",
        steps=tuple(step for step in STAGES[2:] if step != omit_stage),
    )
    cycle.sensor_tick(CF_SENSOR, "root", children=("cf",))
    cycle.sensor_tick(RESEARCH_SENSOR, "cf", children=("research",))
    result = cycle.observe()
    assert result["status"] == ("PASS" if omit_stage is None else "BLOCKED")
    if omit_stage is None:
        assert "research" in json.dumps(result["runs"])
