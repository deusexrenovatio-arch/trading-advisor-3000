"""Native GraphQL metadata fixtures: hidden run tags are not a schedule witness."""

import copy
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402

ROOT_JOB = "moex_baseline_update_job"


def native(timestamp=1792882800.0, run_id="old", status="SUCCESS"):
    return {
        "repositoriesOrError": {
            "__typename": "RepositoryConnection",
            "nodes": [
                {
                    "name": "__repository__",
                    "location": {
                        "name": "trading_advisor_3000.dagster_defs:product_plane_definitions"
                    },
                    "schedules": [
                        {
                            "name": "moex_baseline_daily_update_schedule",
                            "cronSchedule": "0 2 * * *",
                            "executionTimezone": "Europe/Moscow",
                            "scheduleState": {
                                "id": "origin::selector",
                                "status": "RUNNING",
                                "ticks": [
                                    {
                                        "id": "origin:" + str(float(timestamp)),
                                        "timestamp": timestamp,
                                        "status": status,
                                        "runIds": [run_id],
                                    }
                                ],
                            },
                        }
                    ],
                }
            ],
        }
    }


def prepared():
    return {
        "production_url": "prod",
        "staging_url": "stage",
        "plan": {
            "stage": "acceptance",
            "root_job": ROOT_JOB,
            "source": {"native_schedule": {"cron": "0 2 * * *", "timezone": "Europe/Moscow"}},
        },
    }


def test_native_root_query_includes_failed_unbound_history_without_status_or_plan_filters(
    monkeypatch,
):
    calls = []

    def read(url, query, variables):
        calls.append(variables["filter"])
        return {
            "runsOrError": {
                "__typename": "Runs",
                "results": [{"runId": "unbound", "status": "FAILURE"}],
            }
        }

    monkeypatch.setattr(control, "graphql_read", read)
    assert control.graphql_runs("stage", job_name=ROOT_JOB)[0]["status"] == "FAILURE"
    assert calls == [{"pipelineName": ROOT_JOB}]


@pytest.mark.parametrize("damage", ["tick-origin", "cron", "repository", "duplicate"])
def test_native_schedule_rejects_fabricated_tick_or_route(monkeypatch, damage):
    payload = native()
    repo = payload["repositoriesOrError"]["nodes"][0]
    schedule = repo["schedules"][0]
    if damage == "tick-origin":
        schedule["scheduleState"]["ticks"][0]["id"] = "fake:1792882800.0"
    elif damage == "cron":
        schedule["cronSchedule"] = "* * * * *"
    elif damage == "repository":
        repo["name"] = "caller-repository"
    else:
        schedule["scheduleState"]["ticks"] *= 2

    def read(url, query):
        return payload

    monkeypatch.setattr(control, "graphql_read", read)
    with pytest.raises(ValueError, match="Native|native"):
        control.graphql_schedule(prepared(), "stage")


def test_unbound_failure_after_actual_id_boundary_halts_and_retained_old_failure_is_preserved(
    monkeypatch,
):
    value, payload = prepared(), native()
    roots = [{"runId": "old", "status": "FAILURE", "pipelineName": ROOT_JOB, "tags": []}]
    monkeypatch.setattr(control, "graphql_read", lambda *a: payload)
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: roots)
    control.begin_native_observation(value, "stage")
    control.check_native_failures(value, "stage")
    roots.append(
        {"runId": "new-unbound", "status": "FAILURE", "pipelineName": ROOT_JOB, "tags": []}
    )
    with pytest.raises(ValueError, match="new-unbound"):
        control.check_native_failures(value, "stage")
    assert roots[0]["runId"] == "old" and value["native_observation_boundary"]["root_run_ids"] == {
        "old"
    }


@pytest.mark.parametrize("existing_started", [False, True])
def test_prelaunch_tick_failure_without_any_bound_root_halts(monkeypatch, existing_started):
    value, payload = prepared(), native(status="STARTED" if existing_started else "SUCCESS")
    monkeypatch.setattr(control, "graphql_read", lambda *a: payload)
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [])
    control.begin_native_observation(value, "stage")
    state = payload["repositoriesOrError"]["nodes"][0]["schedules"][0]["scheduleState"]
    if existing_started:
        state["ticks"][0]["status"] = "FAILURE"
    else:
        state["ticks"].append(
            native(1792969200, "new", "FAILURE")["repositoriesOrError"]["nodes"][0]["schedules"][0][
                "scheduleState"
            ]["ticks"][0]
        )
    with pytest.raises(ValueError, match="schedule tick failed"):
        control.check_native_failures(value, "stage")


def test_safe_window_still_waits_for_persisted_success_tick_link(monkeypatch):
    value = prepared()
    value["plan"]["execution_window"] = {
        "start_utc": "2030-01-01T00:00:00Z",
        "end_utc": "2030-01-02T00:00:00Z",
    }
    payload = native(run_id="root", status="STARTED")
    monkeypatch.setattr(control, "graphql_read", lambda *a: payload)
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [])
    root = {"runId": "root", "tags": []}
    now = datetime(2030, 1, 1, 1, tzinfo=UTC)
    assert not control.production_barrier(value, root, now)
    payload["repositoriesOrError"]["nodes"][0]["schedules"][0]["scheduleState"]["ticks"][0][
        "status"
    ] = "SUCCESS"
    assert control.production_barrier(value, root, now)
    assert not control.production_barrier(
        value,
        {
            "runId": "fake",
            "tags": [{"key": "dagster/scheduled_execution_time", "value": now.isoformat()}],
        },
        now,
    )


def test_matching_production_uses_actual_tick_run_ids_with_hidden_tag_absent(monkeypatch):
    value = prepared()
    payloads = {"stage": native(run_id="candidate"), "prod": native(run_id="production")}

    def read(url, query):
        return copy.deepcopy(payloads[url])

    def runs(url, **kw):
        if kw.get("job_name"):
            return [
                {"runId": "production", "status": "SUCCESS", "pipelineName": ROOT_JOB, "tags": []}
            ]
        upstream = (kw.get("tags") or {}).get("ta3000/upstream_run_id")
        if upstream:
            job, run_id = (
                ("moex_cf_catch_up_job", "cf")
                if upstream == "production"
                else ("research_data_prep_job", "research")
            )
            return [{"runId": run_id, "status": "SUCCESS", "pipelineName": job, "tags": []}]
        return []

    monkeypatch.setattr(control, "graphql_read", read)
    monkeypatch.setattr(control, "graphql_runs", runs)
    assert control.production_barrier(value, {"runId": "candidate", "tags": []}, datetime.now(UTC))
    payloads["prod"] = native(1792969200, "production")
    assert not control.production_barrier(
        value, {"runId": "candidate", "tags": []}, datetime.now(UTC)
    )
