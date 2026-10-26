from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from trading_advisor_3000.product_plane.common import staging_execution as guard


@pytest.fixture
def recipe():
    config = {
        "ops": {
            "baseline": {
                "config": {
                    "run_id": "captured",
                    "refresh_window_days": 7,
                    "raw_table_path": "/ta3000-data/raw.delta",
                    "ingest_till_utc": "2030-01-02T00:00:00Z",
                }
            }
        },
        "execution": {"config": {"multiprocess": {"max_concurrent": 1}}},
    }
    plan = {
        "root_job": "baseline_job",
        "require_scheduled": False,
        "jobs": {"baseline_job": {"upstream_job": None}},
        "candidate": {"head_sha": "a" * 40},
        "source": {"native_schedule": {"run_config": config}},
    }
    run = SimpleNamespace(
        run_id="actual-root",
        job_name="baseline_job",
        run_config=deepcopy(config),
        status=SimpleNamespace(value="STARTING"),
        tags={},
        parent_run_id=None,
    )

    class Instance:
        is_persistent = True

        def get_run_by_id(self, run_id):
            assert run_id == run.run_id
            return run

        def add_run_tags(self, run_id, tags):
            pass

    return plan, run, Instance()


@pytest.mark.parametrize("logical_id", ["captured", "manual-logical-id"])
def test_root_recipe_preserves_exact_json_and_only_permits_logical_id(recipe, logical_id):
    plan, run, instance = recipe
    original = deepcopy(plan)
    run.run_config["ops"]["baseline"]["config"]["run_id"] = logical_id
    assert guard._bind_run(plan, instance, run, reserve=False) is run
    assert plan == original


@pytest.mark.parametrize(
    "field,value",
    [
        ("refresh_window_days", 365),
        ("raw_table_path", "/outside/raw.delta"),
        ("ingest_till_utc", "2031-01-01T00:00:00Z"),
    ],
)
def test_launcher_rejects_root_config_drift_before_quota_and_candidate_rpc(
    recipe, monkeypatch, field, value
):
    from dagster._core.launcher.default_run_launcher import DefaultRunLauncher

    plan, run, instance = recipe
    run.run_config["ops"]["baseline"]["config"][field] = value
    monkeypatch.setattr(guard, "_plan", lambda: plan)
    monkeypatch.setattr(guard, "_require_permit", lambda plan: None)
    monkeypatch.setattr(guard, "_root_reservation", lambda *args: pytest.fail("quota was spent"))
    monkeypatch.setattr(
        DefaultRunLauncher, "launch_run", lambda *args: pytest.fail("candidate RPC")
    )
    launcher = guard.StagingRunLauncher()
    launcher.register_instance(instance)
    with pytest.raises(guard.StagingExecutionError, match="protected native recipe"):
        launcher.launch_run(SimpleNamespace(dagster_run=run, workspace=None))


def test_reference_keeps_dynamic_native_input_validation(recipe, monkeypatch):
    plan, run, instance = recipe
    plan["mode"] = "reference"
    run.run_config["ops"]["baseline"]["config"]["daily_open_interest_version"] = 42
    monkeypatch.setattr(guard, "_reference_root", lambda *args, **kwargs: run)
    assert guard._bind_run(plan, instance, run, reserve=False) is run
