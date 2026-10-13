from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from trading_advisor_3000.product_plane.research import operations


def test_preflight_rejects_bad_window_before_loading_tables(tmp_path, monkeypatch):
    monkeypatch.setattr(operations, "_store_contracts", lambda: pytest.fail("late validation"))
    with pytest.raises(ValueError, match="instrument_id"):
        operations.preflight({"changed_windows": [{"timeframe": "15m"}]})


def test_preflight_checks_every_existing_writer_layout(tmp_path, monkeypatch):
    path = tmp_path / "research_indicator_frames.delta"
    (path / "_delta_log").mkdir(parents=True)
    monkeypatch.setattr(
        operations,
        "_store_contracts",
        lambda: {"research_indicator_frames": {"partition_columns": ("ts_close_year",)}},
    )
    monkeypatch.setattr(operations, "_table_layout", lambda p: (("dataset_version",), {"ts"}))
    with pytest.raises(ValueError, match="isolated layout migration"):
        operations.preflight({"materialized_output_dir": str(tmp_path)})


def test_preflight_accepts_native_legacy_cf_layout_but_requires_yearly_sidecar(tmp_path):
    from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows

    contracts = operations._store_contracts()
    assert contracts["continuous_front_indicator_frames"]["partition_by"] == ["ts_close_year"]
    for name in ("continuous_front_bars", "continuous_front_qc_report"):
        write_delta_table_rows(
            table_path=tmp_path / f"{name}.delta", rows=[], columns=contracts[name]["columns"]
        )
    config = {"materialized_output_dir": str(tmp_path)}
    operations.preflight(config)
    write_delta_table_rows(
        table_path=tmp_path / "continuous_front_indicator_frames.delta",
        rows=[],
        columns=contracts["continuous_front_indicator_frames"]["columns"],
    )
    with pytest.raises(ValueError, match="continuous_front_indicator_frames.*layout migration"):
        operations.preflight(config)


def test_dead_worker_requires_same_namespace_and_process_identity():
    identity = {"owner": "daemon", "generation": "100", "pid": 10, "process_start": "20"}
    assert (
        operations.worker_state(identity, owner="web", generation="100", process_start="20")
        == "UNKNOWN"
    )
    assert (
        operations.worker_state(identity, owner="daemon", generation="100", process_start="20")
        == "ALIVE"
    )
    assert (
        operations.worker_state(identity, owner="daemon", generation="101", process_start="20")
        == "DEAD"
    )
    assert (
        operations.worker_state(identity, owner="daemon", generation="100", process_start="21")
        == "DEAD"
    )


def test_missing_heartbeat_does_not_override_live_child(monkeypatch):
    identity = {"owner": "daemon", "generation": "100", "pid": 10, "process_start": "20"}
    identity["stage_worker"] = {**identity, "pid": 11, "process_start": "30"}
    monkeypatch.setattr(operations, "worker_identity", lambda: identity)
    monkeypatch.setattr(operations, "_process_start", lambda pid: "30" if pid == 11 else None)
    assert operations.local_worker_state(identity) == "ALIVE"


def test_lock_recovery_refuses_legacy_or_live_owner(tmp_path, monkeypatch):
    lock = tmp_path / ".research_indicator_frames.lock.json"
    monkeypatch.setattr(operations, "local_worker_state", lambda p: "ALIVE")
    lock.write_text(json.dumps({"pid": 10}))
    assert not operations.recover_indicator_lock(tmp_path)
    assert lock.exists()
    lock.write_text(
        json.dumps({"owner": "daemon", "generation": "100", "pid": 10, "process_start": "20"})
    )
    assert not operations.recover_indicator_lock(tmp_path)
    monkeypatch.setattr(operations, "local_worker_state", lambda p: "DEAD")
    assert operations.recover_indicator_lock(tmp_path)
    assert not lock.exists()


def test_restart_guard_blocks_queued_running_and_unknown_statuses():
    for status in ("STARTED", "STARTING", "QUEUED", "CANCELING", "UNRECOGNIZED"):
        with pytest.raises(RuntimeError, match="active"):
            operations.assert_restart_safe([{"runId": "r", "status": status}])
    operations.assert_restart_safe([{"runId": "r", "status": "SUCCESS"}])


def test_preflight_accepts_native_closed_single_bar_window(tmp_path):
    operations.preflight(
        {
            "materialized_output_dir": str(tmp_path),
            "changed_windows": [
                {
                    "instrument_id": "Si",
                    "timeframe": "15m",
                    "start_ts": "2026-10-12T10:00:00",
                    "end_ts": "2026-10-12T10:00:00",
                }
            ],
        }
    )


def test_graphql_control_queries_match_installed_dagster_schema(monkeypatch):
    from dagster_graphql.schema import create_schema
    from graphql import parse, validate

    schema = create_schema().graphql_schema
    queries = []

    def graphql(url, query, variables=None):
        queries.append(query)
        return {
            "runsOrError": {"__typename": "Runs", "results": []},
            "launchRunReexecution": {"__typename": "LaunchRunSuccess", "run": {}},
        }

    monkeypatch.setattr(operations, "_graphql", graphql)
    operations.check_restart("unused")
    operations.resume("unused", "parent")
    for query in queries:
        assert validate(schema, parse(query)) == []


def test_health_sensor_is_registered_in_product_definitions():
    from trading_advisor_3000.dagster_defs.product_plane_definitions import (
        product_plane_definitions,
    )

    names = {sensor.name for sensor in product_plane_definitions.get_repository_def().sensor_defs}
    assert "research_worker_health_sensor" in names


def test_linux_dead_process_and_kernel_lock_recovery(tmp_path):
    import subprocess
    import sys

    if sys.platform != "linux":
        pytest.skip("production worker identities use Linux /proc")
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        identity = operations.worker_identity(process.pid)
        assert operations.local_worker_state(identity) == "ALIVE"
        lock = tmp_path / ".research_indicator_frames.lock.json"
        lock.write_text(json.dumps(identity))
        assert not operations.recover_indicator_lock(tmp_path)
        process.terminate()
        process.wait(timeout=10)
        assert operations.local_worker_state(identity) == "DEAD"
        assert operations.recover_indicator_lock(tmp_path)
        assert not lock.exists()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)


def test_reconcile_marks_only_proven_dead_running_worker(tmp_path, monkeypatch):
    leases = tmp_path / ".research_workers"
    leases.mkdir()
    (leases / "lease.json").write_text(
        json.dumps({"run_id": "r", "state": "ACTIVE", "owner": "daemon"})
    )
    run = SimpleNamespace(status=SimpleNamespace(value="STARTED"))
    failed = []
    instance = SimpleNamespace(
        get_run_by_id=lambda r: run, report_run_failed=lambda r, message: failed.append(message)
    )
    monkeypatch.setattr(operations, "local_worker_state", lambda p: "UNKNOWN")
    assert operations.reconcile_workers(instance, tmp_path) == []
    monkeypatch.setattr(operations, "local_worker_state", lambda p: "DEAD")
    assert operations.reconcile_workers(instance, tmp_path) == ["r"]
    assert len(failed) == 1
    assert json.loads((leases / "lease.json").read_text())["state"] == "LOST"
