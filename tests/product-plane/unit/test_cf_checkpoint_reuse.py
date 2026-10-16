from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as snapshots
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_arrow,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research import data_snapshots as owner
from trading_advisor_3000.product_plane.research.continuous_front import (
    CONTINUOUS_FRONT_TABLES,
    continuous_front_store_contract,
)


@pytest.fixture
def checkpoint(tmp_path):
    canonical = tmp_path / "canonical"
    canonical.mkdir()
    source = canonical / "canonical_bars.delta"
    write_deltalake(str(source), pa.table({"id": [1]}))
    parent = snapshots.create_snapshot(
        tables={"canonical_bars": snapshots.capture_table(source)},
        required_tables=["canonical_bars"],
        dataset_version="test",
        run_id="data-run",
        code_version="test",
        quality_checks={"inputs": "PASS"},
    )
    parent_path = snapshots.save_snapshot(canonical / ".data_snapshots", parent)
    snapshots.publish_snapshot(canonical / ".data_snapshots", parent_path, expected_current_id=None)
    config = {
        "data_snapshot_enabled": True,
        "canonical_output_dir": str(canonical),
        "materialized_output_dir": str(tmp_path / "research"),
        "dataset_version": "test",
        "code_version": "upstream-label",
        "series_mode": "continuous_front",
        "continuous_front_policy": {},
        "timeframes": ["15m"],
    }
    source_run = SimpleNamespace(
        job_name="moex_cf_catch_up_job", status=SimpleNamespace(value="SUCCESS")
    )
    step = SimpleNamespace(
        step_key="continuous_front_bars", status=SimpleNamespace(value="SUCCESS")
    )
    instance = SimpleNamespace(
        get_run_by_id=lambda _: source_run,
        get_run_step_stats=lambda _: [step],
    )
    producer = SimpleNamespace(
        run_id="cf-producer",
        op_config=config,
        instance=instance,
        dagster_run=SimpleNamespace(tags={}),
    )
    calls = []

    @owner.snapshot_materialization(CONTINUOUS_FRONT_TABLES)
    def continuous_front_bars(context):
        calls.append(context.run_id)
        root = tmp_path / "research"
        paths = {name: str(root / f"{name}.delta") for name in CONTINUOUS_FRONT_TABLES}
        defaults = {
            "string": "test",
            "timestamp": "2026-01-01T00:00:00Z",
            "int": 1,
            "bigint": 1,
            "double": 1.0,
            "boolean": False,
        }
        for name, path in paths.items():
            columns = continuous_front_store_contract()[name]["columns"]
            row = {column: defaults[kind] for column, kind in columns.items()}
            row.update(
                {
                    key: value
                    for key, value in {
                        "instrument_id": "BR",
                        "timeframe": "15m",
                        "status": "PASS",
                        "blocked_reason": None,
                    }.items()
                    if key in columns
                }
            )
            write_delta_table_rows(table_path=Path(path), columns=columns, rows=[row])
        return {
            "materialized_output_dir": str(root),
            "dataset_version": "test",
            "status": "PASS",
            "output_paths": paths,
            "rows_by_table": {name: 1 for name in paths},
            "qc_rows": [{"status": "PASS"}],
            "stage_timings": {"old": {"elapsed_seconds": 10}},
        }

    continuous_front_bars(producer)
    consumer = SimpleNamespace(
        run_id="research-consumer",
        op_config={**config, "code_version": "consumer-label"},
        instance=instance,
        dagster_run=SimpleNamespace(
            tags={
                "ta3000/upstream_job": "moex_cf_catch_up_job",
                "ta3000/upstream_run_id": "cf-producer",
            }
        ),
    )
    return continuous_front_bars, producer, consumer, calls, source_run, step, source


def test_verified_cf_checkpoint_reuses_exact_native_delta_versions(checkpoint):
    function, producer, consumer, calls, *_ = checkpoint
    source_path = owner._pending_path(producer, producer.op_config)
    before = json.loads(source_path.read_text())
    result = function(consumer)
    assert calls == ["cf-producer"]
    assert result["status"] == "PASS-NOOP"
    assert result["stage_timings"] == {}
    pending = json.loads(owner._pending_path(consumer, consumer.op_config).read_text())
    assert pending["quality_owners"]["continuous_front_bars"] == "cf-producer"
    for name in CONTINUOUS_FRONT_TABLES:
        assert pending["tables"][name] == before["tables"][name]
        with snapshots.materialization_snapshot_scope(
            snapshots.create_snapshot(
                tables={name: pending["tables"][name]},
                required_tables=[name],
                dataset_version="test",
                run_id="readback",
                code_version="test",
                quality_checks={"readback": "PASS"},
            )
        ):
            assert read_delta_table_arrow(
                Path(result["output_paths"][name]), filters=[("dataset_version", "=", "test")]
            )["dataset_version"].to_pylist() == ["test"]


@pytest.mark.parametrize(
    "change", ["policy", "output", "status", "job", "step", "legacy", "malformed"]
)
def test_ineligible_cf_checkpoint_recomputes(checkpoint, change):
    function, producer, consumer, calls, source_run, step, _ = checkpoint
    if change == "policy":
        consumer.op_config["continuous_front_policy"] = {"confirmation_bars": 2}
    elif change == "output":
        path = consumer.op_config["materialized_output_dir"] + "/continuous_front_bars.delta"
        write_deltalake(path, DeltaTable(path).to_pyarrow_table(), mode="append")
    elif change == "status":
        source_run.status.value = "FAILURE"
    elif change == "job":
        source_run.job_name = "other-job"
    elif change == "step":
        step.status.value = "FAILURE"
    else:
        path = owner._pending_path(producer, producer.op_config)
        payload = json.loads(path.read_text())
        if change == "malformed":
            payload["quality_checks"] = []
        else:
            payload["quality_evidence"]["continuous_front_bars"].pop("cf_result")
        snapshots.atomic_json(path, payload)
    assert function(consumer)["status"] == "PASS"
    assert calls == ["cf-producer", "research-consumer"]


def test_changed_current_data_remains_fail_closed(checkpoint):
    function, _, consumer, calls, _, _, source = checkpoint
    write_deltalake(str(source), pa.table({"id": [2]}), mode="append")
    with pytest.raises(snapshots.SnapshotUnavailableError):
        function(consumer)
    assert calls == ["cf-producer"]


def test_cf_checkpoint_cannot_certify_concurrent_new_output(checkpoint, monkeypatch):
    function, _, consumer, calls, *_ = checkpoint
    original = owner.assert_tables_unchanged
    changed = False

    def mutate_after_validation(tables):
        nonlocal changed
        original(tables)
        if not changed and set(CONTINUOUS_FRONT_TABLES).issubset(tables):
            changed = True
            path = tables["continuous_front_bars"]["table_path"]
            write_deltalake(path, DeltaTable(path).to_pyarrow_table(), mode="append")

    monkeypatch.setattr(owner, "assert_tables_unchanged", mutate_after_validation)
    with pytest.raises(snapshots.SnapshotUnavailableError):
        function(consumer)
    assert calls == ["cf-producer"]
    assert not owner._pending_path(consumer, consumer.op_config).exists()
