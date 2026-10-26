"""Native Dagster re-execution with a real Delta checkpoint, no market history."""

from pathlib import Path

import pyarrow as pa
import pytest
from dagster import (
    DagsterInstance,
    ReexecutionOptions,
    execute_job,
    in_process_executor,
    job,
    op,
    reconstructable,
)
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common.data_snapshots import (
    capture_table,
    create_snapshot,
    publish_snapshot,
    save_snapshot,
)
from trading_advisor_3000.product_plane.research.data_snapshots import (
    _initial,
    _pending_path,
    snapshot_materialization,
)


@op(config_schema=dict)
@snapshot_materialization(("research_indicator_frames",))
def checkpoint_l1(context):
    root = Path(context.op_config["materialized_output_dir"])
    count = root / "l1-calls.txt"
    count.write_text(str(int(count.read_text()) + 1) if count.exists() else "1")
    write_deltalake(
        str(root / "research_indicator_frames.delta"),
        pa.table(
            {
                "ts_close_year": [2026],
                "value": [1.0],
                "dataset_version": ["fixture"],
                "contour_id": ["native_tradable"],
                "series_mode": ["contract"],
                "series_id": ["contract-1"],
                "indicator_set_version": ["indicators-v1"],
                "timeframe": ["15m"],
                "ts": ["2026-10-12T10:00:00Z"],
            }
        ),
        partition_by=["ts_close_year"],
    )
    return {"status": "PASS"}


@op(config_schema=dict)
@snapshot_materialization(())
def checkpoint_r2(context, checkpoint_l1):
    root = Path(context.op_config["materialized_output_dir"])
    flag = root / "first-r2-failed"
    if not flag.exists():
        flag.write_text("failed")
        raise RuntimeError("simulated R2 worker failure")
    pending = _initial(context, context.op_config)
    assert pending["quality_owners"]["checkpoint_l1"] == context.dagster_run.parent_run_id
    return {"status": "PASS"}


@job(executor_def=in_process_executor)
def checkpoint_job():
    checkpoint_r2(checkpoint_l1())


def reconstruct_checkpoint_job():
    return checkpoint_job


@pytest.mark.parametrize("changed", [None, "input", "config", "l1", "disabled"])
def test_native_from_failure_reuses_l1_and_transfers_verified_checkpoint(tmp_path, changed):
    canonical = tmp_path / "canonical"
    output = tmp_path / "research"
    canonical.mkdir()
    output.mkdir()
    table = canonical / "canonical_bars.delta"
    write_deltalake(str(table), pa.table({"value": [1.0]}))
    manifest = create_snapshot(
        tables={"canonical_bars": capture_table(table)},
        required_tables=("canonical_bars",),
        dataset_version="data-fixture",
        run_id="data-fixture",
        code_version="fixture",
        quality_checks={"canonical": "PASS"},
    )
    registry = canonical / ".data_snapshots"
    path = save_snapshot(registry, manifest)
    publish_snapshot(registry, path, expected_current_id=None)
    config = {
        "canonical_output_dir": str(canonical),
        "materialized_output_dir": str(output),
        "dataset_version": "recovery-fixture",
        "code_version": "fixture",
        "data_snapshot_enabled": changed != "disabled",
    }
    from trading_advisor_3000.product_plane.research.admission import configure_admission

    config = configure_admission(config, operation="rebuild")
    run_config = {"ops": {name: {"config": config} for name in ("checkpoint_l1", "checkpoint_r2")}}
    (tmp_path / "instance").mkdir()
    with DagsterInstance.local_temp(str(tmp_path / "instance")) as instance:
        with execute_job(
            reconstructable(reconstruct_checkpoint_job),
            instance=instance,
            run_config=run_config,
            raise_on_error=False,
        ) as original:
            assert not original.success
            parent_id = original.run_id
        if changed == "input":
            write_deltalake(str(table), pa.table({"value": [2.0]}), mode="overwrite")
        if changed == "config":
            config["code_version"] = "different-code"
        if changed == "l1":
            l1 = output / "research_indicator_frames.delta"
            rows = DeltaTable(str(l1)).to_pyarrow_table()
            rows = rows.set_column(rows.schema.get_field_index("value"), "value", pa.array([2.0]))
            write_deltalake(str(l1), rows, mode="overwrite", partition_by=["ts_close_year"])
        with execute_job(
            reconstructable(reconstruct_checkpoint_job),
            instance=instance,
            reexecution_options=ReexecutionOptions.from_failure(parent_id, instance),
            run_config=run_config,
            raise_on_error=False,
        ) as recovered:
            assert recovered.success == (changed is None)
            assert (output / "l1-calls.txt").read_text() == "1"
            context = type("Context", (), {"run_id": recovered.run_id})()
            import json

            if changed:
                assert not _pending_path(context, config).exists()
                return

            checkpoint = json.loads(_pending_path(context, config).read_text())
            assert "research_indicator_frames" in checkpoint["tables"]
            assert checkpoint["recovery_lineage"] == [parent_id]
            assert checkpoint["quality_owners"]["checkpoint_r2"] == recovered.run_id


def test_native_step_evidence_is_required_for_reuse():
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.research.data_snapshots import _assert_quality_owners

    instance = SimpleNamespace(get_run_step_stats=lambda _: [])
    with pytest.raises(ValueError, match="native successful step evidence"):
        _assert_quality_owners(
            instance, {"quality_owners": {"checkpoint_l1": "old"}}, current_run_id="new"
        )
