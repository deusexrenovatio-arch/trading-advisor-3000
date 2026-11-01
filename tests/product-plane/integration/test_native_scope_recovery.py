"""Real native producer -> metadata Data publication -> daily scope consumer."""

import copy
import os
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as storage
from trading_advisor_3000.product_plane.data_plane import data_snapshots as owner
from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_registry
from trading_advisor_3000.product_plane.research.refresh_scope import resolve_refresh_scope
from trading_advisor_3000.spark_jobs.snapshot_diff_job import compare_snapshot_tables

pytestmark = pytest.mark.skipif(os.name == "nt", reason="native Spark/Delta proof runs on Linux")


def spark_factory(app, master):
    jars = os.environ.get("TA3000_PROOF_DELTA_JARS")
    if not jars:
        from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session

        return _create_spark_session(app, master)
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(master)
        .appName(app)
        .config("spark.jars", jars)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


@pytest.fixture
def chain(tmp_path):
    canonical, output = tmp_path / "canonical", tmp_path / "research"
    path = canonical / "canonical_bars.delta"
    rows = [
        {
            "instrument_id": "FUT_BR",
            "timeframe": "15m",
            "ts": datetime(2024, 1, 1, tzinfo=UTC),
            "close": 1.0,
            "ts_close_year": 2024,
        },
        {
            "instrument_id": "FUT_BR",
            "timeframe": "15m",
            "ts": datetime(2026, 7, 30, tzinfo=UTC),
            "close": 2.0,
            "ts_close_year": 2026,
        },
    ]
    write_deltalake(str(path), pa.Table.from_pylist(rows), partition_by=["ts_close_year"])
    registry = canonical / ".data_snapshots"

    def publish(parent=None, full=False):
        tables = {"canonical_bars": storage.capture_table(path)}
        m = storage.create_snapshot(
            tables=tables,
            required_tables=list(tables),
            dataset_version="data",
            run_id="native",
            code_version="test",
            quality_checks={"producer": "PASS"},
            parents=[parent["snapshot_id"]] if parent else [],
            parameters={"downstream_scope": {"version": 2 if parent else 1, "full_refresh": full}},
        )
        storage.publish_snapshot(
            registry,
            storage.save_snapshot(registry, m),
            expected_current_id=parent["snapshot_id"] if parent else None,
        )
        return m

    source = publish()
    result = output / "accepted_output.delta"
    write_deltalake(str(result), pa.table({"result": [1.0]}))
    research_root = snapshot_registry(output, "nightly", "pit_active_front")
    research = storage.create_snapshot(
        tables={**source["tables"], "accepted_output": storage.capture_table(result)},
        required_tables=["canonical_bars", "accepted_output"],
        dataset_version="nightly",
        run_id="research",
        code_version="test",
        quality_checks={"native": "PASS"},
        parents=[source["snapshot_id"]],
        parameters={"refresh_scope_version": 1, "timeframes": ["15m"]},
    )
    storage.publish_snapshot(
        research_root, storage.save_snapshot(research_root, research), expected_current_id=None
    )
    write_deltalake(
        str(path),
        pa.Table.from_pylist([{**rows[-1], "close": 3.0}]),
        mode="overwrite",
        predicate="ts_close_year = 2026",
    )
    target = publish(source, full=True)
    return canonical, output, research_root, source, target, research, path


@pytest.mark.parametrize("change", ["append", "delete", "replace"])
def test_closed_session_dates_bind_old_and_new_effect_dates(chain, change):
    canonical, _, _, source, _, _, _ = chain
    path = canonical / "canonical_closed_session_dates.delta"
    old_row = {
        "instrument_id": "FUT_BR",
        "session_date": datetime(2026, 7, 29).date(),
        "policy_id": "closed.v1",
        "source_id": "MOEX",
        "source_document_hash": "old",
    }
    old_table = pa.Table.from_pylist([old_row])
    write_deltalake(str(path), old_table)
    previous = storage.capture_table(path)
    new_row = {**old_row, "session_date": datetime(2026, 7, 31).date()}
    rows = [new_row] if change != "delete" else []
    write_deltalake(
        str(path),
        pa.Table.from_pylist(rows, schema=old_table.schema),
        mode="append" if change == "append" else "overwrite",
    )
    current = storage.capture_table(path)
    report = compare_snapshot_tables(
        source_tables={**source["tables"], "canonical_closed_session_dates": previous},
        target_tables={**source["tables"], "canonical_closed_session_dates": current},
        timeframes=["15m"],
        not_before="2026-07-01T00:00:00Z",
        spark_session_factory=spark_factory,
    )
    effect = report["tables"]["canonical_closed_session_dates"]
    assert effect["changed_rows"] == (2 if change == "replace" else 1)
    assert effect["start_ts"] == (
        "2026-07-31T00:00:00Z" if change == "append" else "2026-07-29T00:00:00Z"
    )
    assert report["windows"][0]["end_ts"] == (
        "2026-07-30T00:00:00Z" if change == "delete" else "2026-08-01T00:00:00Z"
    )
    assert report["preserved_prefix_rows"] == 1
    assert storage.capture_table(path) == current


def test_native_diff_bridge_retains_immutable_history_and_admits_scoped_daily(chain):
    canonical, output, research_root, source, target, research, path = chain
    root = canonical / ".data_snapshots"
    saved = {
        m["snapshot_id"]: (root / "manifests" / f"{m['snapshot_id']}.json").read_bytes()
        for m in (source, target)
    }
    before = DeltaTable(str(path)).version()
    options = dict(
        canonical_root=canonical,
        research_root=research_root,
        not_before="2026-07-29T00:00:00Z",
        expected_data_id=target["snapshot_id"],
        expected_research_id=research["snapshot_id"],
        spark_session_factory=spark_factory,
    )
    value = owner.recover_data_scope(**options, publish=True)
    bridge = value["manifest"]
    report = value["report"]
    assert report["tables"]["canonical_bars"]["changed_rows"] == 2
    assert report["preserved_prefix_rows"] == 1
    assert bridge["parents"] == [target["snapshot_id"]] and bridge["tables"] == target["tables"]
    assert DeltaTable(str(path)).version() == before
    assert all(
        (root / "manifests" / f"{key}.json").read_bytes() == data for key, data in saved.items()
    )
    resolved = resolve_refresh_scope(
        data_manifest_path=root / "manifests" / f"{bridge['snapshot_id']}.json",
        materialized_output_dir=output,
        dataset_version="nightly",
        timeframes=["15m"],
    )
    assert resolved["outcome"] == "SCOPED" and resolved["force_full_refresh"] is False
    assert resolved["windows"][0]["start_ts"] == "2026-07-23T00:00:00Z"
    for mutate in ("research", "scope", "references"):
        damaged = copy.deepcopy(bridge)
        if mutate == "research":
            damaged["parameters"]["downstream_scope"]["recovery"]["research"]["snapshot_id"] = (
                "a" * 32
            )
        if mutate == "scope":
            damaged["parameters"]["downstream_scope"]["windows"] = []
        if mutate == "references":
            damaged["tables"]["canonical_bars"]["delta_version"] += 1
        with pytest.raises(ValueError):
            storage.validate_recovery_bridge(damaged, root=root, research=research)
    with pytest.raises(ValueError, match="parents changed"):
        owner.recover_data_scope(**options, publish=True)


def test_native_diff_does_not_truncate_a_change_before_operator_bound(chain):
    _, _, _, source, target, _, _ = chain
    with pytest.raises(ValueError, match="precedes explicit lower bound"):
        compare_snapshot_tables(
            source_tables=source["tables"],
            target_tables=target["tables"],
            timeframes=["15m"],
            not_before="2026-08-01T00:00:00Z",
            spark_session_factory=spark_factory,
        )


def test_native_compaction_only_is_zero_diff_and_deleted_old_pair_remains_dirty(chain):
    canonical, _, _, _, _, _, _ = chain
    path = canonical / "compaction.delta"
    rows = [
        {
            "instrument_id": "FUT_BR",
            "timeframe": "15m",
            "ts": datetime(2024, 1, 1, tzinfo=UTC),
            "close": 1.0,
            "ts_close_year": 2024,
        },
        {
            "instrument_id": "FUT_OLD",
            "timeframe": "15m",
            "ts": datetime(2026, 7, 30, tzinfo=UTC),
            "close": 2.0,
            "ts_close_year": 2026,
        },
    ]
    write_deltalake(str(path), pa.Table.from_pylist(rows), partition_by=["ts_close_year"])
    write_deltalake(
        str(path),
        pa.Table.from_pylist([{**rows[-1], "ts": datetime(2026, 7, 31, tzinfo=UTC)}]),
        mode="append",
    )
    original = storage.capture_table(path)
    table = DeltaTable(str(path))
    table.optimize.compact(target_size=512 * 1024 * 1024)
    compact = storage.capture_table(path)
    assert compact["delta_version"] > original["delta_version"]
    report = compare_snapshot_tables(
        source_tables={"canonical_bars": original},
        target_tables={"canonical_bars": compact},
        timeframes=["15m"],
        not_before="2026-07-29T00:00:00Z",
        spark_session_factory=spark_factory,
    )
    assert report["tables"]["canonical_bars"]["changed_rows"] == 0 and report["windows"] == []
    table = DeltaTable(str(path))
    table.delete("instrument_id = 'FUT_OLD'")
    deleted = storage.capture_table(path)
    report = compare_snapshot_tables(
        source_tables={"canonical_bars": original},
        target_tables={"canonical_bars": deleted},
        timeframes=["15m"],
        not_before="2026-07-29T00:00:00Z",
        spark_session_factory=spark_factory,
    )
    assert report["tables"]["canonical_bars"]["changed_rows"] == 2
    assert "FUT_OLD" in {row["instrument_id"] for row in report["windows"]}


def test_native_null_effect_time_is_not_hidden_by_min(chain):
    _, _, _, source, _, _, path = chain
    table = DeltaTable(str(path)).to_pyarrow_table()
    row = {
        "instrument_id": "FUT_BR",
        "timeframe": "15m",
        "ts": None,
        "close": 9.0,
        "ts_close_year": 2026,
    }
    write_deltalake(str(path), pa.Table.from_pylist([row], schema=table.schema), mode="append")
    current = storage.capture_table(path)
    with pytest.raises(ValueError, match="null or unparseable"):
        compare_snapshot_tables(
            source_tables=source["tables"],
            target_tables={"canonical_bars": current},
            timeframes=["15m"],
            not_before="2026-07-29T00:00:00Z",
            spark_session_factory=spark_factory,
        )


def test_research_change_after_data_cas_leaves_bridge_unusable(chain, monkeypatch):
    canonical, output, research_root, _, target, research, _ = chain
    original = owner.publish_snapshot

    def race(root, path, *, expected_current_id):
        original(root, path, expected_current_id=expected_current_id)
        revised = storage.create_snapshot(
            tables=research["tables"],
            required_tables=research["required_tables"],
            dataset_version=research["dataset_version"],
            run_id="concurrent-research",
            code_version="test",
            quality_checks=research["quality_checks"],
            parents=research["parents"],
            parameters=research["parameters"],
        )
        storage.publish_snapshot(
            research_root,
            storage.save_snapshot(research_root, revised),
            expected_current_id=research["snapshot_id"],
        )

    monkeypatch.setattr(owner, "publish_snapshot", race)
    with pytest.raises(ValueError, match="after Data CAS"):
        owner.recover_data_scope(
            canonical_root=canonical,
            research_root=research_root,
            not_before="2026-07-29T00:00:00Z",
            expected_data_id=target["snapshot_id"],
            expected_research_id=research["snapshot_id"],
            publish=True,
            spark_session_factory=spark_factory,
        )
    bridge = storage.current_snapshot(canonical / ".data_snapshots")
    result = resolve_refresh_scope(
        data_manifest_path=canonical
        / ".data_snapshots/manifests"
        / f"{bridge['snapshot_id']}.json",
        materialized_output_dir=output,
        dataset_version="nightly",
        timeframes=["15m"],
    )
    assert result["outcome"] == "BLOCKED" and "Research anchor changed" in result["reason"]
