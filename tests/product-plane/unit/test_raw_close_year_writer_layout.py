from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    replace_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.schemas.delta import (
    historical_data_delta_schema_manifest,
)

COLUMNS = {"ts_open": "timestamp", "ts_close": "timestamp", "close": "double"}


def test_raw_writer_uses_utc_close_year_and_preserves_business_values(tmp_path: Path) -> None:
    target = tmp_path / "raw_moex_history.delta"
    rows = [
        {"ts_open": "2025-12-31T23:59:00Z", "ts_close": "2026-01-01T00:00:00Z", "close": 100.0},
        {
            "ts_open": "2025-12-31T21:00:00Z",
            "ts_close": "2026-01-01T01:00:00+03:00",
            "close": 101.0,
        },
    ]
    write_delta_table_rows(table_path=target, rows=rows, columns=COLUMNS)
    table = DeltaTable(str(target))
    assert table.metadata().partition_columns == ["ts_close_year"]
    output = table.to_pyarrow_table().sort_by("close")
    assert output.column("ts_close_year").to_pylist() == [2026, 2025]
    assert output.column("ts_close").to_pylist() == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2025, 12, 31, 22, tzinfo=UTC),
    ]
    assert output.column("close").to_pylist() == [100.0, 101.0]


def test_raw_replace_rejects_legacy_layout_before_delete(tmp_path: Path) -> None:
    target = tmp_path / "raw_moex_history.delta"
    seed = pa.table(
        {
            "ts_open": [datetime(2025, 12, 31, 23, 59, tzinfo=UTC)],
            "ts_close": [datetime(2026, 1, 1, tzinfo=UTC)],
            "close": [100.0],
        }
    )
    write_deltalake(str(target), seed)
    before = DeltaTable(str(target))
    identity, version, files = before.metadata().id, before.version(), before.files()
    with pytest.raises(ValueError, match="layout.*migration"):
        replace_delta_table_rows(
            table_path=target, rows=[], columns=COLUMNS, predicate="close = 100"
        )
    after = DeltaTable(str(target))
    assert (after.metadata().id, after.version(), after.files()) == (identity, version, files)
    assert after.to_pyarrow_table().column("close").to_pylist() == [100.0]


def test_raw_contract_declares_measured_file_policy_without_changing_business_key() -> None:
    raw = historical_data_delta_schema_manifest()["raw_moex_history"]
    assert raw["file_policy"]["max_compressed_file_bytes"] == 512 * 1024 * 1024
    assert raw["file_policy"]["files_per_nonempty_partition"] == 1
    assert raw["file_policy"]["allow_empty_partitions"] is False
    assert raw["file_policy"]["oversize_action"] == "fail_closed_use_isolated_finalizer"
    assert "unique(internal_id, timeframe, moex_secid, ts_open, ts_close)" in raw["constraints"]


def test_fresh_raw_batch_stream_keeps_identity_pins_and_compacts_once(tmp_path: Path) -> None:
    from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
        write_delta_table_row_batches,
    )

    target = tmp_path / "raw_moex_history.delta"
    batches = [
        [
            {
                "ts_open": f"2025-12-31T23:{minute:02d}:00Z",
                "ts_close": f"2025-12-31T23:{minute + 1:02d}:00Z",
                "close": float(minute),
            }
        ]
        for minute in range(3)
    ]
    assert write_delta_table_row_batches(
        table_path=target, row_batches=batches, columns=COLUMNS, max_rows_per_delta_write=1
    ) == (3, 3)
    table = DeltaTable(str(target))
    pinned = DeltaTable(str(target), version=0)
    assert table.metadata().id == pinned.metadata().id
    assert pinned.to_pyarrow_table().column("close").to_pylist() == [0.0]
    assert sorted(table.to_pyarrow_table().column("close").to_pylist()) == [0.0, 1.0, 2.0]
    assert len(table.get_add_actions().to_pylist()) == 1
    assert len(list(target.rglob("*.parquet"))) > 1  # Retained tombstones support old pins.
    before = table.version()
    with pytest.raises(ValueError, match="existing raw Arrow target"):
        write_delta_table_rows(table_path=target, rows=[], columns=COLUMNS)
    assert DeltaTable(str(target)).version() == before


def test_empty_fresh_raw_has_partition_metadata_and_no_empty_data_files(tmp_path: Path) -> None:
    target = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=target, rows=[], columns=COLUMNS)
    table = DeltaTable(str(target))
    assert table.metadata().partition_columns == ["ts_close_year"]
    assert "ts_close_year" in table.schema().to_pyarrow().names
    assert table.get_add_actions().num_rows == 0


def test_supplied_arrow_year_is_recomputed_from_close(tmp_path: Path) -> None:
    target = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=target,
        columns={**COLUMNS, "ts_close_year": "int"},
        rows=[
            {
                "ts_open": "2025-12-31T23:59:00Z",
                "ts_close": "2026-01-01T00:00:00Z",
                "close": 10.0,
                "ts_close_year": 1999,
            }
        ],
    )
    assert DeltaTable(str(target)).to_pyarrow_table().column("ts_close_year").to_pylist() == [2026]


@pytest.mark.parametrize("operation", ["write", "batch", "replace", "ensure"])
def test_flat_with_year_is_rejected_before_any_raw_mutation(tmp_path: Path, operation: str) -> None:
    from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
        ensure_delta_table_columns,
        write_delta_table_row_batches,
    )

    target = tmp_path / "raw_moex_history.delta"
    seed = pa.table(
        {
            "ts_open": [datetime(2025, 12, 31, 23, 59, tzinfo=UTC)],
            "ts_close": [datetime(2026, 1, 1, tzinfo=UTC)],
            "close": [100.0],
            "ts_close_year": pa.array([2026], type=pa.int32()),
        }
    )
    write_deltalake(str(target), seed)
    before = DeltaTable(str(target))
    identity, version, files = before.metadata().id, before.version(), before.files()
    with pytest.raises(ValueError, match="layout.*migration"):
        if operation == "write":
            write_delta_table_rows(table_path=target, rows=[], columns=COLUMNS)
        elif operation == "batch":
            write_delta_table_row_batches(
                table_path=target,
                row_batches=[],
                columns=COLUMNS,
                max_rows_per_delta_write=1,
                replace_predicate="close = 100",
            )
        elif operation == "replace":
            replace_delta_table_rows(
                table_path=target, rows=[], columns=COLUMNS, predicate="close = 100"
            )
        else:
            ensure_delta_table_columns(table_path=target, columns={**COLUMNS, "extra": "string"})
    after = DeltaTable(str(target))
    assert (after.metadata().id, after.version(), after.files()) == (identity, version, files)
    assert after.to_pyarrow_table().column("close").to_pylist() == [100.0]


@pytest.mark.parametrize("kind", ["null", "oversize", "empty", "fragmented"])
def test_measured_active_file_contract_fails_closed(kind: str) -> None:
    from trading_advisor_3000.product_plane.common.table_layout import validate_file_actions

    action = {"partition.ts_close_year": "2026", "size_bytes": 10, "num_records": 1}
    if kind == "null":
        action["partition.ts_close_year"] = None
    if kind == "oversize":
        action["size_bytes"] = 513
    if kind == "empty":
        action["num_records"] = 0
    actions = [action, dict(action)] if kind == "fragmented" else [action]
    with pytest.raises(ValueError):
        validate_file_actions(actions, max_file_bytes=512)


def test_size_dependent_multiple_files_and_unactivated_families_are_supported(
    tmp_path: Path,
) -> None:
    from trading_advisor_3000.product_plane.common.table_layout import validate_file_actions

    validate_file_actions(
        [
            {"partition.ts_close_year": "2026", "size_bytes": 512, "num_records": 1},
            {"partition.ts_close_year": "2026", "size_bytes": 1, "num_records": 1},
        ],
        max_file_bytes=512,
    )
    target = tmp_path / "canonical_bars.delta"
    write_delta_table_rows(
        table_path=target, rows=[{"ts": "2026-01-01T00:00:00Z"}], columns={"ts": "timestamp"}
    )
    assert DeltaTable(str(target)).metadata().partition_columns == []
    assert DeltaTable(str(target)).schema().to_pyarrow().names == ["ts"]


def test_raw_arrow_candidate_rejects_changed_stream_identity_before_append(tmp_path: Path) -> None:
    from trading_advisor_3000.product_plane.common.table_layout import write_layout_arrow_table

    target = tmp_path / "raw_moex_history.delta"
    table = pa.table({"ts_close": [datetime(2026, 1, 1, tzinfo=UTC)]})
    write_layout_arrow_table(table_path=target, table=table, mode="overwrite", compact=False)
    before = DeltaTable(str(target)).version()
    with pytest.raises(ValueError, match="owned raw candidate"):
        write_layout_arrow_table(
            table_path=target, table=table, mode="append", candidate_id="wrong-owner"
        )
    assert DeltaTable(str(target)).version() == before


def test_noop_raw_retry_rejects_fragmented_active_files_without_new_commit(
    tmp_path: Path, monkeypatch
) -> None:
    from unittest.mock import MagicMock

    from trading_advisor_3000.spark_jobs import moex_raw_ingest_job as raw_job

    target = tmp_path / "raw_moex_history.delta"
    seed = pa.table(
        {
            "ts_close": [datetime(2026, 1, 1, tzinfo=UTC)],
            "ts_close_year": pa.array([2026], type=pa.int32()),
        }
    )
    write_deltalake(str(target), seed, partition_by=["ts_close_year"])
    write_deltalake(str(target), seed, mode="append", partition_by=["ts_close_year"])
    before = DeltaTable(str(target))
    assert len(before.files()) == 2
    fake_frame = MagicMock()
    for method in ("cache", "select", "distinct"):
        getattr(fake_frame, method).return_value = fake_frame
    fake_frame.count.return_value = 0
    fake_spark = MagicMock()
    fake_spark.createDataFrame.return_value = fake_frame
    monkeypatch.setattr(raw_job, "_ensure_spark_delta_runtime_available", lambda: None)
    monkeypatch.setattr(raw_job, "_load_spark_modules", lambda: (None, None, None, None))
    with pytest.raises(ValueError, match="files in small year"):
        raw_job.run_moex_raw_ingest_spark_delta_job(
            table_path=target,
            source_rows=[],
            window_scopes=[],
            initial_watermarks={},
            run_id="postcommit-retry",
            ingest_till_utc="2026-01-01T00:00:00Z",
            refresh_overlap_minutes=0,
            progress_path=tmp_path / "progress.jsonl",
            progress_latest_path=tmp_path / "progress.json",
            error_path=tmp_path / "error.jsonl",
            error_latest_path=tmp_path / "error.json",
            spark_session_factory=lambda *_: fake_spark,
        )
    assert DeltaTable(str(target)).version() == before.version()


@pytest.mark.parametrize("operation", ["append", "finalize"])
@pytest.mark.parametrize("missing_log", [False, True], ids=["absent-target", "missing-log"])
def test_owned_raw_stream_never_recreates_a_vanished_target(
    tmp_path: Path,
    operation: str,
    missing_log: bool,
) -> None:
    from trading_advisor_3000.product_plane.common.table_layout import (
        compact_default_table,
        write_layout_arrow_table,
    )

    target = tmp_path / "vanished" / "raw_moex_history.delta"
    if missing_log:
        target.mkdir(parents=True)
        (target / "existing-marker").write_text("unchanged", encoding="utf-8")
    with pytest.raises(ValueError, match="owned raw candidate"):
        if operation == "append":
            write_layout_arrow_table(
                table_path=target,
                table=pa.table({"ts_close": [datetime(2026, 1, 1, tzinfo=UTC)]}),
                mode="append",
                candidate_id="created-earlier",
            )
        else:
            compact_default_table(target, candidate_id="created-earlier")
    assert not (target / "_delta_log").exists()
    if missing_log:
        assert list(target.iterdir()) == [target / "existing-marker"]
        assert (target / "existing-marker").read_text() == "unchanged"
    else:
        assert not target.exists()


@pytest.mark.parametrize("direction", ["target_inside_source", "source_inside_target"])
def test_raw_migration_rejects_ancestor_overlap_before_spark(
    tmp_path: Path, monkeypatch, direction: str
) -> None:
    from trading_advisor_3000.spark_jobs import moex_raw_layout_migration_job as migration

    source = tmp_path / "source.delta"
    write_deltalake(str(source), pa.table({"id": [1]}))
    target = source / "nested.delta" if direction == "target_inside_source" else tmp_path

    def unexpected_spark():
        raise AssertionError("overlap guard must precede Spark")

    monkeypatch.setattr(migration, "_ensure_spark_delta_runtime_available", unexpected_spark)
    with pytest.raises(ValueError, match="must not overlap"):
        migration.run_moex_raw_layout_migration_spark_job(
            source_table_path=source,
            staged_table_path=target,
            report_path=tmp_path / "report.json",
            run_id="overlap",
        )
    assert DeltaTable(str(source)).version() == 0


def test_raw_migration_rejects_ntz_before_timestamp_normalization() -> None:
    from unittest.mock import MagicMock

    from pyspark.sql.types import StructField, StructType, TimestampNTZType

    from trading_advisor_3000.spark_jobs import moex_raw_layout_migration_job as migration

    frame = MagicMock()
    frame.schema = StructType(
        [StructField("ts_open", TimestampNTZType()), StructField("ts_close", TimestampNTZType())]
    )
    with pytest.raises(ValueError, match="timestamp_ntz"):
        migration._normalized_raw_frame(frame, MagicMock())
    frame.select.assert_not_called()
