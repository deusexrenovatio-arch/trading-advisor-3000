from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.schemas.delta import (
    historical_data_delta_schema_manifest,
)
from trading_advisor_3000.spark_jobs import moex_raw_layout_migration_job


def _delta_dir(path: Path, marker: str) -> None:
    (path / "_delta_log").mkdir(parents=True)
    (path / marker).write_text(marker, encoding="utf-8")


def test_raw_moex_history_manifest_declares_year_partitioned_layout() -> None:
    manifest = historical_data_delta_schema_manifest()

    assert manifest["raw_moex_history"]["partition_by"] == [
        "ts_close_year",
    ]
    assert manifest["raw_moex_history"]["columns"]["ts_close_year"] == "int"
    assert (
        "unique(internal_id, timeframe, moex_secid, ts_open, ts_close)"
        in manifest["raw_moex_history"]["constraints"]
    )


def test_raw_layout_migration_uses_spark_partitioned_rewrite_and_validation() -> None:
    source = inspect.getsource(
        moex_raw_layout_migration_job.run_moex_raw_layout_migration_spark_job
    )
    storage_frame_source = inspect.getsource(moex_raw_layout_migration_job._raw_storage_frame)
    dedup_source = inspect.getsource(moex_raw_layout_migration_job._deduplicated_raw_frame)
    storage_expr_source = inspect.getsource(moex_raw_layout_migration_job._storage_column_expr)
    conflict_source = inspect.getsource(
        moex_raw_layout_migration_job._conflicting_duplicate_key_count
    )
    watermark_source = inspect.getsource(moex_raw_layout_migration_job._watermark_mismatch_count)

    assert 'spark.read.format("delta").load(str(source_table_path))' in source
    assert "normalized_source_df = _normalized_raw_frame(source_raw_df, functions)" in source
    assert "_raw_storage_frame(normalized_source_df, functions)" in source
    assert "_with_raw_layout_columns" in storage_frame_source
    assert "functions.to_timestamp(source_column)" in storage_expr_source
    assert "functions.max_by(value_struct, order_struct)" in dedup_source
    assert ".groupBy(*RAW_KEY_COLUMNS)" in dedup_source
    assert "deduplicated_row_count" in source
    assert "conflicting_duplicate_key_count" in source
    assert "countDistinct" in conflict_source
    assert "migrated_row_count == source_key_count" in source
    assert 'functions.to_timestamp("ts_close")' in watermark_source
    assert ".repartition(*RAW_LAYOUT_PARTITION_COLUMNS)" in source
    assert ".partitionBy(*RAW_LAYOUT_PARTITION_COLUMNS)" in source
    assert "source_row_count" in source
    assert "migrated_row_count" in source
    assert "duplicate_key_count" in source
    assert "watermark_mismatch_count" in source
    assert "file_profile_passes" in source
    assert "partition_columns" in source
    assert '"runtime_owner": "spark_delta"' in source
    assert "shutil" not in source
    assert ".cache()" not in source


def test_raw_layout_schema_validation_accepts_spark_physical_json_type() -> None:
    class _SparkFrame:
        dtypes = [
            (
                column_name,
                "string"
                if type_name == "json"
                else moex_raw_layout_migration_job._spark_dtype_name(type_name),
            )
            for column_name, type_name in moex_raw_layout_migration_job.RAW_STORAGE_COLUMNS.items()
        ]

    assert moex_raw_layout_migration_job._raw_storage_schema_matches(_SparkFrame())


def test_raw_layout_file_profile_ignores_delta_log_checkpoints(tmp_path: Path) -> None:
    table = tmp_path / "raw_moex_history.delta"
    data_file = table / "ts_close_year=2026" / "part-00000.parquet"
    checkpoint_file = table / "_delta_log" / "00000000000000000010.checkpoint.parquet"
    data_file.parent.mkdir(parents=True)
    checkpoint_file.parent.mkdir(parents=True)
    data_file.write_bytes(b"data")
    checkpoint_file.write_bytes(b"checkpoint")

    profile = moex_raw_layout_migration_job._parquet_file_profile(table)

    assert profile["parquet_files"] == 1
    assert profile["total_bytes"] == len(b"data")


def test_raw_layout_promote_requires_passed_report_and_moves_roots(tmp_path: Path) -> None:
    current = tmp_path / "raw_moex_history.delta"
    staged = tmp_path / "raw_moex_history.layout-staged.delta"
    backup = tmp_path / "raw_moex_history.pre-layout-backup.delta"
    report = tmp_path / "raw-layout-migration-report.json"
    _delta_dir(current, "old-root.txt")
    _delta_dir(staged, "new-root.txt")
    report.write_text(
        json.dumps(
            {
                "status": "PASS",
                "staged_table_path": staged.as_posix(),
                "partition_columns": {
                    "actual": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                    "expected": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                },
                "file_profile_passes": True,
            }
        ),
        encoding="utf-8",
    )

    result = moex_raw_layout_migration_job.promote_moex_raw_layout_migration(
        current_table_path=current,
        staged_table_path=staged,
        backup_table_path=backup,
        report_path=report,
    )

    assert result["status"] == "PASS"
    assert (current / "new-root.txt").exists()
    assert (backup / "old-root.txt").exists()
    assert not staged.exists()


def test_raw_layout_promote_requires_report_staged_path(tmp_path: Path) -> None:
    staged = tmp_path / "raw_moex_history.layout-staged.delta"
    report = tmp_path / "raw-layout-migration-report.json"
    report.write_text(
        json.dumps(
            {
                "status": "PASS",
                "partition_columns": {
                    "actual": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                    "expected": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                },
                "file_profile_passes": True,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="missing staged_table_path"):
        moex_raw_layout_migration_job._load_passed_migration_report(report, staged)


def test_raw_layout_promote_blocks_report_without_file_profile_proof(tmp_path: Path) -> None:
    current = tmp_path / "raw_moex_history.delta"
    staged = tmp_path / "raw_moex_history.layout-staged.delta"
    backup = tmp_path / "raw_moex_history.pre-layout-backup.delta"
    report = tmp_path / "raw-layout-migration-report.json"
    _delta_dir(current, "old-root.txt")
    _delta_dir(staged, "new-root.txt")
    report.write_text(
        json.dumps(
            {
                "status": "PASS",
                "staged_table_path": staged.as_posix(),
                "partition_columns": {
                    "actual": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                    "expected": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                },
                "file_profile_passes": False,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="improved file profile"):
        moex_raw_layout_migration_job.promote_moex_raw_layout_migration(
            current_table_path=current,
            staged_table_path=staged,
            backup_table_path=backup,
            report_path=report,
        )

    assert (current / "old-root.txt").exists()
    assert (staged / "new-root.txt").exists()
    assert not backup.exists()


def test_raw_layout_promote_restores_backup_after_partial_current_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = tmp_path / "raw_moex_history.delta"
    staged = tmp_path / "raw_moex_history.layout-staged.delta"
    backup = tmp_path / "raw_moex_history.pre-layout-backup.delta"
    report = tmp_path / "raw-layout-migration-report.json"
    _delta_dir(current, "old-root.txt")
    _delta_dir(staged, "new-root.txt")
    report.write_text(
        json.dumps(
            {
                "status": "PASS",
                "staged_table_path": staged.as_posix(),
                "partition_columns": {
                    "actual": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                    "expected": list(moex_raw_layout_migration_job.RAW_LAYOUT_PARTITION_COLUMNS),
                },
                "file_profile_passes": True,
            }
        ),
        encoding="utf-8",
    )
    real_move = moex_raw_layout_migration_job.shutil.move

    def _move(src: str, dst: str) -> str:
        src_path = Path(src)
        dst_path = Path(dst)
        if src_path == current and dst_path == backup:
            return real_move(src, dst)
        if src_path == staged and dst_path == current:
            current.mkdir(parents=True, exist_ok=True)
            (current / "partial-root.txt").write_text("partial", encoding="utf-8")
            raise RuntimeError("staged move failed")
        return real_move(src, dst)

    monkeypatch.setattr(moex_raw_layout_migration_job.shutil, "move", _move)

    with pytest.raises(RuntimeError, match="staged move failed"):
        moex_raw_layout_migration_job.promote_moex_raw_layout_migration(
            current_table_path=current,
            staged_table_path=staged,
            backup_table_path=backup,
            report_path=report,
        )

    assert (current / "old-root.txt").exists()
    assert not (current / "partial-root.txt").exists()
    assert (staged / "new-root.txt").exists()
    assert not backup.exists()


def test_raw_layout_promote_blocks_failed_report_without_moving_roots(tmp_path: Path) -> None:
    current = tmp_path / "raw_moex_history.delta"
    staged = tmp_path / "raw_moex_history.layout-staged.delta"
    backup = tmp_path / "raw_moex_history.pre-layout-backup.delta"
    report = tmp_path / "raw-layout-migration-report.json"
    _delta_dir(current, "old-root.txt")
    _delta_dir(staged, "new-root.txt")
    report.write_text(json.dumps({"status": "BLOCKED"}), encoding="utf-8")

    with pytest.raises(RuntimeError, match="passed migration report"):
        moex_raw_layout_migration_job.promote_moex_raw_layout_migration(
            current_table_path=current,
            staged_table_path=staged,
            backup_table_path=backup,
            report_path=report,
        )

    assert (current / "old-root.txt").exists()
    assert (staged / "new-root.txt").exists()
    assert not backup.exists()
