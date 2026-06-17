from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session, _load_spark_modules
from .moex_raw_ingest_job import (
    KEY_SCOPE_COLUMNS,
    RAW_COLUMNS,
    RAW_KEY_COLUMNS,
    RAW_LAYOUT_PARTITION_COLUMNS,
    RAW_STORAGE_COLUMNS,
    _ensure_spark_delta_runtime_available,
    _with_raw_layout_columns,
)

RAW_LAYOUT_MIGRATION_REPORT_FILENAME = "raw-layout-migration-report.json"
MIN_STAGED_AVERAGE_PARQUET_BYTES = 128 * 1024
MIGRATION_VALUE_COLUMN = "_migration_value"
MIGRATION_PAYLOAD_HASH_COLUMN = "_migration_payload_hash"
RAW_VOLATILE_DEDUP_COLUMNS = frozenset({"ingest_run_id", "ingested_at_utc", "provenance_json"})


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _delta_log_payload(path: Path) -> dict[str, object]:
    return {"path": path.as_posix(), "delta_log": has_delta_log(path)}


def _parquet_file_profile(path: Path) -> dict[str, object]:
    files = list(path.rglob("*.parquet")) if path.exists() else []
    sizes = [file.stat().st_size for file in files if file.is_file()]
    total_bytes = sum(sizes)
    return {
        "parquet_files": len(sizes),
        "total_bytes": total_bytes,
        "average_bytes": int(total_bytes / len(sizes)) if sizes else 0,
        "min_bytes": min(sizes) if sizes else 0,
        "max_bytes": max(sizes) if sizes else 0,
    }


def _file_profile_passes(
    *, source_profile: Mapping[str, object], staged_profile: Mapping[str, object]
) -> bool:
    source_files = int(source_profile.get("parquet_files") or 0)
    staged_files = int(staged_profile.get("parquet_files") or 0)
    staged_average = int(staged_profile.get("average_bytes") or 0)
    if source_files <= 0 or staged_files <= 0:
        return False
    return staged_files < source_files and staged_average >= MIN_STAGED_AVERAGE_PARQUET_BYTES


def _delta_table_partition_columns(spark: Any, table_path: Path) -> tuple[str, ...] | None:
    if not has_delta_log(table_path):
        return None
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    rows = DeltaTable.forPath(spark, str(table_path)).detail().select("partitionColumns").collect()
    if not rows:
        return ()
    return tuple(str(item) for item in (rows[0]["partitionColumns"] or []))


def _storage_column_expr(functions: Any, column: str, type_name: str) -> Any:
    normalized = type_name.strip().lower()
    source_column = functions.col(column)
    if normalized == "timestamp":
        return functions.to_timestamp(source_column).alias(column)
    if normalized in {"int", "integer"}:
        return source_column.cast("int").alias(column)
    if normalized in {"bigint", "long"}:
        return source_column.cast("bigint").alias(column)
    if normalized in {"double", "float"}:
        return source_column.cast("double").alias(column)
    return source_column.cast("string").alias(column)


def _normalized_raw_frame(dataframe: Any, functions: Any) -> Any:
    return dataframe.select(
        *[
            _storage_column_expr(functions, column, type_name)
            for column, type_name in RAW_COLUMNS.items()
        ]
    )


def _deduplicated_raw_frame(dataframe: Any, functions: Any) -> Any:
    value_columns = [column for column in RAW_COLUMNS if column not in RAW_KEY_COLUMNS]
    value_struct = functions.struct(
        *[functions.col(column).alias(column) for column in value_columns]
    )
    order_struct = functions.struct(
        functions.col("ingested_at_utc").alias("ingested_at_utc"),
        functions.coalesce(functions.col("ingest_run_id"), functions.lit("")).alias(
            "ingest_run_id"
        ),
        functions.coalesce(functions.col("provenance_json"), functions.lit("")).alias(
            "provenance_json"
        ),
    )
    deduplicated = dataframe.groupBy(*RAW_KEY_COLUMNS).agg(
        functions.max_by(value_struct, order_struct).alias(MIGRATION_VALUE_COLUMN)
    )
    return deduplicated.select(
        *[functions.col(column).alias(column) for column in RAW_KEY_COLUMNS],
        *[
            functions.col(f"{MIGRATION_VALUE_COLUMN}.{column}").alias(column)
            for column in value_columns
        ],
    )


def _raw_storage_frame(normalized_dataframe: Any, functions: Any) -> Any:
    deduplicated = _deduplicated_raw_frame(normalized_dataframe, functions)
    return _with_raw_layout_columns(deduplicated, functions)


def _spark_dtype_name(type_name: str) -> str:
    normalized = type_name.strip().lower()
    if normalized == "json":
        return "string"
    if normalized == "integer":
        return "int"
    if normalized == "long":
        return "bigint"
    return normalized


def _raw_storage_schema_matches(dataframe: Any) -> bool:
    actual = {str(column_name): str(data_type) for column_name, data_type in dataframe.dtypes}
    return all(
        actual.get(column_name) == _spark_dtype_name(type_name)
        for column_name, type_name in RAW_STORAGE_COLUMNS.items()
    )


def _watermark_mismatch_count(*, source_df: Any, migrated_df: Any, functions: Any) -> int:
    source_watermarks = source_df.groupBy(*KEY_SCOPE_COLUMNS).agg(
        functions.max(functions.to_timestamp("ts_close")).alias("source_ts_close")
    )
    migrated_watermarks = migrated_df.groupBy(*KEY_SCOPE_COLUMNS).agg(
        functions.max("ts_close").alias("migrated_ts_close")
    )
    mismatched = source_watermarks.join(
        migrated_watermarks,
        list(KEY_SCOPE_COLUMNS),
        "full_outer",
    ).where(
        functions.col("source_ts_close").isNull()
        | functions.col("migrated_ts_close").isNull()
        | (~functions.col("source_ts_close").eqNullSafe(functions.col("migrated_ts_close")))
    )
    return int(mismatched.count())


def _duplicate_key_count(dataframe: Any, functions: Any) -> int:
    duplicates = (
        dataframe.groupBy(*RAW_KEY_COLUMNS)
        .agg(functions.count("*").alias("_count"))
        .where(functions.col("_count") > 1)
    )
    return int(duplicates.count())


def _conflicting_duplicate_key_count(dataframe: Any, functions: Any) -> int:
    payload_columns = [column for column in RAW_COLUMNS if column not in RAW_VOLATILE_DEDUP_COLUMNS]
    payload_hash = functions.sha2(
        functions.to_json(
            functions.struct(
                *[functions.col(column).cast("string").alias(column) for column in payload_columns]
            )
        ),
        256,
    )
    conflicts = (
        dataframe.withColumn(MIGRATION_PAYLOAD_HASH_COLUMN, payload_hash)
        .groupBy(*RAW_KEY_COLUMNS)
        .agg(functions.countDistinct(MIGRATION_PAYLOAD_HASH_COLUMN).alias("_payload_versions"))
        .where(functions.col("_payload_versions") > 1)
    )
    return int(conflicts.count())


def run_moex_raw_layout_migration_spark_job(
    *,
    source_table_path: Path,
    staged_table_path: Path,
    report_path: Path,
    run_id: str,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
    overwrite_staged: bool = False,
) -> dict[str, object]:
    if source_table_path.resolve() == staged_table_path.resolve():
        raise ValueError("source_table_path and staged_table_path must be different paths")
    if not has_delta_log(source_table_path):
        raise FileNotFoundError(
            f"raw source Delta table is missing _delta_log: {source_table_path.as_posix()}"
        )
    if staged_table_path.exists() and not overwrite_staged:
        raise FileExistsError(
            "staged raw layout table already exists; remove it or pass overwrite_staged=True: "
            f"{staged_table_path.as_posix()}"
        )

    _ensure_spark_delta_runtime_available()
    spark_session_factory = spark_session_factory or _create_spark_session
    spark = spark_session_factory("ta3000-moex-raw-layout-migration", spark_master)
    _, _, functions, _ = _load_spark_modules()
    try:
        source_raw_df = spark.read.format("delta").load(str(source_table_path)).select(*RAW_COLUMNS)
        normalized_source_df = _normalized_raw_frame(source_raw_df, functions)
        staged_df = _raw_storage_frame(normalized_source_df, functions)
        source_row_count = int(normalized_source_df.count())
        source_key_count = int(normalized_source_df.select(*RAW_KEY_COLUMNS).distinct().count())
        deduplicated_row_count = source_row_count - source_key_count
        conflicting_duplicate_key_count = _conflicting_duplicate_key_count(
            normalized_source_df, functions
        )
        source_file_profile = _parquet_file_profile(source_table_path)
        staged_table_path.parent.mkdir(parents=True, exist_ok=True)
        (
            staged_df.repartition(*RAW_LAYOUT_PARTITION_COLUMNS)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(*RAW_LAYOUT_PARTITION_COLUMNS)
            .save(str(staged_table_path))
        )

        staged_file_profile = _parquet_file_profile(staged_table_path)
        migrated_df = spark.read.format("delta").load(str(staged_table_path))
        migrated_row_count = int(migrated_df.count())
        migrated_key_count = int(migrated_df.select(*RAW_KEY_COLUMNS).distinct().count())
        duplicate_key_count = _duplicate_key_count(migrated_df, functions)
        watermark_mismatch_count = _watermark_mismatch_count(
            source_df=normalized_source_df,
            migrated_df=migrated_df,
            functions=functions,
        )
        partition_columns = _delta_table_partition_columns(spark, staged_table_path)
        partition_matches = partition_columns == RAW_LAYOUT_PARTITION_COLUMNS
        schema_matches = _raw_storage_schema_matches(migrated_df)
        file_profile_passes = _file_profile_passes(
            source_profile=source_file_profile,
            staged_profile=staged_file_profile,
        )
        status = (
            "PASS"
            if (
                migrated_row_count == source_key_count
                and migrated_key_count == source_key_count
                and duplicate_key_count == 0
                and conflicting_duplicate_key_count == 0
                and watermark_mismatch_count == 0
                and partition_matches
                and schema_matches
                and file_profile_passes
                and has_delta_log(staged_table_path)
            )
            else "BLOCKED"
        )
        report = {
            "run_id": run_id,
            "runtime_owner": "spark_delta",
            "status": status,
            "migration_mode": "stage-only",
            "source_table_path": source_table_path.as_posix(),
            "staged_table_path": staged_table_path.as_posix(),
            "source_row_count": source_row_count,
            "migrated_row_count": migrated_row_count,
            "source_key_count": source_key_count,
            "migrated_key_count": migrated_key_count,
            "deduplicated_row_count": deduplicated_row_count,
            "duplicate_key_count": duplicate_key_count,
            "conflicting_duplicate_key_count": conflicting_duplicate_key_count,
            "watermark_mismatch_count": watermark_mismatch_count,
            "schema_matches": schema_matches,
            "file_profile_passes": file_profile_passes,
            "file_profile": {
                "source": source_file_profile,
                "staged": staged_file_profile,
                "min_staged_average_parquet_bytes": MIN_STAGED_AVERAGE_PARQUET_BYTES,
            },
            "partition_columns": {
                "actual": list(partition_columns or []),
                "expected": list(RAW_LAYOUT_PARTITION_COLUMNS),
            },
            "delta_log": {
                "source": _delta_log_payload(source_table_path),
                "staged": _delta_log_payload(staged_table_path),
            },
            "storage_columns": list(RAW_STORAGE_COLUMNS),
            "built_at_utc": _utc_now_iso(),
        }
        _write_json(report_path, report)
        return report
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def _load_passed_migration_report(report_path: Path, staged_table_path: Path) -> dict[str, Any]:
    if not report_path.exists():
        raise FileNotFoundError(f"raw layout migration report not found: {report_path.as_posix()}")
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(
            f"raw layout migration report must be a JSON object: {report_path.as_posix()}"
        )
    if payload.get("status") != "PASS":
        raise RuntimeError("raw layout promotion requires a passed migration report")
    expected_staged = str(payload.get("staged_table_path", "")).strip()
    if expected_staged and Path(expected_staged).resolve() != staged_table_path.resolve():
        raise RuntimeError(
            "raw layout migration report staged path does not match requested staged table: "
            f"{expected_staged} vs {staged_table_path.as_posix()}"
        )
    partition_payload = payload.get("partition_columns")
    if not isinstance(partition_payload, dict):
        raise RuntimeError("raw layout migration report is missing partition_columns")
    actual = tuple(str(item) for item in list(partition_payload.get("actual") or []))
    expected = tuple(str(item) for item in list(partition_payload.get("expected") or []))
    if actual != RAW_LAYOUT_PARTITION_COLUMNS or expected != RAW_LAYOUT_PARTITION_COLUMNS:
        raise RuntimeError(
            "raw layout migration report does not prove the expected partition layout"
        )
    if payload.get("file_profile_passes") is not True:
        raise RuntimeError("raw layout migration report does not prove an improved file profile")
    return payload


def _ensure_distinct_paths(*paths: Path) -> None:
    resolved = [path.resolve() for path in paths]
    if len(set(resolved)) != len(resolved):
        raise ValueError("raw layout promotion paths must be distinct")
    for left in resolved:
        for right in resolved:
            if left == right:
                continue
            if left in right.parents:
                raise ValueError("raw layout promotion paths must not be nested")


def promote_moex_raw_layout_migration(
    *,
    current_table_path: Path,
    staged_table_path: Path,
    backup_table_path: Path,
    report_path: Path,
) -> dict[str, object]:
    _ensure_distinct_paths(current_table_path, staged_table_path, backup_table_path)
    _load_passed_migration_report(report_path, staged_table_path)
    if current_table_path.name != "raw_moex_history.delta":
        raise RuntimeError(
            "raw layout promotion only applies to raw_moex_history.delta: "
            f"{current_table_path.as_posix()}"
        )
    if not has_delta_log(current_table_path):
        raise FileNotFoundError(
            f"current raw Delta table is missing _delta_log: {current_table_path.as_posix()}"
        )
    if not has_delta_log(staged_table_path):
        raise FileNotFoundError(
            f"staged raw Delta table is missing _delta_log: {staged_table_path.as_posix()}"
        )
    if backup_table_path.exists():
        raise FileExistsError(
            f"raw layout backup path already exists: {backup_table_path.as_posix()}"
        )

    backup_table_path.parent.mkdir(parents=True, exist_ok=True)
    moved_current = False
    try:
        shutil.move(str(current_table_path), str(backup_table_path))
        moved_current = True
        shutil.move(str(staged_table_path), str(current_table_path))
    except Exception:
        if moved_current and backup_table_path.exists() and not current_table_path.exists():
            shutil.move(str(backup_table_path), str(current_table_path))
        raise

    return {
        "status": "PASS",
        "promotion_mode": "explicit-root-swap",
        "current_table_path": current_table_path.as_posix(),
        "backup_table_path": backup_table_path.as_posix(),
        "report_path": report_path.as_posix(),
        "promoted_at_utc": _utc_now_iso(),
    }
