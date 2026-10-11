from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

YEAR_PARTITION_COLUMN = "ts_close_year"
MAX_COMPRESSED_FILE_BYTES = 512 * 1024 * 1024
RAW_HISTORY_FAMILY = "raw_moex_history"


def default_partition_columns(
    table_path: Path, *, family: str | None = None
) -> tuple[str, ...] | None:
    name = family or table_path.stem
    return (YEAR_PARTITION_COLUMN,) if name == RAW_HISTORY_FAMILY else None


def default_file_policy() -> dict[str, object]:
    return {
        "max_compressed_file_bytes": MAX_COMPRESSED_FILE_BYTES,
        "files_per_nonempty_partition": 1,
        "allow_empty_partitions": False,
        "oversize_action": "fail_closed_use_isolated_finalizer",
    }


def assert_default_table_layout(table_path: Path, *, family: str | None = None) -> None:
    expected = default_partition_columns(table_path, family=family)
    if expected is None or not (table_path / "_delta_log").exists():
        return
    table = DeltaTable(str(table_path))
    actual = tuple(table.metadata().partition_columns)
    if actual != expected:
        raise ValueError(
            f"{table_path.name} layout {actual} != {expected}; "
            "use an isolated layout migration before updating this table"
        )


def assert_fresh_arrow_candidate(
    table_path: Path,
    *,
    family: str | None = None,
    candidate_id: str | None = None,
    mode: str | None = None,
) -> None:
    if default_partition_columns(table_path, family=family) is None:
        return
    if candidate_id is not None:
        if mode != "append" or not (table_path / "_delta_log").exists():
            raise ValueError(
                "owned raw candidate must still exist with its Delta log before append/finalize"
            )
        assert_default_table_layout(table_path, family=family)
        if DeltaTable(str(table_path)).metadata().id != candidate_id:
            raise ValueError("owned raw candidate Delta identity changed before append/finalize")
        return
    assert_default_table_layout(table_path, family=family)
    if not table_path.exists():
        return
    raise ValueError(
        "existing raw Arrow target requires Spark stable writer or a new isolated candidate"
    )


def with_arrow_close_year(table: pa.Table) -> pa.Table:
    if "ts_close" not in table.column_names:
        raise ValueError("raw history layout requires authoritative ts_close")
    close = pc.cast(table.column("ts_close"), pa.timestamp("us", tz="UTC"))
    # Removing the UTC tag preserves epoch values and avoids a host tzdb dependency.
    year = pc.cast(pc.year(pc.cast(close, pa.timestamp("us"))), pa.int32())
    if year.null_count:
        raise ValueError("raw history layout requires non-null valid UTC ts_close")
    if YEAR_PARTITION_COLUMN in table.column_names:
        index = table.column_names.index(YEAR_PARTITION_COLUMN)
        return table.set_column(index, YEAR_PARTITION_COLUMN, year)
    return table.append_column(YEAR_PARTITION_COLUMN, year)


def with_spark_close_year(dataframe: Any, *, functions: Any) -> Any:
    # Timestamp values are instants. Convert the session's wall-clock view to
    # UTC as timestamp_ntz before year extraction; leave the session unchanged.
    timezone = dataframe.sparkSession.conf.get("spark.sql.session.timeZone")
    if dataframe.schema["ts_close"].dataType.simpleString() == "timestamp_ntz":
        raise ValueError(
            "raw physical timestamps require LTZ instants; timestamp_ntz is unsupported"
        )
    utc_close = functions.convert_timezone(
        functions.lit(timezone),
        functions.lit("UTC"),
        functions.col("ts_close").cast("timestamp_ntz"),
    )
    prepared = dataframe.withColumn(YEAR_PARTITION_COLUMN, functions.year(utc_close).cast("int"))
    if prepared.where(functions.col(YEAR_PARTITION_COLUMN).isNull()).limit(1).count():
        raise ValueError("raw history layout requires non-null valid UTC ts_close")
    return prepared


def validate_file_actions(actions: list[dict[str, object]], *, max_file_bytes: int) -> None:
    by_year: dict[str, list[int]] = defaultdict(list)
    for action in actions:
        year = action.get(f"partition.{YEAR_PARTITION_COLUMN}")
        size = int(action["size_bytes"])
        if year is None or not str(year).isdigit():
            raise ValueError("raw history layout has a null or invalid year partition")
        if int(action.get("num_records") or 0) <= 0:
            raise ValueError("raw history layout contains an empty data file")
        if size > max_file_bytes:
            raise ValueError("raw history layout exceeds file size limit; use isolated finalizer")
        by_year[str(year)].append(size)
    for year, sizes in by_year.items():
        if len(sizes) > 1 and sum(sizes) <= max_file_bytes:
            raise ValueError(
                f"raw history layout has {len(sizes)} files in small year {year}; "
                "use isolated finalizer"
            )


def validate_default_table_files(table_path: Path, *, family: str = RAW_HISTORY_FAMILY) -> None:
    assert_default_table_layout(table_path, family=family)
    # Success requires a real current Delta table, including empty and NOOP outcomes.
    table = DeltaTable(str(table_path))
    validate_file_actions(
        table.get_add_actions(flatten=True).to_pylist(), max_file_bytes=MAX_COMPRESSED_FILE_BYTES
    )


def compact_default_table(
    table_path: Path, *, family: str | None = None, candidate_id: str | None = None
) -> None:
    if default_partition_columns(table_path, family=family) is None:
        return
    if candidate_id is None:
        raise ValueError("owned raw candidate identity is required for finalization")
    assert_fresh_arrow_candidate(
        table_path, family=family, candidate_id=candidate_id, mode="append"
    )
    table = DeltaTable(str(table_path))
    table.optimize.compact(target_size=MAX_COMPRESSED_FILE_BYTES, max_concurrent_tasks=1)
    table.update_incremental()
    # Delta active add metadata only; never scan rows or historical tombstones.
    validate_file_actions(
        table.get_add_actions(flatten=True).to_pylist(), max_file_bytes=MAX_COMPRESSED_FILE_BYTES
    )


def compact_spark_table(spark: Any, table_path: Path, *, family: str = RAW_HISTORY_FAMILY) -> None:
    from delta.tables import DeltaTable as SparkDeltaTable

    assert_default_table_layout(table_path, family=family)
    settings = {
        "spark.databricks.delta.optimize.minFileSize": str(MAX_COMPRESSED_FILE_BYTES),
        "spark.databricks.delta.optimize.maxFileSize": str(MAX_COMPRESSED_FILE_BYTES),
        "spark.databricks.delta.optimize.maxThreads": "1",
    }
    prior = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        SparkDeltaTable.forPath(spark, str(table_path)).optimize().executeCompaction()
    finally:
        for key, value in prior.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
    # The connector reads active metadata only; Spark owns all stable writes.
    validate_default_table_files(table_path, family=family)


def write_layout_arrow_table(
    *,
    table_path: Path,
    table: pa.Table,
    mode: str,
    family: str | None = None,
    compact: bool = True,
    candidate_id: str | None = None,
    replace_predicate: str | None = None,
) -> None:
    if replace_predicate is not None:
        if mode != "overwrite" or not replace_predicate.strip():
            raise ValueError("predicate replacement requires overwrite and a non-empty predicate")
    partitions = default_partition_columns(table_path, family=family)
    options: dict[str, Any] = {}
    if partitions is not None:
        assert_fresh_arrow_candidate(
            table_path, family=family, candidate_id=candidate_id, mode=mode
        )
        table = with_arrow_close_year(table)
        options = {"partition_by": list(partitions), "target_file_size": MAX_COMPRESSED_FILE_BYTES}
    if replace_predicate is not None:
        options["predicate"] = replace_predicate
    schema_mode = "overwrite" if mode == "overwrite" and replace_predicate is None else "merge"
    write_deltalake(str(table_path), table, mode=mode, schema_mode=schema_mode, **options)
    if compact:
        owned_id = candidate_id
        if partitions is not None and owned_id is None:
            owned_id = DeltaTable(str(table_path)).metadata().id
        compact_default_table(table_path, family=family, candidate_id=owned_id)
