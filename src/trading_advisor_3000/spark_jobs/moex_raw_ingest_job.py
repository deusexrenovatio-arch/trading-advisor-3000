from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log
from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS
from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import (
    build_raw_ingest_run_report_v2,
)

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session, _load_spark_modules

LOGGER = logging.getLogger(__name__)
KEY_SCOPE_COLUMNS = ("internal_id", "timeframe", "source_interval", "moex_secid")
RAW_KEY_COLUMNS = KEY_SCOPE_COLUMNS + ("ts_open", "ts_close")
RAW_SOURCE_TIMESTAMP_COLUMNS = ("ts_open", "ts_close")
RAW_PROVIDER_METADATA_COLUMNS = (
    "finam_symbol",
    "moex_engine",
    "moex_market",
    "moex_board",
    "asset_group",
    "provenance_json",
)
RAW_VALUE_COLUMNS = ("open", "high", "low", "close", "volume", "open_interest")
RAW_FINGERPRINT_COLUMNS = RAW_KEY_COLUMNS + RAW_PROVIDER_METADATA_COLUMNS + RAW_VALUE_COLUMNS
RECONCILE_WINDOW_COLUMNS = KEY_SCOPE_COLUMNS + ("_window_start_utc", "_window_end_utc")
DELETE_WINDOW_CHUNK_SIZE = 200
_NULL_FINGERPRINT_TOKEN = "__TA3000_NULL__"
_VOLATILE_PROVENANCE_KEYS = (
    "run_id",
    "ingest_run_id",
    "ingested_at_utc",
    "window_start_utc",
    "window_end_utc",
    "stability_lag_minutes",
    "refresh_overlap_minutes",
)


def _ensure_spark_delta_runtime_available() -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        raise RuntimeError(
            "Spark/Delta raw ingest requires HADOOP_HOME on local Windows; "
            "run in the Docker/Linux Spark proof profile or configure Hadoop winutils."
        )


def _parse_iso_utc(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(UTC).replace(tzinfo=None)


def _to_iso_utc(value: object) -> str:
    parsed = _parse_iso_utc(value)
    if parsed is None:
        return ""
    return parsed.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")


def _spark_type(types_module: Any, type_name: str) -> Any:
    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return types_module.StringType()
    if normalized == "timestamp":
        return types_module.TimestampType()
    if normalized in {"int", "integer"}:
        return types_module.IntegerType()
    if normalized in {"bigint", "long"}:
        return types_module.LongType()
    if normalized in {"double", "float"}:
        return types_module.DoubleType()
    return types_module.StringType()


def _raw_schema(types_module: Any, *, include_source_order: bool = False) -> Any:
    fields = [
        types_module.StructField(column, _spark_type(types_module, type_name), True)
        for column, type_name in RAW_COLUMNS.items()
    ]
    if include_source_order:
        fields.append(types_module.StructField("_source_order", types_module.IntegerType(), False))
    return types_module.StructType(fields)


def _key_schema(types_module: Any) -> Any:
    return types_module.StructType(
        [
            types_module.StructField("internal_id", types_module.StringType(), False),
            types_module.StructField("timeframe", types_module.StringType(), False),
            types_module.StructField("source_interval", types_module.IntegerType(), False),
            types_module.StructField("moex_secid", types_module.StringType(), False),
        ]
    )


def _scope_schema(types_module: Any) -> Any:
    return types_module.StructType(
        list(_key_schema(types_module).fields)
        + [
            types_module.StructField("window_start_utc", types_module.TimestampType(), False),
            types_module.StructField("window_end_utc", types_module.TimestampType(), False),
            types_module.StructField("watermark_utc", types_module.TimestampType(), True),
        ]
    )


def _normalize_raw_row(row: Mapping[str, Any], source_order: int) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for column, type_name in RAW_COLUMNS.items():
        value = row.get(column)
        normalized_type = type_name.strip().lower()
        if normalized_type == "timestamp":
            normalized[column] = _parse_iso_utc(value)
        elif normalized_type == "json" and value is not None and not isinstance(value, str):
            normalized[column] = json.dumps(value, ensure_ascii=False, sort_keys=True)
        elif normalized_type in {"int", "integer"} and value is not None:
            normalized[column] = int(value)
        elif normalized_type in {"bigint", "long"} and value is not None:
            normalized[column] = int(value)
        elif normalized_type in {"double", "float"} and value is not None:
            normalized[column] = float(value)
        elif value is None:
            normalized[column] = None
        else:
            normalized[column] = str(value)
    normalized["_source_order"] = source_order
    return normalized


def _normalize_scope(
    scope: Mapping[str, Any],
    initial_watermarks: Mapping[tuple[str, str, int, str], str],
) -> dict[str, object]:
    key = (
        str(scope["internal_id"]),
        str(scope["timeframe"]),
        int(scope["source_interval"]),
        str(scope["moex_secid"]),
    )
    watermark = scope.get("watermark_utc") or initial_watermarks.get(key)
    return {
        "internal_id": key[0],
        "timeframe": key[1],
        "source_interval": key[2],
        "moex_secid": key[3],
        "window_start_utc": _parse_iso_utc(scope["window_start_utc"]),
        "window_end_utc": _parse_iso_utc(scope["window_end_utc"]),
        "watermark_utc": _parse_iso_utc(watermark),
    }


def _append_json_event(*, jsonl_path: Path, latest_path: Path, payload: Mapping[str, Any]) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(text)
        handle.write("\n")
    latest_path.write_text(text + "\n", encoding="utf-8")


def _sql_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_timestamp_literal(value: object) -> str:
    parsed = _parse_iso_utc(value)
    if parsed is None:
        raise ValueError("window delete condition requires non-empty timestamp")
    return "TIMESTAMP '" + parsed.strftime("%Y-%m-%d %H:%M:%S") + "'"


def _column_ref(column: str, *, target_alias: str = "") -> str:
    return f"{target_alias}.{column}" if target_alias else column


def _build_window_delete_condition(
    windows: list[Mapping[str, Any]], *, target_alias: str = ""
) -> str:
    parts: list[str] = []
    for window in windows:
        internal_id = _column_ref("internal_id", target_alias=target_alias)
        timeframe = _column_ref("timeframe", target_alias=target_alias)
        source_interval = _column_ref("source_interval", target_alias=target_alias)
        moex_secid = _column_ref("moex_secid", target_alias=target_alias)
        ts_close = _column_ref("ts_close", target_alias=target_alias)
        parts.append(
            "("
            + " AND ".join(
                [
                    f"{internal_id} = {_sql_string_literal(window['internal_id'])}",
                    f"{timeframe} = {_sql_string_literal(window['timeframe'])}",
                    f"{source_interval} = {int(window['source_interval'])}",
                    f"{moex_secid} = {_sql_string_literal(window['moex_secid'])}",
                    f"{ts_close} >= {_sql_timestamp_literal(window['_window_start_utc'])}",
                    f"{ts_close} <= {_sql_timestamp_literal(window['_window_end_utc'])}",
                ]
            )
            + ")"
        )
    return " OR ".join(parts)


def _iter_window_delete_conditions(
    windows: Any,
    *,
    chunk_size: int = DELETE_WINDOW_CHUNK_SIZE,
) -> Any:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    batch: list[Mapping[str, Any]] = []
    for window in windows:
        batch.append(window)
        if len(batch) >= chunk_size:
            yield _build_window_delete_condition(batch)
            batch = []
    if batch:
        yield _build_window_delete_condition(batch)


def _raw_fingerprint_expr(functions: Any, *, prefix: str = "") -> Any:
    volatile_keys = ", ".join(_sql_string_literal(key) for key in _VOLATILE_PROVENANCE_KEYS)
    provenance_column = f"{prefix}provenance_json"
    stable_provenance = functions.coalesce(
        functions.expr(
            "to_json(map_filter("
            f"from_json({provenance_column}, 'map<string,string>'), "
            f"(k, v) -> NOT array_contains(array({volatile_keys}), k)"
            "))"
        ),
        functions.lit(_NULL_FINGERPRINT_TOKEN),
    ).alias("provenance_json")
    fields = []
    for column in RAW_FINGERPRINT_COLUMNS:
        if column == "provenance_json":
            fields.append(stable_provenance)
            continue
        fields.append(
            functions.coalesce(
                functions.col(f"{prefix}{column}").cast("string"),
                functions.lit(_NULL_FINGERPRINT_TOKEN),
            ).alias(column)
        )
    return functions.sha2(
        functions.to_json(functions.struct(*fields)),
        256,
    )


def compute_raw_watermarks_spark_delta(
    *,
    table_path: Path,
    keys: set[tuple[str, str, int, str]],
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[tuple[str, str, int, str], str]:
    if not keys or not has_delta_log(table_path):
        return {}

    _ensure_spark_delta_runtime_available()
    spark_session_factory = spark_session_factory or _create_spark_session
    spark = spark_session_factory("ta3000-moex-raw-watermarks", spark_master)
    _, _, functions, _ = _load_spark_modules()
    try:
        from pyspark.sql import types as spark_types  # type: ignore[import-not-found]

        key_rows = [
            {
                "internal_id": internal_id,
                "timeframe": timeframe,
                "source_interval": int(source_interval),
                "moex_secid": moex_secid,
            }
            for internal_id, timeframe, source_interval, moex_secid in sorted(keys)
        ]
        keys_df = spark.createDataFrame(key_rows, schema=_key_schema(spark_types))
        raw_df = spark.read.format("delta").load(str(table_path))
        watermark_df = (
            raw_df.join(keys_df, list(KEY_SCOPE_COLUMNS), "inner")
            .groupBy(*KEY_SCOPE_COLUMNS)
            .agg(functions.max("ts_close").alias("ts_close"))
        )
        return {
            (
                str(row["internal_id"]),
                str(row["timeframe"]),
                int(row["source_interval"]),
                str(row["moex_secid"]),
            ): _to_iso_utc(row["ts_close"])
            for row in watermark_df.collect()
            if row["ts_close"] is not None
        }
    finally:
        try:
            spark.stop()
        except Exception as cleanup_exc:  # pragma: no cover - best-effort cleanup
            LOGGER.debug("Spark session cleanup failed: %s", cleanup_exc)


def _filtered_raw_by_scopes(raw_df: Any, scopes_df: Any, functions: Any) -> Any:
    raw = raw_df.alias("raw")
    scope = scopes_df.alias("scope")
    join_condition = [raw[column] == scope[column] for column in KEY_SCOPE_COLUMNS]
    return (
        raw.join(scope, join_condition, "inner")
        .where(
            (raw["ts_close"] >= scope["window_start_utc"])
            & (raw["ts_close"] <= scope["window_end_utc"])
        )
        .select(
            *[raw[column].alias(column) for column in RAW_COLUMNS],
            scope["window_start_utc"].alias("_window_start_utc"),
            scope["window_end_utc"].alias("_window_end_utc"),
        )
    )


def _collect_changed_windows(changed_raw_df: Any, functions: Any) -> list[dict[str, object]]:
    grouped = changed_raw_df.groupBy(
        *KEY_SCOPE_COLUMNS,
        "_window_start_utc",
        "_window_end_utc",
    ).agg(functions.count(functions.lit(1)).alias("incremental_rows"))
    return [
        {
            "internal_id": str(row["internal_id"]),
            "source_timeframe": str(row["timeframe"]),
            "source_interval": int(row["source_interval"]),
            "moex_secid": str(row["moex_secid"]),
            "window_start_utc": _to_iso_utc(row["_window_start_utc"]),
            "window_end_utc": _to_iso_utc(row["_window_end_utc"]),
            "incremental_rows": int(row["incremental_rows"]),
        }
        for row in grouped.collect()
        if int(row["incremental_rows"]) > 0
    ]


def _collect_post_watermarks(*, raw_df: Any, keys_df: Any, functions: Any) -> dict[str, str]:
    watermark_df = (
        raw_df.join(keys_df, list(KEY_SCOPE_COLUMNS), "inner")
        .groupBy(*KEY_SCOPE_COLUMNS)
        .agg(functions.max("ts_close").alias("ts_close"))
    )
    return {
        "|".join(
            (
                str(row["internal_id"]),
                str(row["timeframe"]),
                str(row["source_interval"]),
                str(row["moex_secid"]),
            )
        ): _to_iso_utc(row["ts_close"])
        for row in watermark_df.collect()
        if row["ts_close"] is not None
    }


def run_moex_raw_ingest_spark_delta_job(
    *,
    table_path: Path,
    source_rows: list[Mapping[str, Any]] | None = None,
    source_rows_path: Path | None = None,
    window_scopes: list[Mapping[str, Any]],
    initial_watermarks: Mapping[tuple[str, str, int, str], str],
    run_id: str,
    ingest_till_utc: str,
    refresh_overlap_minutes: int,
    progress_path: Path,
    progress_latest_path: Path,
    error_path: Path,
    error_latest_path: Path,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
    emit_progress_event: bool = True,
) -> dict[str, Any]:
    if refresh_overlap_minutes < 0:
        raise ValueError("refresh_overlap_minutes must be >= 0")

    _ensure_spark_delta_runtime_available()
    spark_session_factory = spark_session_factory or _create_spark_session
    spark = spark_session_factory("ta3000-moex-raw-ingest-delta-native", spark_master)
    _, window, functions, _ = _load_spark_modules()
    try:
        from delta.tables import DeltaTable  # type: ignore[import-not-found]
        from pyspark.sql import types as spark_types  # type: ignore[import-not-found]

        raw_schema = _raw_schema(spark_types)
        source_schema = _raw_schema(spark_types, include_source_order=True)
        scope_payload = [_normalize_scope(scope, initial_watermarks) for scope in window_scopes]
        source_rows_payload = list(source_rows or [])
        if source_rows_path is not None and source_rows_payload:
            raise ValueError("provide either source_rows_path or source_rows, not both")
        if source_rows_path is not None:
            if not source_rows_path.exists():
                raise FileNotFoundError(
                    f"raw source rows staging path does not exist: {source_rows_path.as_posix()}"
                )
            if source_rows_path.stat().st_size > 0:
                source_df = spark.read.schema(source_schema).json(str(source_rows_path)).cache()
            else:
                source_df = spark.createDataFrame([], schema=source_schema).cache()
        else:
            source_payload = [
                _normalize_raw_row(row, source_order=index)
                for index, row in enumerate(source_rows_payload, start=1)
            ]
            source_df = spark.createDataFrame(source_payload, schema=source_schema).cache()
        scopes_df = spark.createDataFrame(scope_payload, schema=_scope_schema(spark_types)).cache()
        keys_df = scopes_df.select(*KEY_SCOPE_COLUMNS).distinct().cache()
        source_rows_count = int(source_df.count())
        table_exists = has_delta_log(table_path)

        if source_rows_count == 0 and not table_exists:
            table_path.parent.mkdir(parents=True, exist_ok=True)
            spark.createDataFrame([], schema=raw_schema).write.format("delta").mode(
                "overwrite"
            ).save(str(table_path))
            table_exists = True

        if source_rows_count == 0 and not scope_payload:
            raw_after_df = (
                spark.read.format("delta").load(str(table_path)) if table_exists else source_df
            )
            watermark_by_key = (
                _collect_post_watermarks(raw_df=raw_after_df, keys_df=keys_df, functions=functions)
                if scope_payload
                else {}
            )
            report = build_raw_ingest_run_report_v2(
                run_id=run_id,
                ingest_till_utc=ingest_till_utc,
                source_rows=0,
                incremental_rows=0,
                deduplicated_rows=0,
                stale_rows=0,
                watermark_by_key=watermark_by_key,
                raw_table_path=table_path.as_posix(),
                raw_ingest_progress_path=progress_path.as_posix(),
                raw_ingest_error_path=error_path.as_posix(),
                raw_ingest_error_latest_path=error_latest_path.as_posix(),
                changed_windows=[],
            )
            if emit_progress_event:
                _append_json_event(
                    jsonl_path=progress_path,
                    latest_path=progress_latest_path,
                    payload={
                        "run_id": run_id,
                        "runtime_owner": "spark_delta",
                        "source_rows": 0,
                        "incremental_rows": 0,
                        "processed_at_utc": _to_iso_utc(datetime.now(tz=UTC)),
                    },
                )
            return report

        ranked_source = source_df.withColumn(
            "_rn",
            functions.row_number().over(
                window.partitionBy(*RAW_KEY_COLUMNS).orderBy(functions.col("_source_order").desc())
            ),
        )
        unique_source = ranked_source.where(functions.col("_rn") == 1).drop("_rn").cache()
        unique_source_count = int(unique_source.count())

        if refresh_overlap_minutes > 0:
            overlap_start = functions.expr(
                f"watermark_utc - INTERVAL {int(refresh_overlap_minutes)} MINUTES"
            )
            within_overlap = (
                functions.col("watermark_utc").isNotNull()
                & (functions.col("ts_close") >= overlap_start)
                & (functions.col("ts_close") <= functions.col("watermark_utc"))
            )
        else:
            within_overlap = functions.lit(False)

        eligible_condition = (
            functions.col("watermark_utc").isNull()
            | (functions.col("ts_close") > functions.col("watermark_utc"))
            | within_overlap
        )
        source_scope_candidates = unique_source.join(
            scopes_df, list(KEY_SCOPE_COLUMNS), "left"
        ).cache()
        source_with_scope = source_scope_candidates.where(
            functions.col("window_start_utc").isNotNull()
            & functions.col("window_end_utc").isNotNull()
            & (functions.col("ts_close") >= functions.col("window_start_utc"))
            & (functions.col("ts_close") <= functions.col("window_end_utc"))
        ).cache()
        matched_source_keys = source_with_scope.select(*RAW_KEY_COLUMNS).distinct().cache()
        unmatched_source_count = int(
            unique_source.join(matched_source_keys, list(RAW_KEY_COLUMNS), "left_anti").count()
        )
        if unmatched_source_count > 0:
            raise ValueError(
                f"{unmatched_source_count} raw source rows did not match declared window scopes"
            )
        eligible_source_with_scope = (
            source_with_scope.where(eligible_condition)
            .select(
                *[functions.col(column).alias(column) for column in RAW_COLUMNS],
                functions.col("window_start_utc").alias("_window_start_utc"),
                functions.col("window_end_utc").alias("_window_end_utc"),
            )
            .cache()
        )
        eligible_source_count = int(eligible_source_with_scope.count())
        stale_rows = unique_source_count - eligible_source_count

        if table_exists:
            raw_existing = spark.read.format("delta").load(str(table_path))
            baseline_scoped = _filtered_raw_by_scopes(raw_existing, scopes_df, functions).cache()
            baseline_compare = (
                baseline_scoped.withColumn(
                    "_target_fingerprint_sha256",
                    _raw_fingerprint_expr(functions),
                )
                .select(*RAW_KEY_COLUMNS, "_target_fingerprint_sha256")
                .cache()
            )
            source_with_fingerprint = eligible_source_with_scope.withColumn(
                "_source_fingerprint_sha256",
                _raw_fingerprint_expr(functions),
            ).cache()
            joined = source_with_fingerprint.join(baseline_compare, list(RAW_KEY_COLUMNS), "left")
            changed_source_df = (
                joined.where(
                    functions.col("_target_fingerprint_sha256").isNull()
                    | (
                        ~functions.col("_source_fingerprint_sha256").eqNullSafe(
                            functions.col("_target_fingerprint_sha256")
                        )
                    )
                )
                .select(*RAW_COLUMNS, "_window_start_utc", "_window_end_utc")
                .cache()
            )
            source_keys = eligible_source_with_scope.select(*RAW_KEY_COLUMNS).distinct().cache()
            target_missing_df = (
                baseline_scoped.join(source_keys, list(RAW_KEY_COLUMNS), "left_anti")
                .select(*RECONCILE_WINDOW_COLUMNS)
                .cache()
            )
            changed_window_events_df = (
                changed_source_df.select(*RECONCILE_WINDOW_COLUMNS)
                .unionByName(target_missing_df.select(*RECONCILE_WINDOW_COLUMNS))
                .cache()
            )
            windows_to_reconcile_df = (
                changed_window_events_df.select(*RECONCILE_WINDOW_COLUMNS).distinct().cache()
            )
            replacement_source_df = (
                eligible_source_with_scope.join(
                    windows_to_reconcile_df,
                    list(RECONCILE_WINDOW_COLUMNS),
                    "inner",
                )
                .select(*RAW_COLUMNS)
                .cache()
            )
            target_missing_rows = int(target_missing_df.count())
        else:
            changed_source_df = eligible_source_with_scope.select(
                *RAW_COLUMNS,
                "_window_start_utc",
                "_window_end_utc",
            ).cache()
            changed_window_events_df = changed_source_df.select(*RECONCILE_WINDOW_COLUMNS).cache()
            windows_to_reconcile_df = (
                changed_window_events_df.select(*RECONCILE_WINDOW_COLUMNS).distinct().cache()
            )
            replacement_source_df = changed_source_df.select(*RAW_COLUMNS).cache()
            target_missing_rows = 0

        changed_source_rows = int(changed_source_df.count())
        incremental_rows = changed_source_rows + target_missing_rows
        deduplicated_rows = max(0, source_rows_count - changed_source_rows - stale_rows)
        changed_windows = _collect_changed_windows(changed_window_events_df, functions)

        table_path.parent.mkdir(parents=True, exist_ok=True)
        if not table_exists:
            replacement_source_df.select(*RAW_COLUMNS).write.format("delta").mode("overwrite").save(
                str(table_path)
            )
        elif incremental_rows > 0:
            reconcile_windows = [row.asDict() for row in windows_to_reconcile_df.toLocalIterator()]
            scoped_delete_condition = _build_window_delete_condition(
                reconcile_windows, target_alias="target"
            )
            merge_condition = " AND ".join(
                f"target.{column} <=> source.{column}" for column in RAW_KEY_COLUMNS
            )
            delta_table = DeltaTable.forPath(spark, str(table_path))
            merge_builder = (
                delta_table.alias("target")
                .merge(
                    replacement_source_df.select(*RAW_COLUMNS).alias("source"),
                    merge_condition,
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
            )
            if scoped_delete_condition:
                merge_builder = merge_builder.whenNotMatchedBySourceDelete(scoped_delete_condition)
            merge_builder.execute()

        raw_after_df = spark.read.format("delta").load(str(table_path))
        watermark_by_key = _collect_post_watermarks(
            raw_df=raw_after_df,
            keys_df=keys_df,
            functions=functions,
        )
        report = build_raw_ingest_run_report_v2(
            run_id=run_id,
            ingest_till_utc=ingest_till_utc,
            source_rows=source_rows_count,
            incremental_rows=incremental_rows,
            deduplicated_rows=deduplicated_rows,
            stale_rows=stale_rows,
            watermark_by_key=watermark_by_key,
            raw_table_path=table_path.as_posix(),
            raw_ingest_progress_path=progress_path.as_posix(),
            raw_ingest_error_path=error_path.as_posix(),
            raw_ingest_error_latest_path=error_latest_path.as_posix(),
            changed_windows=changed_windows,
        )
        if emit_progress_event:
            _append_json_event(
                jsonl_path=progress_path,
                latest_path=progress_latest_path,
                payload={
                    "run_id": run_id,
                    "runtime_owner": "spark_delta",
                    "source_rows": source_rows_count,
                    "incremental_rows": incremental_rows,
                    "deduplicated_rows": deduplicated_rows,
                    "stale_rows": stale_rows,
                    "deleted_rows": target_missing_rows,
                    "fingerprint_columns": list(RAW_FINGERPRINT_COLUMNS),
                    "changed_windows": len(changed_windows),
                    "processed_at_utc": _to_iso_utc(datetime.now(tz=UTC)),
                },
            )
        return report
    except Exception as exc:
        _append_json_event(
            jsonl_path=error_path,
            latest_path=error_latest_path,
            payload={
                "run_id": run_id,
                "runtime_owner": "spark_delta",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "reported_at_utc": _to_iso_utc(datetime.now(tz=UTC)),
            },
        )
        raise
    finally:
        try:
            spark.stop()
        except Exception as cleanup_exc:  # pragma: no cover - best-effort cleanup
            LOGGER.debug("Spark session cleanup failed: %s", cleanup_exc)
