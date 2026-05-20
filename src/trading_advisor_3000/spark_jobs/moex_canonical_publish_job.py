from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log
from trading_advisor_3000.product_plane.data_plane.schemas import (
    historical_data_delta_schema_manifest,
)

from .canonical_bars_job import (
    DEFAULT_SPARK_MASTER,
    _create_spark_session,
    _load_spark_modules,
    _write_delta_dataframe,
)
from .moex_canonicalization_job import _prepare_official_session_intervals

CANONICAL_KEY_COLUMNS = ("contract_id", "instrument_id", "timeframe", "ts")
SIDECAR_ROLL_REASON = "max_open_interest_then_latest_ts_close"
SIDECAR_OVERLAP_POLICY = "affected_sessions_plus_minus_1_day"
PUBLISH_SCOPE_COLUMNS: dict[str, str] = {
    "instrument_id": "string",
    "timeframe": "string",
    "target_minutes": "int",
    "window_start_utc": "timestamp",
    "window_end_utc": "timestamp",
}
SIDECAR_SESSION_SCOPE_COLUMNS: dict[str, str] = {
    "instrument_id": "string",
    "session_date": "date",
}

CANONICAL_PROVENANCE_COLUMNS: dict[str, str] = {
    "contract_id": "string",
    "instrument_id": "string",
    "timeframe": "string",
    "ts": "timestamp",
    "bar_start_ts": "timestamp",
    "bar_end_ts": "timestamp",
    "session_interval_id": "string",
    "source_provider": "string",
    "source_timeframe": "string",
    "source_interval": "int",
    "source_run_id": "string",
    "source_ingest_run_id": "string",
    "source_row_count": "int",
    "source_ts_open_first": "timestamp",
    "source_ts_close_last": "timestamp",
    "open_interest_imputed": "int",
    "build_run_id": "string",
    "built_at_utc": "timestamp",
}


def _delta_log_payload(path: Path) -> dict[str, object]:
    return {"path": path.as_posix(), "delta_log": has_delta_log(path)}


def _ensure_spark_delta_runtime_available() -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        raise RuntimeError(
            "Spark/Delta canonical publish requires HADOOP_HOME on local Windows; "
            "run in the Docker/Linux Spark proof profile or configure Hadoop winutils."
        )


def _table_or_empty(spark: Any, *, table_path: Path, schema: str) -> Any:
    if has_delta_log(table_path):
        return spark.read.format("delta").load(str(table_path))
    return spark.createDataFrame([], schema)


def _schema_from_columns(columns: dict[str, str]) -> str:
    return ",".join(f"{key} {value}" for key, value in columns.items())


def _merge_upsert_delta_dataframe(
    *,
    spark: Any,
    table_path: Path,
    dataframe: Any,
    key_columns: tuple[str, ...],
) -> None:
    if int(dataframe.count()) == 0:
        return
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    condition = " AND ".join(f"target.{column} <=> source.{column}" for column in key_columns)
    (
        DeltaTable.forPath(spark, str(table_path))
        .alias("target")
        .merge(dataframe.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_replace_delta_dataframe(
    *,
    spark: Any,
    table_path: Path,
    staged_dataframe: Any,
    stale_keys_path: Path,
    stale_key_count: int,
    key_columns: tuple[str, ...],
    functions: Any,
) -> None:
    if int(staged_dataframe.count()) > 0:
        _merge_upsert_delta_dataframe(
            spark=spark,
            table_path=table_path,
            dataframe=staged_dataframe,
            key_columns=key_columns,
        )

    if stale_key_count == 0:
        return

    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    delete_source_df = spark.read.format("delta").load(str(stale_keys_path))
    condition = " AND ".join(f"target.{column} <=> source.{column}" for column in key_columns)
    (
        DeltaTable.forPath(spark, str(table_path))
        .alias("target")
        .merge(delete_source_df.alias("source"), condition)
        .whenMatchedDelete()
        .execute()
    )


def _materialize_stale_keys(
    *,
    dataframe: Any,
    table_path: Path,
    key_columns: tuple[str, ...],
) -> None:
    table_path.parent.mkdir(parents=True, exist_ok=True)
    (
        dataframe.select(*key_columns)
        .distinct()
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(table_path))
    )


def _delta_table_version(spark: Any, table_path: Path) -> int | None:
    if not has_delta_log(table_path):
        return None
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    history_rows = (
        DeltaTable.forPath(spark, str(table_path)).history(1).select("version").limit(1).collect()
    )
    if not history_rows:
        return None
    return int(history_rows[0]["version"])


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _qc_gate(gate: str, violations: int, samples: list[str] | None = None) -> dict[str, object]:
    return {
        "gate": gate,
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
        "samples": list(samples or [])[:20],
    }


def _collect_sample_strings(dataframe: Any, *, limit: int = 20) -> list[str]:
    return [str(row.asDict()) for row in dataframe.limit(limit).collect()]


def _explicit_publish_scope_keys(
    *,
    spark: Any,
    publish_scope_path: Path | None,
    target_bars_df: Any,
    functions: Any,
) -> Any:
    empty_keys_df = target_bars_df.select(*CANONICAL_KEY_COLUMNS).limit(0)
    if publish_scope_path is None or not publish_scope_path.exists():
        return empty_keys_df

    scope_df = spark.read.json(str(publish_scope_path)).select(
        functions.col("instrument_id").cast("string").alias("instrument_id"),
        functions.col("timeframe").cast("string").alias("timeframe"),
        functions.col("target_minutes").cast("int").alias("target_minutes"),
        functions.to_timestamp(functions.col("window_start_utc")).alias("window_start_utc"),
        functions.to_timestamp(functions.col("window_end_utc")).alias("window_end_utc"),
    )
    if int(scope_df.count()) == 0:
        return empty_keys_df

    scope_df = scope_df.withColumn(
        "_canonical_scope_start",
        functions.to_timestamp(
            functions.from_unixtime(
                functions.unix_timestamp(functions.col("window_start_utc"))
                - (functions.col("target_minutes") * functions.lit(60))
            )
        ),
    )
    return (
        target_bars_df.alias("target")
        .join(scope_df.alias("scope"), ["instrument_id", "timeframe"], "inner")
        .where(
            (functions.col("target.ts") >= functions.col("scope._canonical_scope_start"))
            & (functions.col("target.ts") <= functions.col("scope.window_end_utc"))
        )
        .select(
            *[functions.col(f"target.{column}").alias(column) for column in CANONICAL_KEY_COLUMNS]
        )
        .distinct()
    )


def _staged_provenance_scope_keys(
    *,
    target_bars_df: Any,
    staged_provenance_df: Any,
    functions: Any,
) -> Any:
    if int(staged_provenance_df.count()) == 0:
        return target_bars_df.select(*CANONICAL_KEY_COLUMNS).limit(0)

    scope_df = staged_provenance_df.groupBy("contract_id", "instrument_id", "timeframe").agg(
        functions.min("source_ts_open_first").alias("_scope_start"),
        functions.max("source_ts_close_last").alias("_scope_end"),
    )
    return (
        target_bars_df.alias("target")
        .join(scope_df.alias("scope"), ["contract_id", "instrument_id", "timeframe"], "inner")
        .where(
            (functions.col("target.ts") >= functions.col("scope._scope_start"))
            & (functions.col("target.ts") <= functions.col("scope._scope_end"))
        )
        .select(
            *[functions.col(f"target.{column}").alias(column) for column in CANONICAL_KEY_COLUMNS]
        )
        .distinct()
    )


def _impacted_scope_keys(
    *,
    spark: Any,
    publish_scope_path: Path | None,
    target_bars_df: Any,
    staged_provenance_df: Any,
    functions: Any,
) -> Any:
    explicit_keys_df = _explicit_publish_scope_keys(
        spark=spark,
        publish_scope_path=publish_scope_path,
        target_bars_df=target_bars_df,
        functions=functions,
    )
    staged_scope_keys_df = _staged_provenance_scope_keys(
        target_bars_df=target_bars_df,
        staged_provenance_df=staged_provenance_df,
        functions=functions,
    )
    return explicit_keys_df.unionByName(staged_scope_keys_df).distinct()


def _build_qc_report(
    *,
    bars_df: Any,
    provenance_df: Any,
    run_id: str,
    window: Any,
    functions: Any,
) -> dict[str, object]:
    duplicate_bars = (
        bars_df.groupBy(*CANONICAL_KEY_COLUMNS).count().where(functions.col("count") > 1)
    )
    duplicate_provenance = (
        provenance_df.groupBy(*CANONICAL_KEY_COLUMNS).count().where(functions.col("count") > 1)
    )
    invalid_ohlcv = bars_df.where(
        (functions.col("high") < functions.greatest(functions.col("open"), functions.col("close")))
        | (functions.col("low") > functions.least(functions.col("open"), functions.col("close")))
        | (functions.col("volume") < 0)
        | (functions.col("open_interest") < 0)
    )
    missing_provenance = bars_df.join(
        provenance_df.select(*CANONICAL_KEY_COLUMNS),
        list(CANONICAL_KEY_COLUMNS),
        "left_anti",
    )
    incomplete_provenance = provenance_df.where(
        functions.col("source_provider").isNull()
        | (functions.trim(functions.col("source_provider")) == "")
        | functions.col("source_timeframe").isNull()
        | (functions.trim(functions.col("source_timeframe")) == "")
        | functions.col("source_interval").isNull()
        | functions.col("source_run_id").isNull()
        | (functions.trim(functions.col("source_run_id")) == "")
        | functions.col("source_ingest_run_id").isNull()
        | (functions.trim(functions.col("source_ingest_run_id")) == "")
        | functions.col("source_row_count").isNull()
        | (functions.col("source_row_count") <= 0)
        | functions.col("bar_start_ts").isNull()
        | functions.col("bar_end_ts").isNull()
        | (
            ~functions.lower(functions.col("timeframe")).isin("1d", "d", "1w", "w")
            & (
                functions.col("session_interval_id").isNull()
                | (functions.trim(functions.col("session_interval_id")) == "")
            )
        )
        | functions.col("source_ts_open_first").isNull()
        | functions.col("source_ts_close_last").isNull()
        | functions.col("build_run_id").isNull()
        | (functions.trim(functions.col("build_run_id")) == "")
        | functions.col("built_at_utc").isNull()
    )
    timestamp_order_window = window.partitionBy(
        "contract_id",
        "instrument_id",
        "timeframe",
    ).orderBy(functions.col("ts"))
    non_monotonic_timestamps = (
        bars_df.withColumn("_previous_ts", functions.lag("ts").over(timestamp_order_window))
        .where(
            functions.col("_previous_ts").isNotNull()
            & (functions.col("_previous_ts") >= functions.col("ts"))
        )
        .drop("_previous_ts")
    )
    non_monotonic_source_windows = provenance_df.where(
        (functions.col("source_ts_open_first") > functions.col("source_ts_close_last"))
        | (functions.col("bar_start_ts") > functions.col("bar_end_ts"))
    )
    provenance_completeness_violations = int(missing_provenance.count()) + int(
        incomplete_provenance.count()
    )
    bars_count = int(bars_df.count())
    provenance_count = int(provenance_df.count())

    gate_results = [
        _qc_gate(
            "unique_bar_key",
            int(duplicate_bars.count()),
            _collect_sample_strings(duplicate_bars),
        ),
        _qc_gate(
            "unique_provenance_key",
            int(duplicate_provenance.count()),
            _collect_sample_strings(duplicate_provenance),
        ),
        _qc_gate(
            "ohlcv_validity",
            int(invalid_ohlcv.count()),
            _collect_sample_strings(invalid_ohlcv),
        ),
        _qc_gate(
            "canonical_timestamp_monotonicity",
            int(non_monotonic_timestamps.count()),
            _collect_sample_strings(non_monotonic_timestamps),
        ),
        _qc_gate(
            "source_window_monotonicity",
            int(non_monotonic_source_windows.count()),
            _collect_sample_strings(non_monotonic_source_windows),
        ),
        _qc_gate(
            "provenance_completeness",
            provenance_completeness_violations,
            _collect_sample_strings(missing_provenance)
            + _collect_sample_strings(incomplete_provenance),
        ),
        _qc_gate(
            "canonical_provenance_row_count_match",
            0 if bars_count == provenance_count else 1,
            []
            if bars_count == provenance_count
            else [f"bars={bars_count} provenance={provenance_count}"],
        ),
    ]
    failed_gates = [str(item["gate"]) for item in gate_results if item["status"] == "FAIL"]
    return {
        "run_id": run_id,
        "runtime_owner": "spark_delta",
        "status": "PASS" if not failed_gates else "FAIL",
        "publish_decision": "publish" if not failed_gates else "blocked",
        "failed_gates": failed_gates,
        "gate_results": gate_results,
    }


def _build_contract_compatibility_report(
    *,
    bars_df: Any,
    run_id: str,
    functions: Any,
) -> dict[str, object]:
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "product_plane"
        / "contracts"
        / "schemas"
        / "canonical_bar.v1.json"
    )
    schema_payload = json.loads(schema_path.read_text(encoding="utf-8"))
    required_fields = {str(item) for item in schema_payload.get("required", [])}
    property_fields = {str(item) for item in schema_payload.get("properties", {})}
    allowed_timeframes = {
        str(item)
        for item in schema_payload.get("properties", {}).get("timeframe", {}).get("enum", [])
    }
    runtime_fields = set(historical_data_delta_schema_manifest()["canonical_bars"]["columns"])
    dataframe_fields = set(bars_df.columns)

    errors: list[str] = []
    if required_fields != runtime_fields:
        errors.append(
            "canonical schema required fields mismatch with runtime columns: "
            f"schema={sorted(required_fields)} runtime={sorted(runtime_fields)}"
        )
    if property_fields != runtime_fields:
        errors.append(
            "canonical schema properties mismatch with runtime columns: "
            f"schema={sorted(property_fields)} runtime={sorted(runtime_fields)}"
        )
    if dataframe_fields != runtime_fields:
        errors.append(
            "Spark canonical output columns mismatch with runtime columns: "
            f"spark={sorted(dataframe_fields)} runtime={sorted(runtime_fields)}"
        )

    missing_required = bars_df.where(
        functions.lit(False)
        | functions.col("contract_id").isNull()
        | functions.col("instrument_id").isNull()
        | functions.col("timeframe").isNull()
        | functions.col("ts").isNull()
        | functions.col("open").isNull()
        | functions.col("high").isNull()
        | functions.col("low").isNull()
        | functions.col("close").isNull()
        | functions.col("volume").isNull()
        | functions.col("open_interest").isNull()
    )
    unsupported_timeframes = bars_df.where(
        ~functions.col("timeframe").isin(sorted(allowed_timeframes))
    )
    missing_required_count = int(missing_required.count())
    unsupported_timeframe_count = int(unsupported_timeframes.count())
    checked_rows = int(bars_df.count())
    if checked_rows == 0:
        errors.append("No canonical bars checked: 0 rows")
    if missing_required_count:
        errors.append(f"Spark canonical output has null required fields: {missing_required_count}")
    if unsupported_timeframe_count:
        errors.append(
            f"Spark canonical output has unsupported timeframes: {unsupported_timeframe_count}"
        )

    return {
        "run_id": run_id,
        "runtime_owner": "spark_delta",
        "schema_path": schema_path.as_posix(),
        "status": "PASS" if not errors else "FAIL",
        "errors": errors[:20],
        "checked_rows": checked_rows,
        "required_fields": sorted(required_fields),
        "allowed_timeframes": sorted(allowed_timeframes),
    }


def _sidecar_frames(
    *,
    spark: Any,
    bars_df: Any,
    provenance_df: Any,
    affected_sessions_df: Any,
    session_intervals_path: Path,
    sidecars_exist: bool,
    window: Any,
    functions: Any,
) -> tuple[Any, Any, str]:
    joined = bars_df.alias("bar").join(
        provenance_df.alias("provenance"),
        list(CANONICAL_KEY_COLUMNS),
        "inner",
    )
    with_session = joined.withColumn(
        "session_date",
        functions.to_date(functions.coalesce(functions.col("bar_start_ts"), functions.col("ts"))),
    )
    if sidecars_exist:
        with_session = with_session.join(
            affected_sessions_df,
            ["instrument_id", "session_date"],
            "inner",
        )
        refresh_mode = "scoped"
    else:
        refresh_mode = "full"

    official_intervals_df = _prepare_official_session_intervals(
        spark=spark,
        session_intervals_path=session_intervals_path,
        functions=functions,
        window=window,
    )
    official_session_bounds_df = (
        official_intervals_df.groupBy("instrument_id", "session_date")
        .agg(
            functions.min("expected_open_ts").alias("session_open_ts"),
            functions.max("expected_close_ts").alias("session_close_ts"),
            functions.max(
                functions.when(
                    functions.col("session_class") == functions.lit("partial_or_gap"),
                    functions.lit(1),
                ).otherwise(functions.lit(0))
            ).alias("_has_partial_session"),
            functions.max(
                functions.when(
                    functions.col("session_class") != functions.lit("regular"),
                    functions.col("session_class"),
                )
            ).alias("_special_session_class"),
        )
        .withColumn(
            "session_class",
            functions.when(
                functions.col("_has_partial_session") > functions.lit(0),
                functions.lit("partial_or_gap"),
            )
            .when(
                functions.col("_special_session_class").isNotNull(),
                functions.col("_special_session_class"),
            )
            .otherwise(functions.lit("regular")),
        )
        .select(
            "instrument_id",
            "session_date",
            "session_open_ts",
            "session_close_ts",
            "session_class",
        )
    )
    session_scope_df = with_session.select("instrument_id", "timeframe", "session_date").distinct()
    session_calendar_df = session_scope_df.join(
        official_session_bounds_df,
        on=["instrument_id", "session_date"],
        how="inner",
    )
    expected_session_rows = int(session_scope_df.count())
    resolved_session_rows = int(session_calendar_df.count())
    if expected_session_rows != resolved_session_rows:
        raise ValueError(
            "official session interval coverage is incomplete for canonical sidecars: "
            f"expected_session_rows={expected_session_rows}; "
            f"resolved_session_rows={resolved_session_rows}"
        )

    roll_window = window.partitionBy("instrument_id", "session_date").orderBy(
        functions.col("open_interest").desc(),
        functions.col("source_ts_close_last").desc(),
        functions.col("contract_id").desc(),
    )
    roll_map_df = (
        with_session.withColumn("_rn", functions.row_number().over(roll_window))
        .where(functions.col("_rn") == 1)
        .select(
            "instrument_id",
            "session_date",
            functions.col("contract_id").alias("active_contract_id"),
            functions.lit(SIDECAR_ROLL_REASON).alias("reason"),
        )
    )
    return session_calendar_df, roll_map_df, refresh_mode


def _expand_sidecar_sessions(*, affected_sessions_df: Any, functions: Any) -> Any:
    previous_sessions = affected_sessions_df.select(
        "instrument_id",
        functions.date_sub(functions.col("session_date"), 1).alias("session_date"),
    )
    next_sessions = affected_sessions_df.select(
        "instrument_id",
        functions.date_add(functions.col("session_date"), 1).alias("session_date"),
    )
    return affected_sessions_df.unionByName(previous_sessions).unionByName(next_sessions).distinct()


def _sessions_from_bars(*, bars_df: Any, functions: Any) -> Any:
    return (
        bars_df.select(
            "instrument_id",
            functions.to_date(functions.col("ts")).alias("session_date"),
        )
        .where(functions.col("session_date").isNotNull())
        .distinct()
    )


def _sessions_from_provenance(*, provenance_df: Any, functions: Any) -> Any:
    return (
        provenance_df.select(
            "instrument_id",
            functions.to_date(
                functions.coalesce(functions.col("bar_start_ts"), functions.col("ts"))
            ).alias("session_date"),
        )
        .where(functions.col("session_date").isNotNull())
        .distinct()
    )


def _materialize_sidecar_session_scope(*, dataframe: Any, table_path: Path) -> None:
    _write_delta_dataframe(
        dataframe=dataframe.select(*SIDECAR_SESSION_SCOPE_COLUMNS.keys()).distinct(),
        table_path=table_path,
        manifest_entry={"columns": dict(SIDECAR_SESSION_SCOPE_COLUMNS)},
    )


def _sidecar_stale_keys(
    *,
    target_dataframe: Any,
    replacement_dataframe: Any,
    scope_sessions_df: Any,
    key_columns: tuple[str, ...],
) -> Any:
    scoped_target_keys_df = (
        target_dataframe.join(scope_sessions_df, ["instrument_id", "session_date"], "inner")
        .select(*key_columns)
        .distinct()
    )
    replacement_keys_df = replacement_dataframe.select(*key_columns).distinct()
    return scoped_target_keys_df.join(replacement_keys_df, list(key_columns), "left_anti")


def run_moex_canonical_publish_spark_delta_job(
    *,
    staged_bars_path: Path,
    staged_provenance_path: Path,
    target_bars_path: Path,
    target_provenance_path: Path,
    session_calendar_path: Path,
    session_intervals_path: Path | None,
    roll_map_path: Path,
    output_dir: Path,
    run_id: str,
    publish_scope_path: Path | None = None,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    _ensure_spark_delta_runtime_available()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = historical_data_delta_schema_manifest()

    spark_session_factory = spark_session_factory or _create_spark_session
    spark = spark_session_factory("ta3000-moex-canonical-publish", spark_master)
    _, window, functions, _ = _load_spark_modules()

    try:
        staged_bars_df = _table_or_empty(
            spark,
            table_path=staged_bars_path,
            schema=_schema_from_columns(manifest["canonical_bars"]["columns"]),
        ).select(*manifest["canonical_bars"]["columns"].keys())
        staged_provenance_df = _table_or_empty(
            spark,
            table_path=staged_provenance_path,
            schema=_schema_from_columns(CANONICAL_PROVENANCE_COLUMNS),
        ).select(*CANONICAL_PROVENANCE_COLUMNS.keys())

        target_bars_df = _table_or_empty(
            spark,
            table_path=target_bars_path,
            schema=_schema_from_columns(manifest["canonical_bars"]["columns"]),
        ).select(*manifest["canonical_bars"]["columns"].keys())
        target_provenance_df = _table_or_empty(
            spark,
            table_path=target_provenance_path,
            schema=_schema_from_columns(CANONICAL_PROVENANCE_COLUMNS),
        ).select(*CANONICAL_PROVENANCE_COLUMNS.keys())

        impacted_scope_keys_df = _impacted_scope_keys(
            spark=spark,
            publish_scope_path=publish_scope_path,
            target_bars_df=target_bars_df,
            staged_provenance_df=staged_provenance_df,
            functions=functions,
        ).cache()
        staged_bar_keys_df = staged_bars_df.select(*CANONICAL_KEY_COLUMNS).distinct().cache()
        staged_provenance_keys_df = (
            staged_provenance_df.select(*CANONICAL_KEY_COLUMNS).distinct().cache()
        )
        stale_bars_df = (
            target_bars_df.join(impacted_scope_keys_df, list(CANONICAL_KEY_COLUMNS), "inner")
            .join(staged_bar_keys_df, list(CANONICAL_KEY_COLUMNS), "left_anti")
            .cache()
        )
        stale_provenance_df = (
            target_provenance_df.join(
                impacted_scope_keys_df,
                list(CANONICAL_KEY_COLUMNS),
                "inner",
            )
            .join(staged_provenance_keys_df, list(CANONICAL_KEY_COLUMNS), "left_anti")
            .cache()
        )
        replacement_bar_keys_df = staged_bar_keys_df.unionByName(
            stale_bars_df.select(*CANONICAL_KEY_COLUMNS)
        ).distinct()
        replacement_provenance_keys_df = staged_provenance_keys_df.unionByName(
            stale_provenance_df.select(*CANONICAL_KEY_COLUMNS)
        ).distinct()
        candidate_bars_df = target_bars_df.join(
            replacement_bar_keys_df,
            list(CANONICAL_KEY_COLUMNS),
            "left_anti",
        ).unionByName(staged_bars_df)
        candidate_provenance_df = target_provenance_df.join(
            replacement_provenance_keys_df,
            list(CANONICAL_KEY_COLUMNS),
            "left_anti",
        ).unionByName(staged_provenance_df)

        qc_report = _build_qc_report(
            bars_df=candidate_bars_df,
            provenance_df=candidate_provenance_df,
            run_id=run_id,
            window=window,
            functions=functions,
        )
        contract_report = _build_contract_compatibility_report(
            bars_df=candidate_bars_df,
            run_id=run_id,
            functions=functions,
        )
        publish_allowed = qc_report["status"] == "PASS" and contract_report["status"] == "PASS"
        staged_rows = int(staged_bars_df.count())
        impacted_scope_rows = int(impacted_scope_keys_df.count())
        stale_bar_rows = int(stale_bars_df.count())
        stale_provenance_rows = int(stale_provenance_df.count())
        stale_bar_keys_path = output_dir / "stale-keys" / "canonical_bars.delta"
        stale_provenance_keys_path = output_dir / "stale-keys" / "canonical_bar_provenance.delta"
        if stale_bar_rows > 0:
            _materialize_stale_keys(
                dataframe=stale_bars_df,
                table_path=stale_bar_keys_path,
                key_columns=CANONICAL_KEY_COLUMNS,
            )
        if stale_provenance_rows > 0:
            _materialize_stale_keys(
                dataframe=stale_provenance_df,
                table_path=stale_provenance_keys_path,
                key_columns=CANONICAL_KEY_COLUMNS,
            )
        affected_sessions_df = (
            _sessions_from_provenance(
                provenance_df=staged_provenance_df,
                functions=functions,
            )
            .unionByName(_sessions_from_bars(bars_df=stale_bars_df, functions=functions))
            .unionByName(
                _sessions_from_provenance(
                    provenance_df=stale_provenance_df,
                    functions=functions,
                )
            )
            .distinct()
            .cache()
        )
        affected_session_rows = int(affected_sessions_df.count())
        affected_sessions_path = output_dir / "sidecar-scope" / "affected_sessions.delta"
        if affected_session_rows > 0:
            _materialize_sidecar_session_scope(
                dataframe=affected_sessions_df,
                table_path=affected_sessions_path,
            )
        mutation_applied = False
        recovery_manifest_path = output_dir / "publish-recovery-manifest.json"
        publish_protocol: dict[str, object] = {
            "operation": "delta_merge_replace",
            "recoverable": True,
            "recovery_manifest_path": recovery_manifest_path.as_posix(),
            "publish_scope_path": publish_scope_path.as_posix()
            if publish_scope_path is not None
            else "",
            "scoped_replacement": {
                "impacted_scope_key_rows": impacted_scope_rows,
                "stale_bar_rows": stale_bar_rows,
                "stale_provenance_rows": stale_provenance_rows,
            },
            "pre_publish_versions": {
                "canonical_bars": _delta_table_version(spark, target_bars_path),
                "canonical_bar_provenance": _delta_table_version(spark, target_provenance_path),
                "canonical_session_calendar": _delta_table_version(spark, session_calendar_path),
                "canonical_roll_map": _delta_table_version(spark, roll_map_path),
            },
            "target_paths": {
                "canonical_bars": target_bars_path.as_posix(),
                "canonical_bar_provenance": target_provenance_path.as_posix(),
                "canonical_session_calendar": session_calendar_path.as_posix(),
                "canonical_roll_map": roll_map_path.as_posix(),
            },
            "staged_paths": {
                "canonical_bars": staged_bars_path.as_posix(),
                "canonical_bar_provenance": staged_provenance_path.as_posix(),
            },
            "sidecar_scope_path": affected_sessions_path.as_posix()
            if affected_session_rows > 0
            else "",
        }
        _write_json(recovery_manifest_path, publish_protocol)

        if publish_allowed and (staged_rows > 0 or stale_bar_rows > 0 or stale_provenance_rows > 0):
            if has_delta_log(target_bars_path):
                _merge_replace_delta_dataframe(
                    spark=spark,
                    table_path=target_bars_path,
                    staged_dataframe=staged_bars_df,
                    stale_keys_path=stale_bar_keys_path,
                    stale_key_count=stale_bar_rows,
                    key_columns=CANONICAL_KEY_COLUMNS,
                    functions=functions,
                )
            else:
                _write_delta_dataframe(
                    dataframe=staged_bars_df,
                    table_path=target_bars_path,
                    manifest_entry=dict(manifest["canonical_bars"]),
                )

            if has_delta_log(target_provenance_path):
                _merge_replace_delta_dataframe(
                    spark=spark,
                    table_path=target_provenance_path,
                    staged_dataframe=staged_provenance_df,
                    stale_keys_path=stale_provenance_keys_path,
                    stale_key_count=stale_provenance_rows,
                    key_columns=CANONICAL_KEY_COLUMNS,
                    functions=functions,
                )
            else:
                _write_delta_dataframe(
                    dataframe=staged_provenance_df,
                    table_path=target_provenance_path,
                    manifest_entry={"columns": dict(CANONICAL_PROVENANCE_COLUMNS)},
                )
            mutation_applied = True

        if mutation_applied:
            spark.catalog.clearCache()

        final_bars_df = _table_or_empty(
            spark,
            table_path=target_bars_path,
            schema=_schema_from_columns(manifest["canonical_bars"]["columns"]),
        ).select(*manifest["canonical_bars"]["columns"].keys())
        final_provenance_df = _table_or_empty(
            spark,
            table_path=target_provenance_path,
            schema=_schema_from_columns(CANONICAL_PROVENANCE_COLUMNS),
        ).select(*CANONICAL_PROVENANCE_COLUMNS.keys())

        if affected_session_rows > 0:
            affected_sessions_df = spark.read.format("delta").load(str(affected_sessions_path))
        else:
            affected_sessions_df = spark.createDataFrame(
                [],
                _schema_from_columns(SIDECAR_SESSION_SCOPE_COLUMNS),
            )
        sidecar_scope_sessions_df = _expand_sidecar_sessions(
            affected_sessions_df=affected_sessions_df,
            functions=functions,
        ).cache()
        sidecar_scope_session_rows = int(sidecar_scope_sessions_df.count())
        sidecars_exist = has_delta_log(session_calendar_path) and has_delta_log(roll_map_path)
        sidecar_mutation = False
        refreshed_session_rows = 0
        refreshed_roll_rows = 0
        refresh_mode = "noop"
        if (
            publish_allowed
            and session_intervals_path is not None
            and (not sidecars_exist or affected_session_rows > 0)
        ):
            session_calendar_df, roll_map_df, refresh_mode = _sidecar_frames(
                spark=spark,
                bars_df=final_bars_df,
                provenance_df=final_provenance_df,
                affected_sessions_df=sidecar_scope_sessions_df,
                session_intervals_path=session_intervals_path,
                sidecars_exist=sidecars_exist,
                window=window,
                functions=functions,
            )
            if sidecars_exist:
                target_session_calendar_df = _table_or_empty(
                    spark,
                    table_path=session_calendar_path,
                    schema=_schema_from_columns(manifest["canonical_session_calendar"]["columns"]),
                ).select(*manifest["canonical_session_calendar"]["columns"].keys())
                target_roll_map_df = _table_or_empty(
                    spark,
                    table_path=roll_map_path,
                    schema=_schema_from_columns(manifest["canonical_roll_map"]["columns"]),
                ).select(*manifest["canonical_roll_map"]["columns"].keys())
                stale_session_calendar_df = _sidecar_stale_keys(
                    target_dataframe=target_session_calendar_df,
                    replacement_dataframe=session_calendar_df,
                    scope_sessions_df=sidecar_scope_sessions_df,
                    key_columns=("instrument_id", "timeframe", "session_date"),
                ).cache()
                stale_roll_map_df = _sidecar_stale_keys(
                    target_dataframe=target_roll_map_df,
                    replacement_dataframe=roll_map_df,
                    scope_sessions_df=sidecar_scope_sessions_df,
                    key_columns=("instrument_id", "session_date"),
                ).cache()
                stale_session_calendar_rows = int(stale_session_calendar_df.count())
                stale_roll_map_rows = int(stale_roll_map_df.count())
                stale_session_calendar_path = (
                    output_dir / "stale-keys" / "canonical_session_calendar.delta"
                )
                stale_roll_map_path = output_dir / "stale-keys" / "canonical_roll_map.delta"
                if stale_session_calendar_rows > 0:
                    _materialize_stale_keys(
                        dataframe=stale_session_calendar_df,
                        table_path=stale_session_calendar_path,
                        key_columns=("instrument_id", "timeframe", "session_date"),
                    )
                if stale_roll_map_rows > 0:
                    _materialize_stale_keys(
                        dataframe=stale_roll_map_df,
                        table_path=stale_roll_map_path,
                        key_columns=("instrument_id", "session_date"),
                    )
                _merge_replace_delta_dataframe(
                    spark=spark,
                    table_path=session_calendar_path,
                    staged_dataframe=session_calendar_df,
                    stale_keys_path=stale_session_calendar_path,
                    stale_key_count=stale_session_calendar_rows,
                    key_columns=("instrument_id", "timeframe", "session_date"),
                    functions=functions,
                )
                _merge_replace_delta_dataframe(
                    spark=spark,
                    table_path=roll_map_path,
                    staged_dataframe=roll_map_df,
                    stale_keys_path=stale_roll_map_path,
                    stale_key_count=stale_roll_map_rows,
                    key_columns=("instrument_id", "session_date"),
                    functions=functions,
                )
            else:
                _write_delta_dataframe(
                    dataframe=session_calendar_df,
                    table_path=session_calendar_path,
                    manifest_entry=dict(manifest["canonical_session_calendar"]),
                )
                _write_delta_dataframe(
                    dataframe=roll_map_df,
                    table_path=roll_map_path,
                    manifest_entry=dict(manifest["canonical_roll_map"]),
                )
            refreshed_session_rows = int(session_calendar_df.count())
            refreshed_roll_rows = int(roll_map_df.count())
            sidecar_mutation = True
        elif publish_allowed and session_intervals_path is None:
            refresh_mode = "skipped_manual_session_backfill_required"

        canonical_rows = int(final_bars_df.count())
        provenance_rows = int(final_provenance_df.count())
        output_paths = {
            "canonical_bars": target_bars_path.as_posix(),
            "canonical_bar_provenance": target_provenance_path.as_posix(),
        }
        delta_log = {
            "canonical_bars": _delta_log_payload(target_bars_path),
            "canonical_bar_provenance": _delta_log_payload(target_provenance_path),
        }
        if session_intervals_path is not None:
            output_paths["canonical_session_intervals"] = session_intervals_path.as_posix()
            delta_log["canonical_session_intervals"] = _delta_log_payload(session_intervals_path)
        if has_delta_log(session_calendar_path):
            output_paths["canonical_session_calendar"] = session_calendar_path.as_posix()
            delta_log["canonical_session_calendar"] = _delta_log_payload(session_calendar_path)
        if has_delta_log(roll_map_path):
            output_paths["canonical_roll_map"] = roll_map_path.as_posix()
            delta_log["canonical_roll_map"] = _delta_log_payload(roll_map_path)
        return {
            "run_id": run_id,
            "runtime_owner": "spark_delta",
            "status": "PASS" if publish_allowed else "BLOCKED",
            "publish_decision": "publish" if publish_allowed else "blocked",
            "mutation_applied": mutation_applied,
            "scoped_canonical_rows": staged_rows,
            "canonical_rows": canonical_rows,
            "provenance_rows": provenance_rows,
            "qc_report": qc_report,
            "contract_compatibility_report": contract_report,
            "publish_protocol": publish_protocol,
            "sidecar_refresh": {
                "mode": refresh_mode,
                "mutation_applied": sidecar_mutation,
                "refreshed_session_calendar_rows": refreshed_session_rows,
                "refreshed_roll_map_rows": refreshed_roll_rows,
                "affected_session_rows": affected_session_rows,
                "overlap_session_rows": sidecar_scope_session_rows,
                "overlap_policy": SIDECAR_OVERLAP_POLICY,
            },
            "output_paths": output_paths,
            "delta_log": delta_log,
            "spark_profile": {
                "master": spark_master,
                "delta_writer": "spark",
            },
        }
    finally:
        try:
            spark.stop()
        except Exception:
            pass
