from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from trading_advisor_3000.product_plane.contracts import Timeframe
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log

from .canonical_bars_job import (
    DEFAULT_SPARK_MASTER,
    _create_spark_session,
    _load_spark_modules,
    _write_delta_dataframe,
)

TARGET_TIMEFRAME_TO_MINUTES: dict[str, int] = {
    Timeframe.M5.value: 5,
    Timeframe.M15.value: 15,
    Timeframe.H1.value: 60,
    Timeframe.H4.value: 240,
    Timeframe.D1.value: 1440,
    Timeframe.W1.value: 10080,
}
MOEX_SESSION_TIMEZONE = "Europe/Moscow"
SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS = 60

CANONICAL_BAR_SCHEMA = (
    "contract_id string, "
    "instrument_id string, "
    "timeframe string, "
    "ts timestamp, "
    "open double, "
    "high double, "
    "low double, "
    "close double, "
    "volume long, "
    "open_interest long"
)

CANONICAL_PROVENANCE_SCHEMA = (
    "contract_id string, "
    "instrument_id string, "
    "timeframe string, "
    "ts timestamp, "
    "bar_start_ts timestamp, "
    "bar_end_ts timestamp, "
    "session_interval_id string, "
    "source_provider string, "
    "source_timeframe string, "
    "source_interval int, "
    "source_run_id string, "
    "source_ingest_run_id string, "
    "source_row_count int, "
    "source_ts_open_first timestamp, "
    "source_ts_close_last timestamp, "
    "open_interest_imputed int, "
    "build_run_id string, "
    "built_at_utc timestamp"
)

CANONICAL_BAR_MANIFEST = {
    "columns": {
        "contract_id": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "ts": "timestamp",
        "open": "double",
        "high": "double",
        "low": "double",
        "close": "double",
        "volume": "bigint",
        "open_interest": "bigint",
    }
}

CANONICAL_PROVENANCE_MANIFEST = {
    "columns": {
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
}

SESSION_INTERVAL_SCHEMA = (
    "instrument_id string, "
    "session_date date, "
    "interval_id string, "
    "interval_seq int, "
    "expected_open_ts timestamp, "
    "expected_close_ts timestamp, "
    "session_class string, "
    "interval_type string, "
    "policy_id string, "
    "source_id string, "
    "source_document_hash string"
)


def _count_jsonl_rows(path: Path) -> int:
    rows = 0
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows += 1
    return rows


def _read_official_session_intervals(spark: Any, session_intervals_path: Path) -> Any:
    if has_delta_log(session_intervals_path):
        return spark.read.format("delta").load(str(session_intervals_path))
    if session_intervals_path.exists():
        return spark.read.json(str(session_intervals_path))
    raise FileNotFoundError(
        f"official session intervals input is missing: {session_intervals_path.as_posix()}"
    )


def _prepare_official_session_intervals(
    *,
    spark: Any,
    session_intervals_path: Path,
    functions: Any,
    window: Any,
) -> Any:
    intervals_df = _read_official_session_intervals(spark, session_intervals_path).select(
        functions.col("instrument_id").cast("string").alias("instrument_id"),
        functions.to_date(functions.col("session_date")).alias("session_date"),
        functions.col("interval_id").cast("string").alias("interval_id"),
        functions.col("interval_seq").cast("int").alias("interval_seq"),
        functions.to_timestamp(functions.col("expected_open_ts")).alias("expected_open_ts"),
        functions.to_timestamp(functions.col("expected_close_ts")).alias("expected_close_ts"),
        functions.col("session_class").cast("string").alias("session_class"),
        functions.col("interval_type").cast("string").alias("interval_type"),
        functions.col("policy_id").cast("string").alias("policy_id"),
        functions.col("source_id").cast("string").alias("source_id"),
        functions.col("source_document_hash").cast("string").alias("source_document_hash"),
    )
    invalid_provenance_rows = int(
        intervals_df.where(
            functions.col("source_id").isNull()
            | (functions.trim(functions.col("source_id")) == "")
            | functions.col("source_document_hash").isNull()
            | (functions.trim(functions.col("source_document_hash")) == "")
        ).count()
    )
    invalid_interval_rows = int(
        intervals_df.where(
            functions.col("instrument_id").isNull()
            | (functions.trim(functions.col("instrument_id")) == "")
            | functions.col("session_date").isNull()
            | functions.col("interval_id").isNull()
            | (functions.trim(functions.col("interval_id")) == "")
            | functions.col("interval_seq").isNull()
            | (functions.col("interval_seq") <= functions.lit(0))
            | functions.col("expected_open_ts").isNull()
            | functions.col("expected_close_ts").isNull()
            | (functions.col("expected_open_ts") >= functions.col("expected_close_ts"))
        ).count()
    )
    duplicate_interval_rows = int(
        intervals_df.groupBy("instrument_id", "session_date", "interval_id")
        .count()
        .where(functions.col("count") > functions.lit(1))
        .count()
    )
    sequence_window = window.partitionBy("instrument_id", "session_date").orderBy(
        functions.col("expected_open_ts").asc(),
        functions.col("expected_close_ts").asc(),
        functions.col("interval_id").asc(),
    )
    unstable_sequence_rows = int(
        intervals_df.withColumn(
            "_expected_interval_seq",
            functions.row_number().over(sequence_window),
        )
        .where(functions.col("interval_seq") != functions.col("_expected_interval_seq"))
        .count()
    )
    cross_date_rows = int(
        intervals_df.where(
            (
                functions.to_date(
                    functions.from_utc_timestamp(
                        functions.col("expected_open_ts"),
                        MOEX_SESSION_TIMEZONE,
                    )
                )
                != functions.col("session_date")
            )
            | (
                functions.to_date(
                    functions.from_utc_timestamp(
                        functions.col("expected_close_ts") - functions.expr("INTERVAL 1 SECOND"),
                        MOEX_SESSION_TIMEZONE,
                    )
                )
                != functions.col("session_date")
            )
        ).count()
    )
    ordered_window = window.partitionBy("instrument_id", "session_date").orderBy(
        functions.col("expected_open_ts").asc(),
        functions.col("expected_close_ts").asc(),
        functions.col("interval_seq").asc(),
    )
    overlap_rows = int(
        intervals_df.withColumn(
            "_previous_close_ts",
            functions.lag(functions.col("expected_close_ts")).over(ordered_window),
        )
        .where(
            functions.col("_previous_close_ts").isNotNull()
            & (functions.col("expected_open_ts") < functions.col("_previous_close_ts"))
        )
        .count()
    )
    if (
        invalid_provenance_rows
        or invalid_interval_rows
        or duplicate_interval_rows
        or unstable_sequence_rows
        or cross_date_rows
        or overlap_rows
    ):
        raise ValueError(
            "official session intervals failed QC: "
            f"invalid_provenance_rows={invalid_provenance_rows}; "
            f"invalid_interval_rows={invalid_interval_rows}; "
            f"duplicate_interval_rows={duplicate_interval_rows}; "
            f"unstable_sequence_rows={unstable_sequence_rows}; "
            f"cross_date_rows={cross_date_rows}; overlap_rows={overlap_rows}"
        )
    return intervals_df


def _aggregate_joined_source_outputs(
    *,
    joined: Any,
    bucket_seconds: Any,
    build_run_id: str,
    built_at_utc: str,
    functions: Any,
    window: Any,
    enforce_session_bounds: bool,
) -> tuple[Any, Any]:
    if "interval_id" not in joined.columns:
        joined = joined.withColumn("interval_id", functions.lit(None).cast("string"))
    joined = (
        joined.withColumn("bucket_seconds", bucket_seconds.cast("long"))
        .withColumn("bucket_ts", functions.to_timestamp(functions.from_unixtime("bucket_seconds")))
        .withColumn(
            "bucket_end_ts",
            functions.to_timestamp(
                functions.from_unixtime(
                    functions.col("bucket_seconds")
                    + (functions.col("target_minutes") * functions.lit(60))
                )
            ),
        )
    )
    if enforce_session_bounds:
        joined = joined.where(
            (functions.col("target_minutes") >= functions.lit(1440))
            | (
                (functions.col("bucket_ts") >= functions.col("expected_open_ts"))
                & (functions.col("bucket_end_ts") <= functions.col("expected_close_ts"))
            )
        )

    partition_columns = ["contract_id", "instrument_id", "timeframe", "bucket_ts"]
    first_window = window.partitionBy(*partition_columns).orderBy(
        functions.col("ts_open").asc(),
        functions.col("ts_close").asc(),
    )
    last_window = window.partitionBy(*partition_columns).orderBy(
        functions.col("ts_open").desc(),
        functions.col("ts_close").desc(),
    )

    annotated = joined.withColumn("rn_first", functions.row_number().over(first_window)).withColumn(
        "rn_last", functions.row_number().over(last_window)
    )

    aggregated = annotated.groupBy(*partition_columns).agg(
        functions.max(functions.when(functions.col("rn_first") == 1, functions.col("open"))).alias(
            "open"
        ),
        functions.max(functions.col("high")).alias("high"),
        functions.min(functions.col("low")).alias("low"),
        functions.max(functions.when(functions.col("rn_last") == 1, functions.col("close"))).alias(
            "close"
        ),
        functions.sum(functions.col("volume")).cast("long").alias("volume"),
        functions.max(functions.when(functions.col("rn_last") == 1, functions.col("open_interest")))
        .cast("long")
        .alias("open_interest"),
        functions.max(
            functions.when(functions.col("rn_last") == 1, functions.col("source_provider"))
        ).alias("source_provider"),
        functions.max(
            functions.when(functions.col("rn_last") == 1, functions.col("source_timeframe"))
        ).alias("source_timeframe"),
        functions.max(
            functions.when(functions.col("rn_last") == 1, functions.col("source_interval"))
        )
        .cast("int")
        .alias("selected_source_interval"),
        functions.max(
            functions.when(functions.col("rn_last") == 1, functions.col("source_run_id"))
        ).alias("source_run_id"),
        functions.max(
            functions.when(functions.col("rn_last") == 1, functions.col("source_ingest_run_id"))
        ).alias("source_ingest_run_id"),
        functions.count(functions.lit(1)).cast("int").alias("source_row_count"),
        functions.min(functions.col("ts_open")).alias("source_ts_open_first"),
        functions.max(
            functions.when(functions.col("rn_last") == 1, functions.col("ts_close"))
        ).alias("source_ts_close_last"),
        functions.max(
            functions.when(
                (functions.col("rn_last") == 1)
                & (functions.col("target_minutes") < functions.lit(1440)),
                functions.col("interval_id"),
            )
        ).alias("session_interval_id"),
        functions.max(functions.col("open_interest_imputed").cast("int"))
        .cast("int")
        .alias("open_interest_imputed"),
    )

    bars_df = aggregated.select(
        "contract_id",
        "instrument_id",
        "timeframe",
        functions.col("bucket_ts").alias("ts"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
    )

    provenance_df = aggregated.select(
        "contract_id",
        "instrument_id",
        "timeframe",
        functions.col("bucket_ts").alias("ts"),
        functions.col("source_ts_open_first").alias("bar_start_ts"),
        functions.col("source_ts_close_last").alias("bar_end_ts"),
        "session_interval_id",
        "source_provider",
        "source_timeframe",
        functions.col("selected_source_interval").alias("source_interval"),
        "source_run_id",
        "source_ingest_run_id",
        "source_row_count",
        "source_ts_open_first",
        "source_ts_close_last",
        "open_interest_imputed",
        functions.lit(str(build_run_id).strip()).alias("build_run_id"),
        functions.to_timestamp(functions.lit(str(built_at_utc).strip())).alias("built_at_utc"),
    )
    return bars_df, provenance_df


def _build_session_bounded_outputs(
    *,
    spark: Any,
    source_df: Any,
    selected_df: Any,
    session_intervals_path: Path,
    build_run_id: str,
    built_at_utc: str,
    functions: Any,
    window: Any,
) -> tuple[Any, Any, dict[str, object]]:
    intervals_df = _prepare_official_session_intervals(
        spark=spark,
        session_intervals_path=session_intervals_path,
        functions=functions,
        window=window,
    )
    selected_df = selected_df.where(functions.col("source_interval") == functions.lit(1))
    selected_interval_rows = int(selected_df.count())
    if selected_interval_rows == 0:
        return (
            spark.createDataFrame([], CANONICAL_BAR_SCHEMA),
            spark.createDataFrame([], CANONICAL_PROVENANCE_SCHEMA),
            {
                "official_session_interval_rows": int(intervals_df.count()),
                "admission_open_tolerance_seconds": SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS,
                "admitted_source_rows": 0,
                "rejected_out_of_session_rows": 0,
                "rejected_non_1m_source_rows": int(source_df.count()),
                "selected_source_interval_rows": 0,
                "rejected_samples": [],
            },
        )

    minute_source_df = source_df.where(
        (functions.col("source_timeframe") == functions.lit("1m"))
        & (functions.col("source_interval") == functions.lit(1))
    ).withColumn("_source_row_id", functions.monotonically_increasing_id())
    non_1m_source_rows = int(source_df.count()) - int(minute_source_df.count())
    affected_scope_df = minute_source_df.select(
        "instrument_id",
        functions.to_date(
            functions.from_utc_timestamp(functions.col("ts_open"), MOEX_SESSION_TIMEZONE)
        ).alias("session_date"),
    ).distinct()
    covered_scope_df = intervals_df.select("instrument_id", "session_date").distinct()
    missing_official_coverage_rows = int(
        affected_scope_df.join(
            covered_scope_df,
            on=["instrument_id", "session_date"],
            how="left_anti",
        ).count()
    )
    if missing_official_coverage_rows:
        raise ValueError(
            "official session intervals missing affected-scope coverage: "
            f"missing_official_coverage_rows={missing_official_coverage_rows}"
        )

    source_alias = minute_source_df.alias("source")
    intervals_alias = intervals_df.alias("intervals")
    admission_open_ts = functions.col("intervals.expected_open_ts") - functions.expr(
        f"INTERVAL {SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS} SECONDS"
    )
    admitted = (
        source_alias.join(
            intervals_alias,
            (functions.col("source.instrument_id") == functions.col("intervals.instrument_id"))
            & (functions.col("source.ts_open") >= admission_open_ts)
            & (functions.col("source.ts_close") <= functions.col("intervals.expected_close_ts")),
            "inner",
        )
        .select(
            functions.col("source._source_row_id").alias("_source_row_id"),
            functions.col("source.contract_id").alias("contract_id"),
            functions.col("source.instrument_id").alias("instrument_id"),
            functions.col("source.source_timeframe").alias("source_timeframe"),
            functions.col("source.source_interval").alias("source_interval"),
            functions.col("source.ts_open").alias("ts_open"),
            functions.col("source.ts_close").alias("ts_close"),
            functions.col("source.open").alias("open"),
            functions.col("source.high").alias("high"),
            functions.col("source.low").alias("low"),
            functions.col("source.close").alias("close"),
            functions.col("source.volume").alias("volume"),
            functions.col("source.open_interest").alias("open_interest"),
            functions.col("source.open_interest_imputed").alias("open_interest_imputed"),
            functions.col("source.source_provider").alias("source_provider"),
            functions.col("source.source_run_id").alias("source_run_id"),
            functions.col("source.source_ingest_run_id").alias("source_ingest_run_id"),
            functions.col("intervals.session_date").alias("session_date"),
            functions.col("intervals.interval_id").alias("interval_id"),
            functions.col("intervals.expected_open_ts").alias("expected_open_ts"),
            functions.col("intervals.expected_close_ts").alias("expected_close_ts"),
        )
        .cache()
    )
    admitted_source_rows = int(admitted.count())
    admitted_ids = admitted.select("_source_row_id").distinct()
    rejected_df = minute_source_df.join(admitted_ids, on="_source_row_id", how="left_anti")
    rejected_out_of_session_rows = int(rejected_df.count())
    rejected_samples = [
        str(row.asDict())
        for row in rejected_df.select(
            "contract_id", "instrument_id", "source_timeframe", "ts_open", "ts_close"
        )
        .limit(20)
        .collect()
    ]

    selected_alias = selected_df.alias("selected")
    joined = admitted.alias("source").join(
        selected_alias,
        on=["contract_id", "instrument_id", "source_interval"],
        how="inner",
    )
    bucket_seconds = (
        functions.when(
            functions.col("target_minutes") == functions.lit(1440),
            functions.unix_timestamp(
                functions.to_timestamp(functions.col("session_date").cast("string"))
            ),
        )
        .when(
            functions.col("target_minutes") == functions.lit(10080),
            functions.unix_timestamp(
                functions.date_trunc(
                    "week",
                    functions.to_timestamp(functions.col("session_date").cast("string")),
                )
            ),
        )
        .otherwise(
            functions.unix_timestamp(functions.col("expected_open_ts"))
            + (
                functions.floor(
                    (
                        functions.unix_timestamp(functions.col("ts_open"))
                        - functions.unix_timestamp(functions.col("expected_open_ts"))
                    )
                    / (functions.col("target_minutes") * functions.lit(60))
                )
                * (functions.col("target_minutes") * functions.lit(60))
            )
        )
    )
    bars_df, provenance_df = _aggregate_joined_source_outputs(
        joined=joined,
        bucket_seconds=bucket_seconds,
        build_run_id=build_run_id,
        built_at_utc=built_at_utc,
        functions=functions,
        window=window,
        enforce_session_bounds=True,
    )
    return (
        bars_df,
        provenance_df,
        {
            "official_session_interval_rows": int(intervals_df.count()),
            "admission_open_tolerance_seconds": SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS,
            "missing_official_coverage_rows": missing_official_coverage_rows,
            "admitted_source_rows": admitted_source_rows,
            "rejected_out_of_session_rows": rejected_out_of_session_rows,
            "rejected_non_1m_source_rows": non_1m_source_rows,
            "selected_source_interval_rows": selected_interval_rows,
            "rejected_samples": rejected_samples,
        },
    )


def _build_unbounded_outputs(
    *,
    spark: Any,
    source_df: Any,
    selected_df: Any,
    build_run_id: str,
    built_at_utc: str,
    functions: Any,
    window: Any,
) -> tuple[Any, Any, dict[str, object]]:
    selected_interval_rows = int(selected_df.count())
    if selected_interval_rows == 0:
        return (
            spark.createDataFrame([], CANONICAL_BAR_SCHEMA),
            spark.createDataFrame([], CANONICAL_PROVENANCE_SCHEMA),
            {
                "mode": "not_applied_manual_backfill_missing",
                "official_session_interval_rows": 0,
                "missing_official_coverage_rows": 0,
                "admitted_source_rows": 0,
                "rejected_out_of_session_rows": 0,
                "rejected_non_1m_source_rows": 0,
                "selected_source_interval_rows": 0,
                "rejected_samples": [],
            },
        )

    joined = source_df.alias("source").join(
        selected_df.alias("selected"),
        on=["contract_id", "instrument_id", "source_interval"],
        how="inner",
    )
    admitted_source_rows = int(joined.count())
    bucket_seconds = functions.floor(
        functions.unix_timestamp(functions.col("ts_open"))
        / (functions.col("target_minutes") * functions.lit(60))
    ) * (functions.col("target_minutes") * functions.lit(60))
    bars_df, provenance_df = _aggregate_joined_source_outputs(
        joined=joined,
        bucket_seconds=bucket_seconds,
        build_run_id=build_run_id,
        built_at_utc=built_at_utc,
        functions=functions,
        window=window,
        enforce_session_bounds=False,
    )
    return (
        bars_df,
        provenance_df,
        {
            "mode": "not_applied_manual_backfill_missing",
            "official_session_interval_rows": 0,
            "missing_official_coverage_rows": 0,
            "admitted_source_rows": admitted_source_rows,
            "rejected_out_of_session_rows": 0,
            "rejected_non_1m_source_rows": 0,
            "selected_source_interval_rows": selected_interval_rows,
            "rejected_samples": [],
        },
    )


def run_moex_canonicalization_spark_job(
    *,
    normalized_source_path: Path,
    selected_source_intervals_path: Path,
    session_intervals_path: Path | None,
    output_dir: Path,
    build_run_id: str,
    built_at_utc: str,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    bars_path = output_dir / "delta" / "canonical_bars.delta"
    provenance_path = output_dir / "delta" / "canonical_bar_provenance.delta"

    source_row_count = _count_jsonl_rows(normalized_source_path)
    selected_interval_row_count = _count_jsonl_rows(selected_source_intervals_path)
    if source_row_count > 0 and selected_interval_row_count > 0 and session_intervals_path is None:
        raise ValueError("official session intervals input is required for Spark canonicalization")
    session_admission_report: dict[str, object] = {
        "official_session_interval_rows": 0,
        "admitted_source_rows": 0,
        "rejected_out_of_session_rows": 0,
        "rejected_non_1m_source_rows": 0,
        "selected_source_interval_rows": selected_interval_row_count,
        "rejected_samples": [],
    }

    spark_session_factory = spark_session_factory or _create_spark_session
    spark = spark_session_factory("ta3000-moex-canonicalization", spark_master)
    _, window, F, _ = _load_spark_modules()

    try:
        if source_row_count == 0 or selected_interval_row_count == 0:
            bars_df = spark.createDataFrame([], CANONICAL_BAR_SCHEMA)
            provenance_df = spark.createDataFrame([], CANONICAL_PROVENANCE_SCHEMA)
        else:
            source_df = spark.read.json(str(normalized_source_path)).select(
                F.col("contract_id").cast("string").alias("contract_id"),
                F.col("instrument_id").cast("string").alias("instrument_id"),
                F.col("source_timeframe").cast("string").alias("source_timeframe"),
                F.col("source_interval").cast("int").alias("source_interval"),
                F.to_timestamp(F.col("ts_open")).alias("ts_open"),
                F.to_timestamp(F.col("ts_close")).alias("ts_close"),
                F.col("open").cast("double").alias("open"),
                F.col("high").cast("double").alias("high"),
                F.col("low").cast("double").alias("low"),
                F.col("close").cast("double").alias("close"),
                F.col("volume").cast("long").alias("volume"),
                F.col("open_interest").cast("long").alias("open_interest"),
                F.col("open_interest_imputed").cast("boolean").alias("open_interest_imputed"),
                F.col("source_provider").cast("string").alias("source_provider"),
                F.col("source_run_id").cast("string").alias("source_run_id"),
                F.col("source_ingest_run_id").cast("string").alias("source_ingest_run_id"),
            )

            selected_df = spark.read.json(str(selected_source_intervals_path)).select(
                F.col("contract_id").cast("string").alias("contract_id"),
                F.col("instrument_id").cast("string").alias("instrument_id"),
                F.col("timeframe").cast("string").alias("timeframe"),
                F.col("target_minutes").cast("int").alias("target_minutes"),
                F.col("source_interval").cast("int").alias("source_interval"),
            )
            if session_intervals_path is None:
                bars_df, provenance_df, session_admission_report = _build_unbounded_outputs(
                    spark=spark,
                    source_df=source_df,
                    selected_df=selected_df,
                    build_run_id=build_run_id,
                    built_at_utc=built_at_utc,
                    functions=F,
                    window=window,
                )
            else:
                bars_df, provenance_df, session_admission_report = _build_session_bounded_outputs(
                    spark=spark,
                    source_df=source_df,
                    selected_df=selected_df,
                    session_intervals_path=session_intervals_path,
                    build_run_id=build_run_id,
                    built_at_utc=built_at_utc,
                    functions=F,
                    window=window,
                )

        _write_delta_dataframe(
            dataframe=bars_df,
            table_path=bars_path,
            manifest_entry=dict(CANONICAL_BAR_MANIFEST),
        )
        _write_delta_dataframe(
            dataframe=provenance_df,
            table_path=provenance_path,
            manifest_entry=dict(CANONICAL_PROVENANCE_MANIFEST),
        )

        return {
            "engine": "spark",
            "input_mode": "normalized_jsonl",
            "build_run_id": str(build_run_id).strip(),
            "built_at_utc": str(built_at_utc).strip(),
            "source_rows": source_row_count,
            "selected_source_interval_rows": selected_interval_row_count,
            "session_admission_report": session_admission_report,
            "canonical_rows": int(bars_df.count()),
            "provenance_rows": int(provenance_df.count()),
            "output_paths": {
                "canonical_bars": bars_path.as_posix(),
                "canonical_bar_provenance": provenance_path.as_posix(),
            },
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


def run_moex_canonicalization_spark_delta_job(
    *,
    raw_table_path: Path,
    changed_windows_path: Path,
    selected_source_intervals_path: Path,
    session_intervals_path: Path | None,
    output_dir: Path,
    build_run_id: str,
    built_at_utc: str,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    bars_path = output_dir / "delta" / "canonical_bars.delta"
    provenance_path = output_dir / "delta" / "canonical_bar_provenance.delta"

    changed_window_row_count = _count_jsonl_rows(changed_windows_path)
    selected_interval_row_count = _count_jsonl_rows(selected_source_intervals_path)
    session_admission_report: dict[str, object] = {
        "official_session_interval_rows": 0,
        "admitted_source_rows": 0,
        "rejected_out_of_session_rows": 0,
        "rejected_non_1m_source_rows": 0,
        "selected_source_interval_rows": selected_interval_row_count,
        "rejected_samples": [],
    }

    spark_session_factory = spark_session_factory or _create_spark_session
    spark = spark_session_factory("ta3000-moex-canonicalization", spark_master)
    _, window, F, _ = _load_spark_modules()

    try:
        unmatched_window_rows = 0
        unmatched_window_samples: list[str] = []
        source_providers: list[str] = []
        if changed_window_row_count == 0 or selected_interval_row_count == 0:
            source_row_count = 0
            bars_df = spark.createDataFrame([], CANONICAL_BAR_SCHEMA)
            provenance_df = spark.createDataFrame([], CANONICAL_PROVENANCE_SCHEMA)
        else:
            raw_df = spark.read.format("delta").load(str(raw_table_path))
            windows_df = spark.read.json(str(changed_windows_path)).select(
                F.col("internal_id").cast("string").alias("internal_id"),
                F.col("source_timeframe").cast("string").alias("timeframe"),
                F.when(
                    (F.col("source_timeframe") == F.lit("1d"))
                    & (F.col("source_interval").cast("int") == F.lit(1440)),
                    F.lit(24),
                )
                .when(
                    (F.col("source_timeframe") == F.lit("1w"))
                    & (F.col("source_interval").cast("int") == F.lit(10080)),
                    F.lit(7),
                )
                .otherwise(F.col("source_interval").cast("int"))
                .alias("source_interval"),
                F.col("moex_secid").cast("string").alias("moex_secid"),
                F.to_timestamp(F.col("window_start_utc")).alias("window_start_utc"),
                F.to_timestamp(F.col("window_end_utc")).alias("window_end_utc"),
            )
            scoped_raw_df = raw_df.join(
                windows_df,
                on=["internal_id", "timeframe", "source_interval", "moex_secid"],
                how="inner",
            ).where(
                (F.to_timestamp(F.col("ts_close")) >= F.col("window_start_utc"))
                & (F.to_timestamp(F.col("ts_close")) <= F.col("window_end_utc"))
            )
            matched_windows_df = scoped_raw_df.select(
                "internal_id",
                "timeframe",
                "source_interval",
                "moex_secid",
                "window_start_utc",
                "window_end_utc",
            ).distinct()
            unmatched_windows_df = windows_df.join(
                matched_windows_df,
                on=[
                    "internal_id",
                    "timeframe",
                    "source_interval",
                    "moex_secid",
                    "window_start_utc",
                    "window_end_utc",
                ],
                how="left_anti",
            )
            unmatched_window_rows = int(unmatched_windows_df.count())
            unmatched_window_samples = [
                str(row.asDict()) for row in unmatched_windows_df.limit(20).collect()
            ]
            provenance_text = F.col("provenance_json").cast("string")
            source_df = scoped_raw_df.select(
                F.col("finam_symbol").cast("string").alias("contract_id"),
                F.col("moex_secid").cast("string").alias("moex_secid"),
                F.col("internal_id").cast("string").alias("instrument_id"),
                F.col("timeframe").cast("string").alias("source_timeframe"),
                F.when(F.col("timeframe") == F.lit("1d"), F.lit(1440))
                .when(F.col("timeframe") == F.lit("1w"), F.lit(10080))
                .otherwise(F.col("source_interval").cast("int"))
                .alias("source_interval"),
                F.to_timestamp(F.col("ts_open")).alias("ts_open"),
                F.to_timestamp(F.col("ts_close")).alias("ts_close"),
                F.col("open").cast("double").alias("open"),
                F.col("high").cast("double").alias("high"),
                F.col("low").cast("double").alias("low"),
                F.col("close").cast("double").alias("close"),
                F.col("volume").cast("long").alias("volume"),
                F.coalesce(F.col("open_interest").cast("long"), F.lit(0)).alias("open_interest"),
                F.col("open_interest").isNull().alias("open_interest_imputed"),
                F.coalesce(
                    F.get_json_object(provenance_text, "$.source_provider"), F.lit("")
                ).alias("source_provider"),
                F.coalesce(F.get_json_object(provenance_text, "$.run_id"), F.lit("")).alias(
                    "source_run_id"
                ),
                F.col("ingest_run_id").cast("string").alias("source_ingest_run_id"),
            )
            source_window = window.partitionBy(
                "contract_id",
                "instrument_id",
                "source_interval",
                "ts_open",
            ).orderBy(F.col("ts_close").desc())
            source_df = (
                source_df.withColumn("_rn", F.row_number().over(source_window))
                .where(F.col("_rn") == 1)
                .drop("_rn")
            )
            source_row_count = int(source_df.count())
            if source_row_count > 0 and session_intervals_path is None:
                raise ValueError(
                    "official session intervals input is required for Spark canonicalization"
                )
            source_providers = sorted(
                str(row["source_provider"]).strip()
                for row in source_df.select("source_provider").distinct().collect()
                if str(row["source_provider"]).strip()
            )

            selected_raw_df = spark.read.json(str(selected_source_intervals_path))
            selected_moex_secid = (
                F.col("moex_secid").cast("string")
                if "moex_secid" in selected_raw_df.columns
                else F.lit(None).cast("string")
            )
            selected_df = selected_raw_df.select(
                F.col("contract_id").cast("string").alias("contract_id"),
                selected_moex_secid.alias("moex_secid"),
                F.col("instrument_id").cast("string").alias("instrument_id"),
                F.col("timeframe").cast("string").alias("timeframe"),
                F.col("target_minutes").cast("int").alias("target_minutes"),
                F.col("source_interval").cast("int").alias("source_interval"),
            )

            source_alias = source_df.alias("source")
            selected_source_keys_df = selected_df.select(
                "contract_id",
                "moex_secid",
                "instrument_id",
                "source_interval",
            ).distinct()
            selected_alias = selected_source_keys_df.alias("selected")
            source_for_canonicalization_df = source_alias.join(
                selected_alias,
                (
                    (F.col("source.instrument_id") == F.col("selected.instrument_id"))
                    & (F.col("source.source_interval") == F.col("selected.source_interval"))
                    & (
                        (F.col("source.contract_id") == F.col("selected.contract_id"))
                        | (F.col("source.moex_secid") == F.col("selected.contract_id"))
                        | (
                            F.col("selected.moex_secid").isNotNull()
                            & (F.col("source.moex_secid") == F.col("selected.moex_secid"))
                        )
                    )
                ),
                how="inner",
            ).select(
                F.col("source.contract_id").alias("contract_id"),
                F.col("source.instrument_id").alias("instrument_id"),
                F.col("source.source_timeframe").alias("source_timeframe"),
                F.col("source.source_interval").alias("source_interval"),
                F.col("source.ts_open").alias("ts_open"),
                F.col("source.ts_close").alias("ts_close"),
                F.col("source.open").alias("open"),
                F.col("source.high").alias("high"),
                F.col("source.low").alias("low"),
                F.col("source.close").alias("close"),
                F.col("source.volume").alias("volume"),
                F.col("source.open_interest").alias("open_interest"),
                F.col("source.open_interest_imputed").alias("open_interest_imputed"),
                F.col("source.source_provider").alias("source_provider"),
                F.col("source.source_run_id").alias("source_run_id"),
                F.col("source.source_ingest_run_id").alias("source_ingest_run_id"),
            )
            source_contract_keys_df = source_df.select(
                "contract_id",
                "moex_secid",
                "instrument_id",
                "source_interval",
            ).distinct()
            source_contract_alias = source_contract_keys_df.alias("source_key")
            selected_for_canonicalization_df = (
                selected_df.alias("selected")
                .join(
                    source_contract_alias,
                    (
                        (F.col("source_key.instrument_id") == F.col("selected.instrument_id"))
                        & (F.col("source_key.source_interval") == F.col("selected.source_interval"))
                        & (
                            (F.col("source_key.contract_id") == F.col("selected.contract_id"))
                            | (F.col("source_key.moex_secid") == F.col("selected.contract_id"))
                            | (
                                F.col("selected.moex_secid").isNotNull()
                                & (F.col("source_key.moex_secid") == F.col("selected.moex_secid"))
                            )
                        )
                    ),
                    how="inner",
                )
                .select(
                    F.col("source_key.contract_id").alias("contract_id"),
                    F.col("selected.instrument_id").alias("instrument_id"),
                    F.col("selected.timeframe").alias("timeframe"),
                    F.col("selected.target_minutes").alias("target_minutes"),
                    F.col("selected.source_interval").alias("source_interval"),
                )
            )
            if session_intervals_path is None:
                (
                    bars_df,
                    provenance_df,
                    session_admission_report,
                ) = _build_unbounded_outputs(
                    spark=spark,
                    source_df=source_for_canonicalization_df,
                    selected_df=selected_for_canonicalization_df,
                    build_run_id=build_run_id,
                    built_at_utc=built_at_utc,
                    functions=F,
                    window=window,
                )
            else:
                (
                    bars_df,
                    provenance_df,
                    session_admission_report,
                ) = _build_session_bounded_outputs(
                    spark=spark,
                    source_df=source_for_canonicalization_df,
                    selected_df=selected_for_canonicalization_df,
                    session_intervals_path=session_intervals_path,
                    build_run_id=build_run_id,
                    built_at_utc=built_at_utc,
                    functions=F,
                    window=window,
                )

        _write_delta_dataframe(
            dataframe=bars_df,
            table_path=bars_path,
            manifest_entry=dict(CANONICAL_BAR_MANIFEST),
        )
        _write_delta_dataframe(
            dataframe=provenance_df,
            table_path=provenance_path,
            manifest_entry=dict(CANONICAL_PROVENANCE_MANIFEST),
        )

        return {
            "engine": "spark",
            "input_mode": "raw_delta",
            "build_run_id": str(build_run_id).strip(),
            "built_at_utc": str(built_at_utc).strip(),
            "source_rows": source_row_count,
            "source_providers": source_providers,
            "unmatched_window_rows": unmatched_window_rows,
            "unmatched_windows": unmatched_window_samples,
            "changed_window_rows": changed_window_row_count,
            "selected_source_interval_rows": selected_interval_row_count,
            "session_admission_report": session_admission_report,
            "canonical_rows": int(bars_df.count()),
            "provenance_rows": int(provenance_df.count()),
            "output_paths": {
                "canonical_bars": bars_path.as_posix(),
                "canonical_bar_provenance": provenance_path.as_posix(),
            },
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
