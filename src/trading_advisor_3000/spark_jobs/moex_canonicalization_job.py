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
    Timeframe.M1.value: 1,
    Timeframe.M5.value: 5,
    Timeframe.M15.value: 15,
    Timeframe.H1.value: 60,
    Timeframe.H4.value: 240,
    Timeframe.D1.value: 1440,
    Timeframe.W1.value: 10080,
}
MOEX_SESSION_TIMEZONE = "Europe/Moscow"
SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS = 60
SOURCE_ADMISSION_ID_COLUMNS = (
    "contract_id",
    "instrument_id",
    "source_timeframe",
    "source_interval",
    "ts_open",
    "ts_close",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "open_interest_imputed",
    "source_provider",
    "source_run_id",
    "source_ingest_run_id",
)
SOURCE_FOR_CANONICALIZATION_DEDUP_COLUMNS = (
    "contract_id",
    "instrument_id",
    "source_timeframe",
    "source_interval",
    "ts_open",
    "ts_close",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "open_interest_imputed",
    "source_provider",
    "source_run_id",
    "source_ingest_run_id",
)
SELECTED_FOR_CANONICALIZATION_DEDUP_COLUMNS = (
    "contract_id",
    "instrument_id",
    "timeframe",
    "target_minutes",
    "source_interval",
)
JOINED_SOURCE_DEDUP_COLUMNS = (
    *SELECTED_FOR_CANONICALIZATION_DEDUP_COLUMNS,
    "source_timeframe",
    "ts_open",
    "ts_close",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "open_interest_imputed",
    "source_provider",
    "source_run_id",
    "source_ingest_run_id",
)

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
    "built_at_utc timestamp, "
    "open_interest_trade_date date, "
    "open_interest_source_trade_date date, "
    "open_interest_availability_status string"
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
        "open_interest_trade_date": "date",
        "open_interest_source_trade_date": "date",
        "open_interest_availability_status": "string",
    }
}


def _stable_source_admission_id(functions: Any) -> Any:
    return functions.sha2(
        functions.to_json(
            functions.struct(
                *(
                    functions.col(column_name).alias(column_name)
                    for column_name in SOURCE_ADMISSION_ID_COLUMNS
                )
            )
        ),
        256,
    )


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


def _daily_open_interest_effective_dates(
    *, daily: Any, intervals: Any, closed_dates: Any, functions: Any, window: Any
) -> tuple[Any, dict[str, object]]:
    """Model D -> next verified open date, without crossing unknown calendar days."""
    F = functions
    keys = ["instrument_id", "moex_secid", "trade_date"]
    invalid = daily.where(
        F.col("instrument_id").isNull()
        | F.col("moex_secid").isNull()
        | F.col("trade_date").isNull()
        | (F.col("open_interest") < 0)
    ).count()
    duplicates = daily.groupBy(*keys).count().where(F.col("count") > 1).count()
    if invalid or duplicates:
        raise ValueError(f"daily OI invalid_rows={invalid}, duplicate_keys={duplicates}")
    known = (
        intervals.select("instrument_id", "session_date")
        .distinct()
        .withColumn("is_open", F.lit(True))
        .unionByName(
            closed_dates.select("instrument_id", "session_date").withColumn("is_open", F.lit(False))
        )
        .groupBy("instrument_id", "session_date")
        .agg(F.max("is_open").alias("is_open"))
    )
    order = window.partitionBy("instrument_id").orderBy("session_date")
    known = known.withColumn("_seq", F.row_number().over(order))
    known = known.withColumn(
        "_next",
        F.first(F.when(F.col("is_open"), F.struct("session_date", "_seq")), ignorenulls=True).over(
            order.rowsBetween(1, window.unboundedFollowing)
        ),
    )
    calendar = known.select(
        "instrument_id",
        F.col("session_date").alias("trade_date"),
        F.when(
            F.col("_next._seq") - F.col("_seq")
            == F.datediff(F.col("_next.session_date"), F.col("session_date")),
            F.col("_next.session_date"),
        ).alias("effective_trade_date"),
    )
    aligned = daily.join(calendar, ["instrument_id", "trade_date"], "left").persist()
    counts = (
        aligned.agg(
            F.count("*").alias("source_rows"),
            F.sum(F.col("effective_trade_date").isNull().cast("long")).alias(
                "unresolved_calendar_rows"
            ),
            F.sum(F.col("open_interest").isNull().cast("long")).alias("source_null_rows"),
            F.sum((F.col("open_interest") == 0).cast("long")).alias("observed_zero_rows"),
        )
        .first()
        .asDict()
    )
    # A closed source date can collide with another observation's next open day.
    # Do not invent a ranking/tie-break policy for such observations.
    collision_keys = ["instrument_id", "moex_secid", "effective_trade_date"]
    collisions = (
        aligned.where(F.col("effective_trade_date").isNotNull())
        .groupBy(*collision_keys)
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    if collisions:
        raise ValueError(f"daily OI effective-date collisions={collisions}")
    return aligned, {
        **counts,
        "availability_status": "ASSUMED_NEXT_TRADING_DATE",
        "carry_forward": False,
        "same_trade_date_use": False,
    }


def _with_daily_open_interest(*, source: Any, daily: Any, contract_map: Any, functions: Any) -> Any:
    """Fill only absent native OI; exact effective-date joins preserve missingness."""
    F = functions
    observations = (
        daily.where(F.col("effective_trade_date").isNotNull())
        .join(
            contract_map.select("instrument_id", "moex_secid", "contract_id").distinct(),
            ["instrument_id", "moex_secid"],
            "inner",
        )
        .select(
            "instrument_id",
            "contract_id",
            F.col("effective_trade_date").alias("open_interest_trade_date"),
            F.col("trade_date").alias("_oi_source_date"),
            F.col("open_interest").alias("_daily_oi"),
        )
    )
    keys = ["instrument_id", "contract_id", "open_interest_trade_date"]
    duplicates = observations.groupBy(*keys).count().where(F.col("count") > 1).count()
    if duplicates:
        raise ValueError(f"daily OI ambiguous canonical contract keys={duplicates}")
    joined = source.join(observations, keys, "left")
    native = F.col("open_interest").isNotNull()
    return (
        joined.withColumn(
            "open_interest_source_trade_date", F.when(~native, F.col("_oi_source_date"))
        )
        .withColumn(
            "open_interest_availability_status",
            F.when(native, F.lit("NATIVE_OBSERVED"))
            .when(F.col("_daily_oi").isNotNull(), F.lit("ASSUMED_NEXT_TRADING_DATE"))
            .when(F.col("_oi_source_date").isNotNull(), F.lit("SOURCE_NULL"))
            .otherwise(F.lit("MISSING_OBSERVATION")),
        )
        .withColumn("open_interest_imputed", (~native & F.col("_daily_oi").isNotNull()).cast("int"))
        .withColumn("open_interest", F.coalesce("open_interest", "_daily_oi"))
        .drop("_oi_source_date", "_daily_oi")
    )


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
    joined = joined.dropDuplicates(list(JOINED_SOURCE_DEDUP_COLUMNS))
    for name, dtype in [
        ("open_interest_trade_date", "date"),
        ("open_interest_source_trade_date", "date"),
        ("open_interest_availability_status", "string"),
    ]:
        if name not in joined.columns:
            joined = joined.withColumn(name, functions.lit(None).cast(dtype))
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
    # Session admission validates source coverage. Partial clock buckets at the
    # session edges are retained; no source candle may cross its target bucket.
    invalid_bucket_rows = int(
        joined.where(
            (functions.col("target_minutes") < functions.lit(1440))
            & (
                (functions.col("ts_open") < functions.col("bucket_ts"))
                | (functions.col("ts_close") > functions.col("bucket_end_ts"))
            )
        ).count()
    )
    if invalid_bucket_rows:
        raise ValueError(f"source_outside_target_bucket_rows={invalid_bucket_rows}")

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

    aggregated = (
        annotated.groupBy(*partition_columns)
        .agg(
            functions.max(
                functions.when(functions.col("rn_first") == 1, functions.col("open"))
            ).alias("open"),
            functions.max(
                functions.greatest(
                    functions.col("open"),
                    functions.col("high"),
                    functions.col("low"),
                    functions.col("close"),
                )
            ).alias("high"),
            functions.min(
                functions.least(
                    functions.col("open"),
                    functions.col("high"),
                    functions.col("low"),
                    functions.col("close"),
                )
            ).alias("low"),
            functions.max(
                functions.when(functions.col("rn_last") == 1, functions.col("close"))
            ).alias("close"),
            functions.sum(functions.col("volume")).cast("long").alias("volume"),
            functions.max(
                functions.when(functions.col("rn_last") == 1, functions.col("open_interest"))
            )
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
            functions.min(functions.col("ts_open")).alias("bar_start_ts"),
            functions.max(
                functions.when(functions.col("rn_last") == 1, functions.col("ts_close"))
            ).alias("bar_end_ts"),
            functions.max(
                functions.when(
                    (functions.col("target_minutes") < functions.lit(1440))
                    & (functions.col("rn_last") == 1),
                    functions.col("interval_id").cast("string"),
                )
            ).alias("session_interval_id"),
            *[
                functions.max(
                    functions.when(functions.col("rn_last") == 1, functions.col(name))
                ).alias(name)
                for name in [
                    "open_interest_trade_date",
                    "open_interest_source_trade_date",
                    "open_interest_availability_status",
                ]
            ],
            functions.max(
                functions.when(
                    functions.col("rn_last") == 1,
                    functions.col("open_interest_imputed").cast("int"),
                )
            )
            .cast("int")
            .alias("open_interest_imputed"),
        )
        .persist()
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
        "bar_start_ts",
        "bar_end_ts",
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
        "open_interest_trade_date",
        "open_interest_source_trade_date",
        "open_interest_availability_status",
        functions.lit(str(build_run_id).strip()).alias("build_run_id"),
        functions.to_timestamp(functions.lit(str(built_at_utc).strip())).alias("built_at_utc"),
    )
    return bars_df, provenance_df


def _prepare_closed_session_dates(
    *, spark: Any, closed_session_dates_path: Path | None, intervals_df: Any, functions: Any
) -> Any:
    if closed_session_dates_path is None:
        return spark.createDataFrame([], "instrument_id STRING, session_date DATE")
    if not has_delta_log(closed_session_dates_path):
        raise FileNotFoundError(
            f"closed session dates input missing Delta log: {closed_session_dates_path}"
        )
    closed = (
        spark.read.format("delta")
        .load(str(closed_session_dates_path))
        .select(
            functions.col("instrument_id").cast("string").alias("instrument_id"),
            functions.to_date("session_date").alias("session_date"),
            "source_id",
            "source_document_hash",
            "policy_id",
        )
    )
    invalid = functions.col("session_date").isNull()
    for field in ("instrument_id", "source_id", "source_document_hash", "policy_id"):
        invalid = (
            invalid | functions.col(field).isNull() | (functions.trim(functions.col(field)) == "")
        )
    invalid_rows = closed.where(invalid).count()
    duplicates = closed.groupBy("instrument_id", "session_date").count().where("count > 1").count()
    conflicts = closed.join(
        intervals_df.select("instrument_id", "session_date").distinct(),
        ["instrument_id", "session_date"],
        "inner",
    ).count()
    if invalid_rows or duplicates or conflicts:
        raise ValueError(
            f"closed session dates failed QC: invalid_closed_date_rows={invalid_rows}; "
            f"duplicate_closed_dates={duplicates}; conflicting_open_closed_dates={conflicts}"
        )
    return closed.select("instrument_id", "session_date")


def _source_session_end_date(functions: Any) -> Any:
    # ISS W1 `end` labels the last included trading date, including at midnight.
    # Keep the original timestamp for source provenance; interpret only its date here.
    moex_weekly = (
        (functions.col("source_provider") == functions.lit("moex_iss"))
        & (functions.col("source_timeframe") == functions.lit("1w"))
        & (functions.col("source_interval") == functions.lit(10080))
    )
    end_timestamp = functions.when(moex_weekly, functions.col("ts_close")).otherwise(
        functions.col("ts_close") - functions.expr("INTERVAL 1 SECOND")
    )
    return functions.to_date(functions.from_utc_timestamp(end_timestamp, MOEX_SESSION_TIMEZONE))


def _intraday_clock_bucket_seconds(functions: Any) -> Any:
    local_open = functions.from_utc_timestamp(functions.col("ts_open"), MOEX_SESSION_TIMEZONE)
    width = functions.col("target_minutes") * functions.lit(60)
    local_bucket = functions.to_timestamp(
        functions.from_unixtime(
            functions.floor(functions.unix_timestamp(local_open) / width) * width
        )
    )
    return functions.unix_timestamp(functions.to_utc_timestamp(local_bucket, MOEX_SESSION_TIMEZONE))


def _build_session_bounded_outputs(
    *,
    spark: Any,
    source_df: Any,
    selected_df: Any,
    session_intervals_path: Path,
    closed_session_dates_path: Path | None = None,
    daily_open_interest_df: Any | None = None,
    contract_map_df: Any | None = None,
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
    closed_dates_df = _prepare_closed_session_dates(
        spark=spark,
        closed_session_dates_path=closed_session_dates_path,
        intervals_df=intervals_df,
        functions=functions,
    )
    selected_interval_rows = int(selected_df.count())
    source_row_count = int(source_df.count())
    if selected_interval_rows == 0:
        return (
            spark.createDataFrame([], CANONICAL_BAR_SCHEMA),
            spark.createDataFrame([], CANONICAL_PROVENANCE_SCHEMA),
            {
                "daily_open_interest": {"mode": "no_selected_sources"},
                "official_session_interval_rows": int(intervals_df.count()),
                "admission_open_tolerance_seconds": SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS,
                "admitted_source_rows": 0,
                "out_of_schedule_rows": 0,
                "ignored_out_of_schedule_rows": 0,
                "ignored_out_of_schedule_reason_counts": {},
                "ignored_out_of_schedule_samples": [],
                "rejected_out_of_session_rows": 0,
                "rejected_non_1m_source_rows": 0,
                "ignored_opening_boundary_rows": 0,
                "ignored_opening_boundary_samples": [],
                "unselected_source_rows": source_row_count,
                "selected_source_interval_rows": 0,
                "rejected_samples": [],
            },
        )

    selected_source_keys_df = selected_df.select(
        "contract_id",
        "instrument_id",
        "source_interval",
    ).distinct()
    source_for_admission_df = (
        source_df.join(
            selected_source_keys_df,
            on=["contract_id", "instrument_id", "source_interval"],
            how="inner",
        )
        .withColumn("_source_row_id", _stable_source_admission_id(functions))
        .withColumn(
            "_session_date",
            functions.to_date(
                functions.from_utc_timestamp(functions.col("ts_open"), MOEX_SESSION_TIMEZONE)
            ),
        )
        .withColumn(
            "_session_end_date",
            _source_session_end_date(functions),
        )
        .cache()
    )
    selected_source_rows = int(source_for_admission_df.count())
    source_for_admission_df = source_for_admission_df.localCheckpoint(eager=True)
    unselected_source_rows = source_row_count - selected_source_rows
    affected_single_session_scope_df = source_for_admission_df.where(
        functions.col("source_interval") <= functions.lit(1440)
    ).select(
        "instrument_id",
        functions.col("_session_date").alias("session_date"),
    )
    multi_day_required_dates_df = source_for_admission_df.where(
        functions.col("source_interval") > functions.lit(1440)
    ).select(
        "_source_row_id",
        "instrument_id",
        functions.explode(
            functions.sequence(
                functions.col("_session_date"),
                functions.col("_session_end_date"),
            )
        ).alias("session_date"),
    )
    closed_session_dates_used = int(
        multi_day_required_dates_df.select("instrument_id", "session_date")
        .distinct()
        .join(closed_dates_df, ["instrument_id", "session_date"], "inner")
        .count()
    )
    multi_day_open_dates_df = multi_day_required_dates_df.join(
        closed_dates_df, ["instrument_id", "session_date"], "left_anti"
    )
    closed_only_source_spans = int(
        multi_day_required_dates_df.select("_source_row_id")
        .distinct()
        .join(
            multi_day_open_dates_df.select("_source_row_id").distinct(),
            "_source_row_id",
            "left_anti",
        )
        .count()
    )
    if closed_only_source_spans:
        raise ValueError(f"closed_only_source_spans={closed_only_source_spans}")
    multi_day_required_dates_df = multi_day_open_dates_df
    affected_scope_df = (
        affected_single_session_scope_df.unionByName(
            multi_day_required_dates_df.select("instrument_id", "session_date")
        )
        .distinct()
        .cache()
    )
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

    def _admitted_columns(
        source_alias_name: str, intervals_alias_name: str, *, multi_day: bool = False
    ) -> list[Any]:
        return [
            functions.col(f"{source_alias_name}._source_row_id").alias("_source_row_id"),
            functions.col(f"{source_alias_name}.contract_id").alias("contract_id"),
            functions.col(f"{source_alias_name}.instrument_id").alias("instrument_id"),
            functions.col(f"{source_alias_name}.source_timeframe").alias("source_timeframe"),
            functions.col(f"{source_alias_name}.source_interval").alias("source_interval"),
            functions.col(f"{source_alias_name}.ts_open").alias("ts_open"),
            functions.col(f"{source_alias_name}.ts_close").alias("ts_close"),
            functions.col(f"{source_alias_name}.open").alias("open"),
            functions.col(f"{source_alias_name}.high").alias("high"),
            functions.col(f"{source_alias_name}.low").alias("low"),
            functions.col(f"{source_alias_name}.close").alias("close"),
            functions.col(f"{source_alias_name}.volume").alias("volume"),
            functions.col(f"{source_alias_name}.open_interest").alias("open_interest"),
            functions.col(f"{source_alias_name}.open_interest_imputed").alias(
                "open_interest_imputed"
            ),
            functions.col(f"{source_alias_name}.source_provider").alias("source_provider"),
            functions.col(f"{source_alias_name}.source_run_id").alias("source_run_id"),
            functions.col(f"{source_alias_name}.source_ingest_run_id").alias(
                "source_ingest_run_id"
            ),
            functions.col(f"{intervals_alias_name}.session_date").alias("session_date"),
            functions.col(
                f"{intervals_alias_name}.open_interest_trade_date"
                if multi_day
                else f"{intervals_alias_name}.session_date"
            ).alias("open_interest_trade_date"),
            functions.col(f"{intervals_alias_name}.interval_id").alias("interval_id"),
            functions.col(f"{intervals_alias_name}.expected_open_ts").alias("expected_open_ts"),
            functions.col(f"{intervals_alias_name}.expected_close_ts").alias("expected_close_ts"),
        ]

    def _cut_lineage(dataframe: Any) -> Any:
        return dataframe.localCheckpoint(eager=True)

    intervals_alias = intervals_df.alias("intervals")
    admission_open_ts = functions.col("intervals.expected_open_ts") - functions.expr(
        f"INTERVAL {SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS} SECONDS"
    )
    intraday_source_alias = source_for_admission_df.where(
        functions.col("source_interval") < functions.lit(1440)
    ).alias("source")
    # MOEX coarse begin values label clock buckets, including partial opening
    # buckets. Their actual end must still be inside the admitted session.
    source_seconds = functions.col("source.source_interval") * functions.lit(60)
    coarse_open = functions.to_timestamp(
        functions.from_unixtime(
            functions.floor(functions.unix_timestamp(admission_open_ts) / source_seconds)
            * source_seconds
        )
    )
    labelled_coarse = (
        (functions.col("source.source_provider") == functions.lit("moex_iss"))
        & (functions.col("source.source_interval") > functions.lit(1))
        & (functions.unix_timestamp(functions.col("source.ts_open")) % source_seconds == 0)
    )
    earliest_open = functions.when(labelled_coarse, coarse_open).otherwise(admission_open_ts)
    admission_order = window.partitionBy(functions.col("source._source_row_id")).orderBy(
        (functions.col("source.ts_open") >= functions.col("intervals.expected_open_ts")).desc(),
        functions.col("intervals.expected_open_ts").asc(),
        functions.col("intervals.interval_id").asc(),
    )
    admitted_intraday = (
        intraday_source_alias.join(
            intervals_alias,
            (functions.col("source.instrument_id") == functions.col("intervals.instrument_id"))
            & (functions.col("source._session_date") == functions.col("intervals.session_date"))
            & (functions.col("source.ts_open") >= earliest_open)
            & (functions.col("source.ts_close") >= admission_open_ts)
            & (functions.col("source.ts_close") <= functions.col("intervals.expected_close_ts"))
            & (functions.col("source.ts_close") != functions.col("intervals.expected_open_ts")),
            "inner",
        )
        .withColumn("_admission_rank", functions.row_number().over(admission_order))
        .where(functions.col("_admission_rank") == 1)
        .select(*_admitted_columns("source", "intervals"))
    )
    session_coverage_df = (
        intervals_df.groupBy("instrument_id", "session_date")
        .agg(
            functions.min("interval_id").alias("interval_id"),
            functions.min("expected_open_ts").alias("expected_open_ts"),
            functions.max("expected_close_ts").alias("expected_close_ts"),
        )
        .alias("coverage")
    )
    daily_source_alias = source_for_admission_df.where(
        functions.col("source_interval") == functions.lit(1440)
    ).alias("source")
    admitted_daily = daily_source_alias.join(
        session_coverage_df,
        (functions.col("source.instrument_id") == functions.col("coverage.instrument_id"))
        & (functions.col("source._session_date") == functions.col("coverage.session_date")),
        "inner",
    ).select(*_admitted_columns("source", "coverage"))
    multi_day_span_coverage_df = (
        multi_day_required_dates_df.alias("required")
        .join(
            session_coverage_df.alias("coverage"),
            (functions.col("required.instrument_id") == functions.col("coverage.instrument_id"))
            & (functions.col("required.session_date") == functions.col("coverage.session_date")),
            "left",
        )
        .groupBy(functions.col("required._source_row_id").alias("_source_row_id"))
        .agg(
            functions.countDistinct(functions.col("required.session_date")).alias(
                "_required_session_dates"
            ),
            functions.count(functions.col("coverage.session_date")).alias("_covered_session_dates"),
            functions.min(functions.col("coverage.session_date")).alias("session_date"),
            functions.max(functions.col("coverage.session_date")).alias("open_interest_trade_date"),
            functions.min(functions.col("coverage.interval_id")).alias("interval_id"),
            functions.min(functions.col("coverage.expected_open_ts")).alias("expected_open_ts"),
            functions.max(functions.col("coverage.expected_close_ts")).alias("expected_close_ts"),
        )
        .where(functions.col("_required_session_dates") == functions.col("_covered_session_dates"))
        .alias("coverage")
    )
    multi_day_source_alias = source_for_admission_df.where(
        functions.col("source_interval") > functions.lit(1440)
    ).alias("source")
    admitted_multi_day = multi_day_source_alias.join(
        multi_day_span_coverage_df,
        on="_source_row_id",
        how="inner",
    ).select(*_admitted_columns("source", "coverage", multi_day=True))
    admitted = admitted_intraday.unionByName(admitted_daily).unionByName(admitted_multi_day).cache()
    admitted_source_rows = int(admitted.count())
    admitted = _cut_lineage(admitted)
    admitted_ids = admitted.select("_source_row_id").distinct()
    raw_rejected_df = source_for_admission_df.join(
        admitted_ids, on="_source_row_id", how="left_anti"
    ).cache()
    out_of_schedule_rows = int(raw_rejected_df.count())
    raw_rejected_df = _cut_lineage(raw_rejected_df)
    rejected_coarse_intraday_df = raw_rejected_df.where(
        (functions.col("source_interval") > functions.lit(1))
        & (functions.col("source_interval") < functions.lit(1440))
    ).cache()
    rejected_coarse_intraday_df = _cut_lineage(rejected_coarse_intraday_df)
    coarse = rejected_coarse_intraday_df.alias("coarse")
    finer = admitted.alias("finer")
    finer_matches_df = (
        coarse.join(
            finer,
            (functions.col("coarse.contract_id") == functions.col("finer.contract_id"))
            & (functions.col("coarse.instrument_id") == functions.col("finer.instrument_id"))
            & (functions.col("coarse._session_date") == functions.col("finer.session_date"))
            & (functions.col("finer.source_interval") < functions.col("coarse.source_interval"))
            & (functions.col("finer.ts_open") >= functions.col("coarse.ts_open"))
            & (functions.col("finer.ts_close") <= functions.col("coarse.ts_close")),
            "inner",
        )
        .select(
            functions.col("coarse._source_row_id").alias("_source_row_id"),
            functions.col("finer.ts_open").alias("finer_ts_open"),
            functions.col("finer.ts_close").alias("finer_ts_close"),
            functions.col("finer.open").alias("finer_open"),
            functions.col("finer.high").alias("finer_high"),
            functions.col("finer.low").alias("finer_low"),
            functions.col("finer.close").alias("finer_close"),
            functions.col("finer.volume").alias("finer_volume"),
        )
        .cache()
    )
    finer_matches_df = _cut_lineage(finer_matches_df)
    finer_covered_aggregates_df = finer_matches_df.groupBy("_source_row_id").agg(
        functions.count(functions.lit(1)).alias("covered_finer_rows"),
        functions.max(functions.col("finer_high")).alias("covered_high"),
        functions.min(functions.col("finer_low")).alias("covered_low"),
        functions.sum(functions.col("finer_volume")).cast("long").alias("covered_volume"),
    )
    first_finer_window = window.partitionBy("_source_row_id").orderBy(
        functions.col("finer_ts_open").asc(), functions.col("finer_ts_close").asc()
    )
    last_finer_window = window.partitionBy("_source_row_id").orderBy(
        functions.col("finer_ts_open").desc(), functions.col("finer_ts_close").desc()
    )
    covered_open_df = (
        finer_matches_df.withColumn("_rn", functions.row_number().over(first_finer_window))
        .where(functions.col("_rn") == functions.lit(1))
        .select("_source_row_id", functions.col("finer_open").alias("covered_open"))
    )
    covered_close_df = (
        finer_matches_df.withColumn("_rn", functions.row_number().over(last_finer_window))
        .where(functions.col("_rn") == functions.lit(1))
        .select("_source_row_id", functions.col("finer_close").alias("covered_close"))
    )
    rejected_coarse_comparison_df = rejected_coarse_intraday_df.select(
        "_source_row_id",
        functions.col("open").alias("coarse_open"),
        functions.col("high").alias("coarse_high"),
        functions.col("low").alias("coarse_low"),
        functions.col("close").alias("coarse_close"),
        functions.col("volume").alias("coarse_volume"),
    )
    finer_covered_coarse_bucket_ids = (
        rejected_coarse_comparison_df.join(
            finer_covered_aggregates_df, on="_source_row_id", how="inner"
        )
        .join(covered_open_df, on="_source_row_id", how="inner")
        .join(covered_close_df, on="_source_row_id", how="inner")
        .where(
            (functions.col("covered_finer_rows") > functions.lit(0))
            & functions.col("coarse_open").eqNullSafe(functions.col("covered_open"))
            & functions.col("coarse_high").eqNullSafe(functions.col("covered_high"))
            & functions.col("coarse_low").eqNullSafe(functions.col("covered_low"))
            & functions.col("coarse_close").eqNullSafe(functions.col("covered_close"))
            & functions.col("coarse_volume").eqNullSafe(functions.col("covered_volume"))
        )
        .select("_source_row_id")
        .distinct()
        .cache()
    )
    finer_covered_coarse_bucket_ids = _cut_lineage(finer_covered_coarse_bucket_ids)
    ignored_out_of_schedule_ids = (
        finer_covered_coarse_bucket_ids.withColumn(
            "reason", functions.lit("finer_covered_coarse_bucket")
        )
        .select("_source_row_id", "reason")
        .cache()
    )
    ignored_out_of_schedule_rows = int(ignored_out_of_schedule_ids.count())
    ignored_out_of_schedule_reason_counts = {
        str(row["reason"]): int(row["count"])
        for row in ignored_out_of_schedule_ids.groupBy("reason").count().collect()
    }
    ignored_out_of_schedule_samples = [
        str(row.asDict())
        for row in raw_rejected_df.join(
            ignored_out_of_schedule_ids, on="_source_row_id", how="inner"
        )
        .select(
            "reason",
            "contract_id",
            "instrument_id",
            "source_timeframe",
            "ts_open",
            "ts_close",
        )
        .limit(20)
        .collect()
    ]
    opening_boundary_candidates_df = (
        rejected_coarse_intraday_df.alias("source")
        .join(
            intervals_alias,
            (functions.col("source.instrument_id") == functions.col("intervals.instrument_id"))
            & (functions.col("source._session_date") == functions.col("intervals.session_date"))
            & (functions.col("source.ts_open") < admission_open_ts)
            & (functions.col("source.ts_close") >= admission_open_ts)
            & (functions.col("source.ts_close") < functions.col("intervals.expected_open_ts")),
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
            functions.col("intervals.session_date").alias("session_date"),
            functions.col("intervals.expected_open_ts").alias("expected_open_ts"),
        )
        .cache()
    )
    corroborated_opening_boundary_ids = (
        opening_boundary_candidates_df.join(
            ignored_out_of_schedule_ids.select("_source_row_id"),
            on="_source_row_id",
            how="inner",
        )
        .select("_source_row_id")
        .distinct()
        .cache()
    )
    ignored_opening_boundary_rows = int(corroborated_opening_boundary_ids.count())
    ignored_opening_boundary_samples = [
        str(row.asDict())
        for row in opening_boundary_candidates_df.join(
            corroborated_opening_boundary_ids, on="_source_row_id", how="inner"
        )
        .select("contract_id", "instrument_id", "source_timeframe", "ts_open", "ts_close")
        .limit(20)
        .collect()
    ]
    rejected_df = raw_rejected_df.join(
        ignored_out_of_schedule_ids.select("_source_row_id"), on="_source_row_id", how="left_anti"
    )
    rejected_out_of_session_rows = int(rejected_df.count())
    rejected_samples = [
        str(row.asDict())
        for row in rejected_df.select(
            "contract_id", "instrument_id", "source_timeframe", "ts_open", "ts_close"
        )
        .limit(20)
        .collect()
    ]

    oi_report: dict[str, object] = {"mode": "native_only"}
    if daily_open_interest_df is not None:
        if contract_map_df is None:
            raise ValueError("daily OI requires a verified raw contract mapping")
        aligned_oi, oi_report = _daily_open_interest_effective_dates(
            daily=daily_open_interest_df,
            intervals=intervals_df,
            closed_dates=closed_dates_df,
            functions=functions,
            window=window,
        )
        admitted = _with_daily_open_interest(
            source=admitted,
            daily=aligned_oi,
            contract_map=contract_map_df,
            functions=functions,
        )
    else:
        admitted = (
            admitted.withColumn("open_interest_source_trade_date", functions.lit(None).cast("date"))
            .withColumn(
                "open_interest_availability_status",
                functions.when(
                    functions.col("open_interest").isNotNull(), "NATIVE_OBSERVED"
                ).otherwise("MISSING_OBSERVATION"),
            )
            .withColumn("open_interest_imputed", functions.lit(0))
        )
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
        .otherwise(_intraday_clock_bucket_seconds(functions))
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
            "daily_open_interest": oi_report,
            "official_session_interval_rows": int(intervals_df.count()),
            "admission_open_tolerance_seconds": SESSION_ADMISSION_OPEN_TOLERANCE_SECONDS,
            "missing_official_coverage_rows": missing_official_coverage_rows,
            "closed_session_dates_used": closed_session_dates_used,
            "admitted_source_rows": admitted_source_rows,
            "out_of_schedule_rows": out_of_schedule_rows,
            "ignored_out_of_schedule_rows": ignored_out_of_schedule_rows,
            "ignored_out_of_schedule_reason_counts": ignored_out_of_schedule_reason_counts,
            "ignored_out_of_schedule_samples": ignored_out_of_schedule_samples,
            "rejected_out_of_session_rows": rejected_out_of_session_rows,
            "rejected_non_1m_source_rows": 0,
            "ignored_opening_boundary_rows": ignored_opening_boundary_rows,
            "ignored_opening_boundary_samples": ignored_opening_boundary_samples,
            "unselected_source_rows": unselected_source_rows,
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
                "out_of_schedule_rows": 0,
                "ignored_out_of_schedule_rows": 0,
                "ignored_out_of_schedule_reason_counts": {},
                "ignored_out_of_schedule_samples": [],
                "rejected_out_of_session_rows": 0,
                "rejected_non_1m_source_rows": 0,
                "ignored_opening_boundary_rows": 0,
                "ignored_opening_boundary_samples": [],
                "selected_source_interval_rows": 0,
                "rejected_samples": [],
            },
        )

    joined = (
        source_df.alias("source")
        .join(
            selected_df.alias("selected"),
            on=["contract_id", "instrument_id", "source_interval"],
            how="inner",
        )
        .withColumn(
            "interval_id",
            functions.lit(None).cast("string"),
        )
    )
    admitted_source_rows = int(joined.count())
    bucket_seconds = functions.when(
        functions.col("target_minutes") < functions.lit(1440),
        _intraday_clock_bucket_seconds(functions),
    ).otherwise(
        functions.floor(
            functions.unix_timestamp(functions.col("ts_open"))
            / (functions.col("target_minutes") * functions.lit(60))
        )
        * (functions.col("target_minutes") * functions.lit(60))
    )
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
            "out_of_schedule_rows": 0,
            "ignored_out_of_schedule_rows": 0,
            "ignored_out_of_schedule_reason_counts": {},
            "ignored_out_of_schedule_samples": [],
            "rejected_out_of_session_rows": 0,
            "rejected_non_1m_source_rows": 0,
            "ignored_opening_boundary_rows": 0,
            "ignored_opening_boundary_samples": [],
            "selected_source_interval_rows": selected_interval_rows,
            "rejected_samples": [],
        },
    )


def run_moex_canonicalization_spark_job(
    *,
    normalized_source_path: Path,
    selected_source_intervals_path: Path,
    session_intervals_path: Path | None,
    closed_session_dates_path: Path | None = None,
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
        "out_of_schedule_rows": 0,
        "ignored_out_of_schedule_rows": 0,
        "ignored_out_of_schedule_reason_counts": {},
        "ignored_out_of_schedule_samples": [],
        "rejected_out_of_session_rows": 0,
        "rejected_non_1m_source_rows": 0,
        "ignored_opening_boundary_rows": 0,
        "ignored_opening_boundary_samples": [],
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
                    closed_session_dates_path=closed_session_dates_path,
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
    closed_session_dates_path: Path | None = None,
    daily_open_interest_path: Path | None = None,
    daily_open_interest_version: int | None = None,
    output_dir: Path,
    build_run_id: str,
    built_at_utc: str,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    if daily_open_interest_path is not None and (
        isinstance(daily_open_interest_version, bool)
        or not isinstance(daily_open_interest_version, int)
        or daily_open_interest_version < 0
    ):
        raise ValueError("daily OI requires a pinned non-negative Delta version")
    output_dir.mkdir(parents=True, exist_ok=True)
    bars_path = output_dir / "delta" / "canonical_bars.delta"
    provenance_path = output_dir / "delta" / "canonical_bar_provenance.delta"

    changed_window_row_count = _count_jsonl_rows(changed_windows_path)
    selected_interval_row_count = _count_jsonl_rows(selected_source_intervals_path)
    session_admission_report: dict[str, object] = {
        "official_session_interval_rows": 0,
        "admitted_source_rows": 0,
        "out_of_schedule_rows": 0,
        "ignored_out_of_schedule_rows": 0,
        "ignored_out_of_schedule_reason_counts": {},
        "ignored_out_of_schedule_samples": [],
        "rejected_out_of_session_rows": 0,
        "rejected_non_1m_source_rows": 0,
        "ignored_opening_boundary_rows": 0,
        "ignored_opening_boundary_samples": [],
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
                F.col("open_interest").cast("long").alias("open_interest"),
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
                .persist()
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
            source_for_canonicalization_df = (
                source_alias.join(
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
                )
                .select(
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
                .dropDuplicates(list(SOURCE_FOR_CANONICALIZATION_DEDUP_COLUMNS))
                .persist()
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
                .dropDuplicates(list(SELECTED_FOR_CANONICALIZATION_DEDUP_COLUMNS))
                .persist()
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
                    closed_session_dates_path=closed_session_dates_path,
                    daily_open_interest_df=(
                        spark.read.format("delta")
                        .option("versionAsOf", daily_open_interest_version)
                        .load(str(daily_open_interest_path))
                        .select(
                            F.col("internal_id").alias("instrument_id"),
                            "moex_secid",
                            F.col("trade_date").cast("date").alias("trade_date"),
                            F.col("open_interest").cast("long").alias("open_interest"),
                        )
                        if daily_open_interest_path is not None
                        else None
                    ),
                    contract_map_df=source_df.select(
                        "instrument_id", "moex_secid", "contract_id"
                    ).distinct(),
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
            "daily_open_interest_path": str(daily_open_interest_path)
            if daily_open_interest_path
            else None,
            "daily_open_interest_version": daily_open_interest_version,
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
