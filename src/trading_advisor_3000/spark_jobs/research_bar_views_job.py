from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    delta_table_columns,
    has_delta_log,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ResearchDatasetManifest,
    materialize_research_dataset,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets.bar_usage import (
    BAR_USAGE_POLICY_ID,
    BAR_USAGE_PROFILE_FLAGS,
    SPECIAL_SESSION_CLASSES,
)

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session

RESEARCH_L0_CONTOURS = ("native_tradable", "pit_active_front")
DAILY_TIMEFRAMES = ("1d", "d", "daily")
WEEKLY_TIMEFRAMES = ("1w", "1wk", "w", "weekly")


@dataclass(frozen=True)
class ResearchBarViewsSparkJobSpec:
    app_name: str = "ta3000-research-bar-views-l0"
    delta_reader: str = "spark"
    delta_writer: str = "spark"


@dataclass(frozen=True)
class BarUsageContextFrames:
    intervals: object
    day_meta: object
    expected_weekly: object
    actual_daily: object

    def unpersist(self) -> None:
        for frame in (self.intervals, self.day_meta, self.expected_weekly, self.actual_daily):
            unpersist = getattr(frame, "unpersist", None)
            if callable(unpersist):
                unpersist()


def build_research_l0_sql_plan() -> str:
    return """
    native_tradable:
      canonical_bars + canonical_bar_provenance + canonical_session_intervals
      + canonical_session_calendar + canonical_roll_map
      -> research_bar_views(contour_id='native_tradable', series_mode='contract')
    pit_active_front:
      continuous_front_bars + canonical_bar_provenance + canonical_session_intervals
      + canonical_session_calendar
      -> research_bar_views(contour_id='pit_active_front', series_mode='continuous_front')
    write:
      Delta scoped replace of research_bar_views and research_instrument_tree
      keyed by dataset_version + contour_id + series_mode + series_id + timeframe + ts
    """.strip()


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _latest_delta_version(table_path: Path) -> int | None:
    log_dir = table_path / "_delta_log"
    if not log_dir.exists():
        return None
    versions: list[int] = []
    for item in log_dir.glob("*.json"):
        try:
            versions.append(int(item.stem))
        except ValueError:
            continue
    return max(versions) if versions else None


def _delta_log_hash(table_path: Path) -> str:
    log_dir = table_path / "_delta_log"
    digest = hashlib.sha256()
    if not log_dir.exists():
        return ""
    for item in sorted(log_dir.glob("*.json")):
        digest.update(item.name.encode("utf-8"))
        digest.update(str(item.stat().st_size).encode("utf-8"))
    return digest.hexdigest()[:16].upper()


def _delta_table_partition_columns(spark: object, table_path: Path) -> tuple[str, ...] | None:
    if not has_delta_log(table_path):
        return None
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    rows = DeltaTable.forPath(spark, str(table_path)).detail().select("partitionColumns").collect()
    if not rows:
        return ()
    return tuple(str(item) for item in (rows[0]["partitionColumns"] or []))


def _delta_table_layout_matches_contract(
    *, spark: object, table_path: Path, table_name: str
) -> bool:
    actual = _delta_table_partition_columns(spark, table_path)
    if actual is None:
        return False
    contract = research_dataset_store_contract()[table_name]
    expected = tuple(str(item) for item in list(contract.get("partition_by") or []))
    return actual == expected


def _combined_source_hash(source_hashes: dict[str, str], keys: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    for key in keys:
        digest.update(key.encode("utf-8"))
        digest.update(b":")
        digest.update(source_hashes.get(key, "").encode("utf-8"))
        digest.update(b"|")
    return digest.hexdigest()[:16].upper()


def _schema_from_contract(table_name: str) -> str:
    contract = research_dataset_store_contract()[table_name]["columns"]
    spark_types = {
        "string": "STRING",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "int": "INT",
        "bigint": "BIGINT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "json": "STRING",
    }
    return ",".join(f"{name} {spark_types[kind]}" for name, kind in contract.items())


def _empty_dataframe(spark: object, table_name: str):
    return spark.createDataFrame([], _schema_from_contract(table_name))


def _cast_to_contract(dataframe, table_name: str):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    columns = research_dataset_store_contract()[table_name]["columns"]
    casted = dataframe
    for column, kind in columns.items():
        if column not in casted.columns:
            casted = casted.withColumn(column, F.lit(None))
        if kind == "json":
            casted = casted.withColumn(column, F.to_json(F.col(column)))
        elif kind == "timestamp":
            casted = casted.withColumn(column, F.col(column).cast("timestamp"))
        elif kind == "date":
            casted = casted.withColumn(column, F.col(column).cast("date"))
        elif kind == "bigint":
            casted = casted.withColumn(column, F.col(column).cast("long"))
        else:
            casted = casted.withColumn(column, F.col(column).cast(kind))
    return casted.select(*columns.keys())


def _apply_filters(dataframe, *, instrument_ids: tuple[str, ...], timeframes: tuple[str, ...]):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    filtered = dataframe
    if instrument_ids:
        filtered = filtered.where(F.col("instrument_id").isin([*instrument_ids]))
    if timeframes:
        filtered = filtered.where(F.col("timeframe").isin([*timeframes]))
    return filtered


def _optional_casted_column(dataframe, names: tuple[str, ...], spark_type: str):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    for name in names:
        if name in dataframe.columns:
            return F.col(name).cast(spark_type)
    return F.lit(None).cast(spark_type)


def _optional_bool_column(dataframe, names: tuple[str, ...]):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    for name in names:
        if name in dataframe.columns:
            return F.coalesce(F.col(name).cast("boolean"), F.lit(False))
    return F.lit(False)


def _require_columns(dataframe, *, table_name: str, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in dataframe.columns]
    if missing:
        raise RuntimeError(f"missing required {table_name} columns: {', '.join(sorted(missing))}")


def _canonical_provenance_frame(spark: object, table_path: Path):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    provenance = spark.read.format("delta").load(str(table_path))
    _require_columns(
        provenance,
        table_name="canonical_bar_provenance",
        columns=(
            "contract_id",
            "timeframe",
            "ts",
            "bar_start_ts",
            "bar_end_ts",
            "session_interval_id",
        ),
    )
    return provenance.select(
        F.col("contract_id").alias("prov_contract_id"),
        F.col("timeframe").alias("prov_timeframe"),
        F.col("ts").alias("prov_ts"),
        F.col("bar_start_ts").cast("timestamp").alias("bar_start_ts"),
        F.col("bar_end_ts").cast("timestamp").alias("bar_end_ts"),
        F.col("session_interval_id").cast("string").alias("provenance_session_interval_id"),
        _optional_bool_column(
            provenance, ("is_boundary_bar", "boundary_bar", "boundary_marker")
        ).alias("provenance_boundary_bar"),
        _optional_bool_column(
            provenance, ("is_shortened_bar", "shortened_bar", "shortened_marker")
        ).alias("provenance_shortened_bar"),
    )


def _session_calendar_frame(
    spark: object,
    table_path: Path,
    *,
    instrument_ids: tuple[str, ...] = (),
    timeframes: tuple[str, ...] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    calendar = spark.read.format("delta").load(str(table_path))
    if instrument_ids:
        calendar = calendar.where(F.col("instrument_id").isin([*instrument_ids]))
    if timeframes:
        calendar = calendar.where(F.col("timeframe").isin([*timeframes]))
    if start_ts:
        calendar = calendar.where(
            F.col("session_date") >= F.date_sub(F.to_date(F.lit(start_ts)), 7)
        )
    if end_ts:
        calendar = calendar.where(F.col("session_date") <= F.date_add(F.to_date(F.lit(end_ts)), 7))
    _require_columns(
        calendar,
        table_name="canonical_session_calendar",
        columns=(
            "instrument_id",
            "timeframe",
            "session_date",
            "session_open_ts",
            "session_close_ts",
            "session_class",
        ),
    )
    return calendar.select(
        F.col("instrument_id").alias("cal_instrument_id"),
        F.col("timeframe").alias("cal_timeframe"),
        F.col("session_date").alias("cal_session_date"),
        F.col("session_open_ts"),
        F.col("session_close_ts"),
        F.col("session_class").cast("string").alias("calendar_session_class"),
    )


def _session_intervals_frame(spark: object, table_path: Path):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    intervals = spark.read.format("delta").load(str(table_path))
    interval_type = _optional_casted_column(intervals, ("interval_type",), "string")
    return intervals.select(
        F.col("instrument_id").alias("interval_instrument_id"),
        F.col("session_date").alias("interval_session_date"),
        _optional_casted_column(intervals, ("interval_id", "session_interval_id"), "string").alias(
            "interval_id"
        ),
        _optional_casted_column(intervals, ("interval_seq",), "int").alias("interval_seq"),
        F.col("expected_open_ts").alias("interval_open_ts"),
        F.col("expected_close_ts").alias("interval_close_ts"),
        _optional_casted_column(intervals, ("session_class",), "string").alias(
            "interval_session_class"
        ),
        interval_type.alias("interval_type"),
        (
            _optional_bool_column(intervals, ("is_shortened_bar", "shortened_bar"))
            | F.lower(F.coalesce(interval_type, F.lit(""))).contains("shortened")
        ).alias("interval_shortened_bar"),
    )


def _bar_usage_flags_expression(profile_column):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    entries = []
    for profile, flags in BAR_USAGE_PROFILE_FLAGS.items():
        entries.extend((F.lit(profile), F.lit(flags)))
    return F.create_map(*entries)[profile_column]


def _bar_usage_context_frames(
    *,
    spark: object,
    canonical_bars_path: Path,
    canonical_session_intervals_path: Path,
    instrument_ids: tuple[str, ...] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
    warmup_bars: int = 0,
) -> BarUsageContextFrames:
    from pyspark import StorageLevel  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    scoped_start_ts = start_ts if warmup_bars <= 0 else None
    intervals = _session_intervals_frame(spark, canonical_session_intervals_path)
    if instrument_ids:
        intervals = intervals.where(F.col("interval_instrument_id").isin([*instrument_ids]))
    if scoped_start_ts:
        intervals = intervals.where(
            F.col("interval_session_date") >= F.date_sub(F.to_date(F.lit(scoped_start_ts)), 7)
        )
    if end_ts:
        intervals = intervals.where(
            F.col("interval_session_date") <= F.date_add(F.to_date(F.lit(end_ts)), 7)
        )
    intervals = intervals.persist(StorageLevel.MEMORY_AND_DISK)
    day_meta = (
        intervals.groupBy("interval_instrument_id", "interval_session_date")
        .agg(
            F.max(
                F.when(
                    F.col("interval_session_class") == F.lit("partial_or_gap"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("day_partial"),
            F.max(
                F.when(
                    F.col("interval_session_class").isin([*SPECIAL_SESSION_CLASSES]),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("day_special"),
            F.max(
                F.when(
                    F.col("interval_session_class").isin([*SPECIAL_SESSION_CLASSES]),
                    F.col("interval_session_class"),
                )
            ).alias("day_special_class"),
            F.max(F.col("interval_shortened_bar").cast("int")).alias("day_shortened"),
        )
        .withColumn(
            "day_session_class",
            F.when(F.col("day_partial") > F.lit(0), F.lit("partial_or_gap"))
            .when(
                F.col("day_special") > F.lit(0),
                F.coalesce(F.col("day_special_class"), F.lit("short")),
            )
            .otherwise(F.lit("regular")),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    expected_weekly = (
        day_meta.withColumn(
            "week_start",
            F.to_date(
                F.date_trunc("week", F.to_timestamp(F.col("interval_session_date").cast("string")))
            ),
        )
        .groupBy("interval_instrument_id", "week_start")
        .agg(
            F.count(F.lit(1)).alias("expected_traded_sessions"),
            F.sum("day_partial").alias("weekly_partial_count"),
            F.sum("day_special").alias("weekly_special_count"),
            F.sum("day_shortened").alias("weekly_shortened_count"),
        )
        .withColumnRenamed("interval_instrument_id", "weekly_expected_instrument_id")
        .withColumnRenamed("week_start", "weekly_expected_week_start")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    day_meta = day_meta.withColumnRenamed(
        "interval_instrument_id", "day_instrument_id"
    ).withColumnRenamed("interval_session_date", "day_session_date")
    actual_daily = (
        spark.read.format("delta")
        .load(str(canonical_bars_path))
        .where(F.lower(F.col("timeframe")).isin([*DAILY_TIMEFRAMES]))
    )
    if instrument_ids:
        actual_daily = actual_daily.where(F.col("instrument_id").isin([*instrument_ids]))
    if scoped_start_ts:
        actual_daily = actual_daily.where(
            F.to_date("ts") >= F.date_sub(F.to_date(F.lit(scoped_start_ts)), 7)
        )
    if end_ts:
        actual_daily = actual_daily.where(
            F.to_date("ts") <= F.date_add(F.to_date(F.lit(end_ts)), 7)
        )
    actual_daily = (
        actual_daily.select(
            F.col("contract_id").alias("daily_contract_id"),
            F.col("instrument_id").alias("daily_instrument_id"),
            F.to_date(F.date_trunc("week", F.col("ts"))).alias("week_start"),
            F.to_date("ts").alias("daily_session_date"),
        )
        .distinct()
        .groupBy("daily_contract_id", "daily_instrument_id", "week_start")
        .agg(F.count(F.lit(1)).alias("actual_daily_sessions"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    return BarUsageContextFrames(
        intervals=intervals,
        day_meta=day_meta,
        expected_weekly=expected_weekly,
        actual_daily=actual_daily,
    )


def _with_bar_usage_contract(
    *,
    spark: object,
    dataframe,
    canonical_bars_path: Path,
    canonical_session_intervals_path: Path,
    usage_context: BarUsageContextFrames | None = None,
):
    from pyspark import StorageLevel  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    context = usage_context or _bar_usage_context_frames(
        spark=spark,
        canonical_bars_path=canonical_bars_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
    )
    intervals = context.intervals
    day_meta = context.day_meta
    expected_weekly = context.expected_weekly
    actual_daily = context.actual_daily

    bar = (
        dataframe.withColumn("_timeframe_lc", F.lower(F.col("timeframe")))
        .withColumn("_is_daily", F.col("_timeframe_lc").isin([*DAILY_TIMEFRAMES]))
        .withColumn("_is_weekly", F.col("_timeframe_lc").isin([*WEEKLY_TIMEFRAMES]))
        .withColumn(
            "_is_intraday",
            ~(F.col("_is_daily") | F.col("_is_weekly")),
        )
        .withColumn("_bar_session_interval_id", F.col("session_interval_id"))
        .withColumn(
            "_week_start",
            F.to_date(F.date_trunc("week", F.to_timestamp(F.col("ts")))),
        )
    )
    joined = (
        bar.alias("bar")
        .join(
            F.broadcast(intervals).alias("intervals"),
            (F.col("bar._is_intraday"))
            & (F.col("bar.instrument_id") == F.col("intervals.interval_instrument_id"))
            & (F.col("bar.session_date") == F.col("intervals.interval_session_date"))
            & (F.col("bar._bar_session_interval_id") == F.col("intervals.interval_id")),
            "left",
        )
        .join(
            F.broadcast(day_meta).alias("day"),
            (F.col("bar.instrument_id") == F.col("day.day_instrument_id"))
            & (F.col("bar.session_date") == F.col("day.day_session_date")),
            "left",
        )
        .join(
            F.broadcast(expected_weekly).alias("weekly_expected"),
            (F.col("bar.instrument_id") == F.col("weekly_expected.weekly_expected_instrument_id"))
            & (F.col("bar._week_start") == F.col("weekly_expected.weekly_expected_week_start")),
            "left",
        )
        .join(
            F.broadcast(actual_daily).alias("weekly_actual"),
            (F.col("bar.contract_id") == F.col("weekly_actual.daily_contract_id"))
            & (F.col("bar.instrument_id") == F.col("weekly_actual.daily_instrument_id"))
            & (F.col("bar._week_start") == F.col("weekly_actual.week_start")),
            "left",
        )
    )
    actual_daily_sessions = F.coalesce(F.col("actual_daily_sessions"), F.lit(0))
    weekly_incomplete = (actual_daily_sessions < F.col("expected_traded_sessions")) | (
        F.col("weekly_partial_count") > F.lit(0)
    )
    weekly_special = F.col("weekly_special_count") > F.lit(0)
    weekly_shortened = F.col("weekly_shortened_count") > F.lit(0)
    weekly_session_class = (
        F.when(F.col("expected_traded_sessions").isNull(), F.lit(None).cast("string"))
        .when(weekly_incomplete, F.lit("partial_or_gap"))
        .when(weekly_special, F.lit("short"))
        .otherwise(F.lit("regular"))
    )
    joined = joined.withColumn(
        "session_interval_id",
        F.when(
            F.col("_is_intraday"),
            F.col("_bar_session_interval_id"),
        ).otherwise(F.lit(None).cast("string")),
    ).withColumn(
        "session_class",
        F.when(F.col("_is_weekly"), weekly_session_class)
        .when(
            F.col("_is_daily"),
            F.col("calendar_session_class"),
        )
        .otherwise(F.col("interval_session_class")),
    )

    boundary_bar = F.coalesce(F.col("provenance_boundary_bar"), F.lit(False)) | (
        F.col("_is_intraday")
        & (
            (F.col("bar_start_ts") == F.col("interval_open_ts"))
            | (F.col("bar_end_ts") == F.col("interval_close_ts"))
            | (
                F.unix_timestamp(F.col("bar_end_ts")) + F.lit(1)
                == F.unix_timestamp(F.col("interval_open_ts"))
            )
        )
    )
    shortened_bar = (
        F.coalesce(F.col("provenance_shortened_bar"), F.lit(False))
        | F.coalesce(F.col("interval_shortened_bar"), F.lit(False))
        | (F.coalesce(weekly_shortened, F.lit(False)) & F.col("_is_weekly"))
    )
    incomplete_bar = (F.col("session_class") == F.lit("partial_or_gap")) | (
        F.coalesce(weekly_incomplete, F.lit(False)) & F.col("_is_weekly")
    )
    special_bar = F.col("session_class").isin([*SPECIAL_SESSION_CLASSES]) | (
        F.coalesce(weekly_special, F.lit(False)) & F.col("_is_weekly")
    )
    with_profile = joined.withColumn(
        "bar_usage_profile",
        F.when(shortened_bar, F.lit("shortened_risk"))
        .when(boundary_bar, F.lit("boundary_risk"))
        .when(incomplete_bar, F.lit("incomplete"))
        .when(special_bar, F.lit("risk_only"))
        .otherwise(F.lit("regular_trading")),
    )
    with_flags = with_profile.withColumn(
        "bar_usage_flags", _bar_usage_flags_expression(F.col("bar_usage_profile"))
    ).withColumn("bar_usage_policy_id", F.lit(BAR_USAGE_POLICY_ID))
    cached = with_flags.persist(StorageLevel.MEMORY_AND_DISK)
    validation_row = cached.agg(
        F.max(
            F.when(
                F.col("bar_start_ts").isNull() | F.col("bar_end_ts").isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("missing_boundary_metadata"),
        F.max(
            F.when(
                F.col("_is_intraday")
                & (F.col("session_interval_id").isNull() | F.col("session_class").isNull()),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("missing_intraday_metadata"),
        F.max(
            F.when(
                (F.col("_is_daily") | F.col("_is_weekly")) & F.col("session_class").isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("missing_daily_weekly_metadata"),
        F.max(F.when(F.col("bar_usage_flags").isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            "missing_usage_flags"
        ),
    ).collect()[0]
    validation = validation_row.asDict()
    if validation.get("missing_boundary_metadata"):
        cached.unpersist()
        raise RuntimeError("missing required canonical provenance bar boundary metadata")
    if validation.get("missing_intraday_metadata"):
        cached.unpersist()
        raise RuntimeError("missing required canonical session interval metadata for intraday bars")
    if validation.get("missing_daily_weekly_metadata"):
        cached.unpersist()
        raise RuntimeError(
            "missing required canonical session class metadata for daily/weekly bars"
        )
    if validation.get("missing_usage_flags"):
        cached.unpersist()
        raise RuntimeError("bar usage profile/flags registry validation failed")
    return cached


def _with_l0_metrics(
    dataframe, *, series_columns: tuple[str, ...], start_ts: str | None, warmup_bars: int
):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F

    series_order = Window.partitionBy(*series_columns).orderBy("ts")
    prev_close = F.lag("close").over(series_order)
    base = (
        dataframe.withColumn("prev_close", prev_close)
        .withColumn(
            "ret_1",
            F.when(prev_close.isNull() | (prev_close == 0), F.lit(None)).otherwise(
                F.col("close") / prev_close - F.lit(1.0)
            ),
        )
        .withColumn(
            "log_ret_1",
            F.when(prev_close.isNull() | (prev_close == 0), F.lit(None)).otherwise(
                F.log(F.col("close") / prev_close)
            ),
        )
        .withColumn(
            "true_range",
            F.greatest(
                F.col("high") - F.col("low"),
                F.when(prev_close.isNull(), F.col("high") - F.col("low")).otherwise(
                    F.abs(F.col("high") - prev_close)
                ),
                F.when(prev_close.isNull(), F.col("high") - F.col("low")).otherwise(
                    F.abs(F.col("low") - prev_close)
                ),
            ),
        )
        .withColumn("hl_range", F.col("high") - F.col("low"))
        .withColumn("oc_range", F.abs(F.col("close") - F.col("open")))
        .withColumn("bar_index", F.row_number().over(series_order) - F.lit(1))
    )
    if start_ts:
        analysis_rank = F.min(F.when(F.col("ts") >= F.lit(start_ts), F.col("bar_index"))).over(
            Window.partitionBy(*series_columns)
        )
        base = base.withColumn("analysis_start_index", analysis_rank).where(
            F.col("analysis_start_index").isNotNull()
            & (
                F.col("bar_index")
                >= F.greatest(F.lit(0), F.col("analysis_start_index") - F.lit(warmup_bars))
            )
        )
        return base.withColumn(
            "slice_role",
            F.when(F.col("bar_index") < F.col("analysis_start_index"), F.lit("warmup")).otherwise(
                F.lit("analysis")
            ),
        )
    return base.withColumn("slice_role", F.lit("analysis"))


def _native_bar_views(
    *,
    spark: object,
    canonical_bars_path: Path,
    canonical_bar_provenance_path: Path,
    canonical_session_intervals_path: Path,
    canonical_session_calendar_path: Path,
    canonical_roll_map_path: Path,
    dataset_version: str,
    instrument_ids: tuple[str, ...],
    contract_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
    warmup_bars: int,
    usage_context: BarUsageContextFrames | None = None,
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    bars = spark.read.format("delta").load(str(canonical_bars_path))
    bars = _apply_filters(bars, instrument_ids=instrument_ids, timeframes=timeframes)
    if contract_ids:
        bars = bars.where(F.col("contract_id").isin([*contract_ids]))
    if end_ts:
        bars = bars.where(F.col("ts") <= F.lit(end_ts))

    provenance = _canonical_provenance_frame(spark, canonical_bar_provenance_path)
    scoped_start_ts = start_ts if warmup_bars <= 0 else None
    calendar = _session_calendar_frame(
        spark,
        canonical_session_calendar_path,
        instrument_ids=instrument_ids,
        timeframes=timeframes,
        start_ts=scoped_start_ts,
        end_ts=end_ts,
    )
    roll_map = (
        spark.read.format("delta")
        .load(str(canonical_roll_map_path))
        .select(
            F.col("instrument_id").alias("roll_instrument_id"),
            F.col("session_date").alias("roll_session_date"),
            F.col("active_contract_id").alias("roll_active_contract_id"),
        )
    )
    if instrument_ids:
        roll_map = roll_map.where(F.col("roll_instrument_id").isin([*instrument_ids]))
    if scoped_start_ts:
        roll_map = roll_map.where(
            F.col("roll_session_date") >= F.date_sub(F.to_date(F.lit(scoped_start_ts)), 7)
        )
    if end_ts:
        roll_map = roll_map.where(
            F.col("roll_session_date") <= F.date_add(F.to_date(F.lit(end_ts)), 7)
        )
    roll_map = roll_map.groupBy("roll_instrument_id", "roll_session_date").agg(
        F.countDistinct("roll_active_contract_id").alias("roll_active_contract_count"),
        F.first("roll_active_contract_id", ignorenulls=True).alias("roll_active_contract_id"),
    )
    with_session = (
        bars.join(
            provenance,
            (F.col("contract_id") == F.col("prov_contract_id"))
            & (F.col("timeframe") == F.col("prov_timeframe"))
            & (F.col("ts") == F.col("prov_ts")),
            "left",
        )
        .withColumn("session_date", F.to_date(F.coalesce(F.col("bar_start_ts"), F.col("ts"))))
        .join(
            F.broadcast(calendar),
            (F.col("instrument_id") == F.col("cal_instrument_id"))
            & (F.col("timeframe") == F.col("cal_timeframe"))
            & (F.col("session_date") == F.col("cal_session_date")),
            "left",
        )
        .join(
            F.broadcast(roll_map),
            (F.col("instrument_id") == F.col("roll_instrument_id"))
            & (F.col("session_date") == F.col("roll_session_date")),
            "left",
        )
    )
    base = with_session.select(
        F.lit(dataset_version).alias("dataset_version"),
        F.lit("native_tradable").alias("contour_id"),
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "open",
        "high",
        "low",
        "close",
        F.col("volume").cast("long").alias("volume"),
        F.col("open_interest").cast("long").alias("open_interest"),
        "session_date",
        "session_open_ts",
        "session_close_ts",
        "bar_start_ts",
        "bar_end_ts",
        F.col("provenance_session_interval_id").alias("session_interval_id"),
        F.lit(None).cast("string").alias("session_class"),
        "calendar_session_class",
        "provenance_boundary_bar",
        "provenance_shortened_bar",
        F.coalesce(F.col("roll_active_contract_id"), F.col("contract_id")).alias(
            "active_contract_id"
        ),
        F.coalesce(F.col("roll_active_contract_count"), F.lit(0)).alias(
            "roll_active_contract_count"
        ),
        F.col("contract_id").alias("series_id"),
        F.lit("contract").alias("series_mode"),
        F.lit(0).alias("roll_epoch"),
        F.lit(None).cast("string").alias("roll_event_id"),
        F.lit(False).alias("is_roll_bar"),
        F.lit(False).alias("is_first_bar_after_roll"),
        F.lit(0).alias("bars_since_roll"),
        F.lit("native").alias("price_space"),
        F.col("open").alias("native_open"),
        F.col("high").alias("native_high"),
        F.col("low").alias("native_low"),
        F.col("close").alias("native_close"),
        F.col("open").alias("continuous_open"),
        F.col("high").alias("continuous_high"),
        F.col("low").alias("continuous_low"),
        F.col("close").alias("continuous_close"),
        F.col("open").alias("execution_open"),
        F.col("high").alias("execution_high"),
        F.col("low").alias("execution_low"),
        F.col("close").alias("execution_close"),
        F.lit(None).cast("string").alias("previous_contract_id"),
        F.lit(None).cast("string").alias("candidate_contract_id"),
        F.lit("").alias("adjustment_mode"),
        F.lit(0.0).alias("cumulative_additive_offset"),
        F.lit(None).cast("double").alias("ratio_factor"),
    )
    base = _with_l0_metrics(
        base,
        series_columns=("contract_id", "instrument_id", "timeframe"),
        start_ts=start_ts,
        warmup_bars=warmup_bars,
    )
    if base.where(F.col("roll_active_contract_count") > F.lit(1)).limit(1).count():
        raise RuntimeError("conflicting canonical roll map active contracts for research bar views")
    base = base.drop("roll_active_contract_count")
    return _with_bar_usage_contract(
        spark=spark,
        dataframe=base,
        canonical_bars_path=canonical_bars_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
        usage_context=usage_context,
    )


def _pit_active_front_bar_views(
    *,
    spark: object,
    continuous_front_bars_path: Path,
    canonical_bar_provenance_path: Path,
    canonical_session_intervals_path: Path,
    canonical_session_calendar_path: Path,
    canonical_bars_path: Path,
    dataset_version: str,
    instrument_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
    warmup_bars: int,
    usage_context: BarUsageContextFrames | None = None,
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    bars = spark.read.format("delta").load(str(continuous_front_bars_path))
    bars = _apply_filters(bars, instrument_ids=instrument_ids, timeframes=timeframes)
    if end_ts:
        bars = bars.where(F.col("ts") <= F.lit(end_ts))

    provenance = _canonical_provenance_frame(spark, canonical_bar_provenance_path)
    scoped_start_ts = start_ts if warmup_bars <= 0 else None
    calendar = _session_calendar_frame(
        spark,
        canonical_session_calendar_path,
        instrument_ids=instrument_ids,
        timeframes=timeframes,
        start_ts=scoped_start_ts,
        end_ts=end_ts,
    )
    with_session = (
        bars.join(
            provenance,
            (F.col("active_contract_id") == F.col("prov_contract_id"))
            & (F.col("timeframe") == F.col("prov_timeframe"))
            & (F.col("ts") == F.col("prov_ts")),
            "left",
        )
        .withColumn("session_date", F.to_date(F.coalesce(F.col("bar_start_ts"), F.col("ts"))))
        .join(
            F.broadcast(calendar),
            (F.col("instrument_id") == F.col("cal_instrument_id"))
            & (F.col("timeframe") == F.col("cal_timeframe"))
            & (F.col("session_date") == F.col("cal_session_date")),
            "left",
        )
    )
    base = with_session.select(
        F.lit(dataset_version).alias("dataset_version"),
        F.lit("pit_active_front").alias("contour_id"),
        F.col("active_contract_id").alias("contract_id"),
        "instrument_id",
        "timeframe",
        "ts",
        F.col("continuous_open").alias("open"),
        F.col("continuous_high").alias("high"),
        F.col("continuous_low").alias("low"),
        F.col("continuous_close").alias("close"),
        F.col("native_volume").cast("long").alias("volume"),
        F.col("native_open_interest").cast("long").alias("open_interest"),
        "session_date",
        "session_open_ts",
        "session_close_ts",
        "bar_start_ts",
        "bar_end_ts",
        F.col("provenance_session_interval_id").alias("session_interval_id"),
        F.lit(None).cast("string").alias("session_class"),
        "calendar_session_class",
        "provenance_boundary_bar",
        "provenance_shortened_bar",
        "active_contract_id",
        F.col("instrument_id").alias("series_id"),
        F.lit("continuous_front").alias("series_mode"),
        "roll_epoch",
        "roll_event_id",
        "is_roll_bar",
        "is_first_bar_after_roll",
        "bars_since_roll",
        "price_space",
        "native_open",
        "native_high",
        "native_low",
        "native_close",
        "continuous_open",
        "continuous_high",
        "continuous_low",
        "continuous_close",
        F.col("native_open").alias("execution_open"),
        F.col("native_high").alias("execution_high"),
        F.col("native_low").alias("execution_low"),
        F.col("native_close").alias("execution_close"),
        "previous_contract_id",
        "candidate_contract_id",
        "adjustment_mode",
        "cumulative_additive_offset",
        "ratio_factor",
    )
    base = _with_l0_metrics(
        base,
        series_columns=("series_id", "instrument_id", "timeframe"),
        start_ts=start_ts,
        warmup_bars=warmup_bars,
    )
    return _with_bar_usage_contract(
        spark=spark,
        dataframe=base,
        canonical_bars_path=canonical_bars_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
        usage_context=usage_context,
    )


def _instrument_tree_from_bar_views(bar_views, *, universe_id: str):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    normalized_instrument = F.upper(F.trim(F.col("instrument_id")))
    bare_instrument = F.when(
        F.substring(normalized_instrument, 1, 4) == F.lit("FUT_"),
        F.substring(normalized_instrument, 5, 1024),
    ).otherwise(normalized_instrument)
    asset_group = (
        F.when(
            bare_instrument.isin("BR", "NG", "GOLD", "SILV", "PLD", "PLT", "WHEAT"),
            F.lit("commodity"),
        )
        .when(bare_instrument.isin("RTS", "MIX", "MXI", "NASD", "SPYF", "RGBI"), F.lit("index"))
        .otherwise(F.lit("unknown"))
    )
    internal_id = F.when(
        F.substring(normalized_instrument, 1, 4) == F.lit("FUT_"),
        normalized_instrument,
    ).otherwise(F.concat(F.lit("FUT_"), bare_instrument))
    row_hash = F.concat_ws(
        "|",
        F.col("contour_id"),
        F.col("series_id"),
        F.col("contract_id"),
        F.col("timeframe"),
        F.col("ts").cast("string"),
        F.col("close").cast("string"),
    )
    aggregated = (
        bar_views.withColumn("row_hash_input", row_hash)
        .withColumn("normalized_internal_id", internal_id)
        .withColumn("normalized_asset_group", asset_group)
        .groupBy("dataset_version", "contour_id", "instrument_id")
        .agg(
            F.lit("futures").alias("asset_class"),
            F.first("normalized_asset_group").alias("asset_group"),
            F.first("normalized_internal_id").alias("internal_id"),
            F.first("instrument_id").alias("source_instrument_id"),
            F.sort_array(F.collect_set("contract_id")).alias("contract_ids_json"),
            F.sort_array(F.collect_set("active_contract_id")).alias("active_contract_ids_json"),
            F.sort_array(F.collect_set("timeframe")).alias("timeframes_json"),
            F.count(F.lit(1)).cast("long").alias("row_count"),
            F.min("ts").alias("first_ts"),
            F.max("ts").alias("last_ts"),
            F.sha2(F.concat_ws("|", F.sort_array(F.collect_list("row_hash_input"))), 256).alias(
                "source_bars_hash"
            ),
            F.lit(_utc_now_iso()).alias("created_at"),
        )
    )
    return aggregated.withColumn(
        "lineage_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("dataset_version"),
                F.col("contour_id"),
                F.col("internal_id"),
                F.concat_ws(",", F.col("contract_ids_json")),
                F.concat_ws(",", F.col("timeframes_json")),
            ),
            256,
        ),
    ).withColumn("universe_id", F.lit(universe_id))


def _spark_sql_literal(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _scoped_delete_condition(
    scope: list[tuple[str, str, object]], *, include_legacy_null_contour: bool = False
) -> str:
    def condition_from_scope(items: list[tuple[str, str, object]]) -> str:
        clauses: list[str] = []
        for column, operator, value in items:
            normalized_operator = str(operator).strip().lower()
            if value is None:
                continue
            if normalized_operator in {"in", "not in"}:
                scoped_values = (
                    tuple(item for item in value if item is not None)
                    if isinstance(value, (list, tuple, set))
                    else ()
                )
                if not scoped_values:
                    continue
                literals = ", ".join(_spark_sql_literal(item) for item in scoped_values)
                sql_operator = "IN" if normalized_operator == "in" else "NOT IN"
                clauses.append(f"{column} {sql_operator} ({literals})")
                continue
            if normalized_operator not in {"=", ">=", "<=", ">", "<"}:
                raise ValueError(f"unsupported scoped Delta delete operator: {operator}")
            clauses.append(f"{column} {normalized_operator} {_spark_sql_literal(value)}")
        if not clauses:
            raise ValueError("scoped Delta delete requires at least one scope value")
        return " AND ".join(clauses)

    base_condition = condition_from_scope(scope)
    if not include_legacy_null_contour:
        return base_condition

    has_dataset_scope = any(
        column == "dataset_version" and value is not None for column, _, value in scope
    )
    has_contour_scope = any(
        column == "contour_id" and value is not None for column, _, value in scope
    )
    if not has_dataset_scope or not has_contour_scope:
        return base_condition

    legacy_scope = [item for item in scope if item[0] != "contour_id"]
    legacy_condition = condition_from_scope(legacy_scope)
    return f"({base_condition}) OR ({legacy_condition} AND contour_id IS NULL)"


def _research_l0_replace_filters(
    *,
    dataset_version: str,
    contours: tuple[str, ...],
    instrument_ids: tuple[str, ...],
    contract_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
) -> list[tuple[str, str, object]]:
    if contract_ids and "pit_active_front" in contours:
        raise ValueError("contract_ids filter is only supported for the native_tradable contour")
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "in", [*contours]),
    ]
    if instrument_ids:
        filters.append(("instrument_id", "in", [*instrument_ids]))
    if contract_ids:
        filters.append(("contract_id", "in", [*contract_ids]))
    if timeframes:
        filters.append(("timeframe", "in", [*timeframes]))
    if start_ts:
        filters.append(("ts", ">=", start_ts))
    if end_ts:
        filters.append(("ts", "<=", end_ts))
    return filters


def _research_instrument_tree_replace_filters(
    *,
    dataset_version: str,
    contours: tuple[str, ...],
    instrument_ids: tuple[str, ...],
) -> list[tuple[str, str, object]]:
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "in", [*contours]),
    ]
    if instrument_ids:
        filters.append(("instrument_id", "in", [*instrument_ids]))
    return filters


def _replace_spark_delta_table(*, dataframe, table_path: Path, table_name: str) -> None:
    contract = research_dataset_store_contract()[table_name]
    partition_by = list(contract.get("partition_by") or [])
    temp_path = table_path.parent / f".{table_path.name}.rewrite-{uuid4().hex}"
    backup_path = table_path.parent / f".{table_path.name}.backup-{uuid4().hex}"
    if temp_path.exists():
        shutil.rmtree(temp_path)
    writer = (
        _cast_to_contract(dataframe, table_name)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(temp_path))
    backup_created = False
    try:
        if table_path.exists():
            table_path.rename(backup_path)
            backup_created = True
        temp_path.rename(table_path)
    except Exception:
        if backup_created and backup_path.exists() and not table_path.exists():
            backup_path.rename(table_path)
        raise
    else:
        if backup_path.exists():
            shutil.rmtree(backup_path)
    finally:
        if temp_path.exists():
            shutil.rmtree(temp_path)


def _write_spark_delta_table(
    *, dataframe, table_path: Path, table_name: str, replace_scope: list[tuple[str, str, object]]
) -> None:
    contract = research_dataset_store_contract()[table_name]
    table_path.parent.mkdir(parents=True, exist_ok=True)
    casted = _cast_to_contract(dataframe, table_name)
    partition_by = list(contract.get("partition_by") or [])
    if not has_delta_log(table_path):
        writer = casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(str(table_path))
        return

    _empty_dataframe(dataframe.sparkSession, table_name).write.format("delta").mode(
        "append"
    ).option("mergeSchema", "true").save(str(table_path))

    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    delete_condition = _scoped_delete_condition(
        replace_scope,
        include_legacy_null_contour=table_name
        in {"research_bar_views", "research_instrument_tree"},
    )
    if not _delta_table_layout_matches_contract(
        spark=dataframe.sparkSession,
        table_path=table_path,
        table_name=table_name,
    ):
        existing = dataframe.sparkSession.read.format("delta").load(str(table_path))
        retained = _cast_to_contract(existing.where(f"NOT ({delete_condition})"), table_name)
        rewritten = retained.unionByName(casted, allowMissingColumns=True)
        _replace_spark_delta_table(
            dataframe=rewritten,
            table_path=table_path,
            table_name=table_name,
        )
        return

    delta_table = DeltaTable.forPath(dataframe.sparkSession, str(table_path))
    delta_table.delete(delete_condition)

    key_columns_by_table = {
        "research_bar_views": (
            "dataset_version",
            "contour_id",
            "series_mode",
            "series_id",
            "timeframe",
            "ts",
        ),
        "research_instrument_tree": (
            "dataset_version",
            "contour_id",
            "instrument_id",
            "internal_id",
        ),
    }
    key_columns = key_columns_by_table[table_name]
    merge_condition = " AND ".join(f"target.{column} = source.{column}" for column in key_columns)
    (
        delta_table.alias("target")
        .merge(casted.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _validate_tables(output_paths: dict[str, str]) -> list[str]:
    errors: list[str] = []
    contract = research_dataset_store_contract()
    for table_name, table_path_text in output_paths.items():
        if table_name == "research_datasets":
            continue
        table_path = Path(table_path_text)
        if not has_delta_log(table_path):
            errors.append(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
            continue
        expected_columns = list(dict(contract[table_name]["columns"]).keys())
        actual_columns = list(delta_table_columns(table_path))
        missing = [column for column in expected_columns if column not in actual_columns]
        extra = [column for column in actual_columns if column not in expected_columns]
        if missing or extra:
            errors.append(
                f"schema mismatch for `{table_name}` "
                f"(missing={','.join(missing) or '-'}; extra={','.join(extra) or '-'})"
            )
    return errors


def run_research_bar_views_spark_job(
    *,
    canonical_bars_path: Path,
    canonical_bar_provenance_path: Path,
    canonical_session_intervals_path: Path,
    canonical_session_calendar_path: Path,
    canonical_roll_map_path: Path,
    continuous_front_bars_path: Path,
    output_dir: Path,
    dataset_version: str,
    dataset_name: str = "research-materialized",
    universe_id: str = "moex-futures",
    run_id: str = "research_l0",
    instrument_ids: tuple[str, ...] = (),
    contract_ids: tuple[str, ...] = (),
    timeframes: tuple[str, ...] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
    warmup_bars: int = 0,
    contours: tuple[str, ...] = RESEARCH_L0_CONTOURS,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    if not contours:
        raise ValueError("research L0 contours cannot be empty")
    invalid = sorted(set(contours) - set(RESEARCH_L0_CONTOURS))
    if invalid:
        raise ValueError(f"unsupported research L0 contours: {', '.join(invalid)}")
    for table_path in (
        canonical_bars_path,
        canonical_bar_provenance_path,
        canonical_session_intervals_path,
        canonical_session_calendar_path,
        canonical_roll_map_path,
    ):
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing canonical delta table: {table_path.as_posix()}")
    if "pit_active_front" in contours and not has_delta_log(continuous_front_bars_path):
        raise RuntimeError(
            f"missing continuous-front delta table: {continuous_front_bars_path.as_posix()}"
        )

    spec = ResearchBarViewsSparkJobSpec()
    spark_factory = spark_session_factory or _create_spark_session
    spark = spark_factory(spec.app_name, spark_master)
    try:
        usage_context = _bar_usage_context_frames(
            spark=spark,
            canonical_bars_path=canonical_bars_path,
            canonical_session_intervals_path=canonical_session_intervals_path,
            instrument_ids=instrument_ids,
            start_ts=start_ts,
            end_ts=end_ts,
            warmup_bars=warmup_bars,
        )
        contour_frames = []
        if "native_tradable" in contours:
            contour_frames.append(
                _native_bar_views(
                    spark=spark,
                    canonical_bars_path=canonical_bars_path,
                    canonical_bar_provenance_path=canonical_bar_provenance_path,
                    canonical_session_intervals_path=canonical_session_intervals_path,
                    canonical_session_calendar_path=canonical_session_calendar_path,
                    canonical_roll_map_path=canonical_roll_map_path,
                    dataset_version=dataset_version,
                    instrument_ids=instrument_ids,
                    contract_ids=contract_ids,
                    timeframes=timeframes,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    warmup_bars=warmup_bars,
                    usage_context=usage_context,
                )
            )
        if "pit_active_front" in contours:
            contour_frames.append(
                _pit_active_front_bar_views(
                    spark=spark,
                    continuous_front_bars_path=continuous_front_bars_path,
                    canonical_bar_provenance_path=canonical_bar_provenance_path,
                    canonical_session_intervals_path=canonical_session_intervals_path,
                    canonical_session_calendar_path=canonical_session_calendar_path,
                    canonical_bars_path=canonical_bars_path,
                    dataset_version=dataset_version,
                    instrument_ids=instrument_ids,
                    timeframes=timeframes,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    warmup_bars=warmup_bars,
                    usage_context=usage_context,
                )
            )
        bar_views = (
            contour_frames[0] if contour_frames else _empty_dataframe(spark, "research_bar_views")
        )
        for frame in contour_frames[1:]:
            bar_views = bar_views.unionByName(frame, allowMissingColumns=True)

        output_dir.mkdir(parents=True, exist_ok=True)
        output_paths = {
            "research_bar_views": (output_dir / "research_bar_views.delta").as_posix(),
            "research_instrument_tree": (output_dir / "research_instrument_tree.delta").as_posix(),
            "research_datasets": (output_dir / "research_datasets.delta").as_posix(),
        }
        _write_spark_delta_table(
            dataframe=bar_views,
            table_path=Path(output_paths["research_bar_views"]),
            table_name="research_bar_views",
            replace_scope=_research_l0_replace_filters(
                dataset_version=dataset_version,
                contours=contours,
                instrument_ids=instrument_ids,
                contract_ids=contract_ids,
                timeframes=timeframes,
                start_ts=start_ts,
                end_ts=end_ts,
            ),
        )
        for frame in contour_frames:
            frame.unpersist()
        usage_context.unpersist()
        from pyspark.sql import functions as F  # type: ignore[import-not-found]

        tree_source = (
            spark.read.format("delta")
            .load(output_paths["research_bar_views"])
            .where(
                (F.col("dataset_version") == F.lit(dataset_version))
                & (F.col("contour_id").isin([*contours]))
            )
        )
        if instrument_ids:
            tree_source = tree_source.where(F.col("instrument_id").isin([*instrument_ids]))
        instrument_tree = _instrument_tree_from_bar_views(tree_source, universe_id=universe_id)
        _write_spark_delta_table(
            dataframe=instrument_tree,
            table_path=Path(output_paths["research_instrument_tree"]),
            table_name="research_instrument_tree",
            replace_scope=_research_instrument_tree_replace_filters(
                dataset_version=dataset_version,
                contours=contours,
                instrument_ids=instrument_ids,
            ),
        )
        contract_errors = _validate_tables(output_paths)
        if contract_errors:
            raise RuntimeError(
                "research bar views Spark contract validation failed: " + "; ".join(contract_errors)
            )

        source_delta_versions = {
            "canonical_bars": _latest_delta_version(canonical_bars_path),
            "canonical_bar_provenance": _latest_delta_version(canonical_bar_provenance_path),
            "canonical_session_intervals": _latest_delta_version(canonical_session_intervals_path),
            "canonical_session_calendar": _latest_delta_version(canonical_session_calendar_path),
            "canonical_roll_map": _latest_delta_version(canonical_roll_map_path),
            "continuous_front_bars": _latest_delta_version(continuous_front_bars_path)
            if has_delta_log(continuous_front_bars_path)
            else None,
        }
        source_delta_hashes = {
            "canonical_bars": _delta_log_hash(canonical_bars_path),
            "canonical_bar_provenance": _delta_log_hash(canonical_bar_provenance_path),
            "canonical_session_intervals": _delta_log_hash(canonical_session_intervals_path),
            "canonical_session_calendar": _delta_log_hash(canonical_session_calendar_path),
            "canonical_roll_map": _delta_log_hash(canonical_roll_map_path),
            "continuous_front_bars": _delta_log_hash(continuous_front_bars_path)
            if has_delta_log(continuous_front_bars_path)
            else "",
        }
        contour_reports: dict[str, dict[str, object]] = {}
        for contour_id in contours:
            bar_count = count_delta_table_rows(
                Path(output_paths["research_bar_views"]),
                filters=[
                    ("dataset_version", "=", dataset_version),
                    ("contour_id", "=", contour_id),
                ],
            )
            tree_count = count_delta_table_rows(
                Path(output_paths["research_instrument_tree"]),
                filters=[
                    ("dataset_version", "=", dataset_version),
                    ("contour_id", "=", contour_id),
                ],
            )
            continuous_front_policy = None
            if contour_id == "pit_active_front":
                from trading_advisor_3000.product_plane.research.datasets import (
                    ContinuousFrontPolicy,
                )

                continuous_front_policy = ContinuousFrontPolicy()
            manifest = ResearchDatasetManifest(
                dataset_version=dataset_version,
                contour_id=contour_id,  # type: ignore[arg-type]
                dataset_name=dataset_name,
                source_table="continuous_front_bars"
                if contour_id == "pit_active_front"
                else "canonical_bars",
                universe_id=universe_id,
                timeframes=timeframes or tuple(),
                base_timeframe=timeframes[0] if timeframes else None,
                start_ts=start_ts,
                end_ts=end_ts,
                series_mode="continuous_front" if contour_id == "pit_active_front" else "contract",
                split_method="full",
                warmup_bars=warmup_bars,
                source_tables=(
                    "continuous_front_bars",
                    "canonical_bar_provenance",
                    "canonical_session_intervals",
                    "canonical_session_calendar",
                )
                if contour_id == "pit_active_front"
                else (
                    "canonical_bars",
                    "canonical_bar_provenance",
                    "canonical_session_intervals",
                    "canonical_session_calendar",
                    "canonical_roll_map",
                ),
                bars_hash=_combined_source_hash(
                    source_delta_hashes,
                    (
                        "continuous_front_bars",
                        "canonical_bar_provenance",
                        "canonical_session_intervals",
                        "canonical_session_calendar",
                    )
                    if contour_id == "pit_active_front"
                    else (
                        "canonical_bars",
                        "canonical_bar_provenance",
                        "canonical_session_intervals",
                        "canonical_session_calendar",
                        "canonical_roll_map",
                    ),
                ),
                run_id=run_id,
                as_of_ts=_utc_now_iso(),
                source_delta_versions=source_delta_versions,
                source_delta_hashes=source_delta_hashes,
                continuous_front_policy=continuous_front_policy,
                code_version="research-bar-views-spark",
                notes={"contours": [*contours], "spark_plan": build_research_l0_sql_plan()},
            )
            contour_reports[contour_id] = materialize_research_dataset(
                manifest_seed=manifest,
                output_dir=output_dir,
                bar_view_count=bar_count,
                instrument_tree_count=tree_count,
                output_paths=output_paths,
            )

        primary_contour = contours[0] if contours else "native_tradable"
        scoped_filters = [
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", primary_contour),
        ]
        rows_by_table = {
            "research_bar_views": count_delta_table_rows(
                Path(output_paths["research_bar_views"]), filters=scoped_filters
            ),
            "research_instrument_tree": count_delta_table_rows(
                Path(output_paths["research_instrument_tree"]), filters=scoped_filters
            ),
            "research_datasets": count_delta_table_rows(
                Path(output_paths["research_datasets"]), filters=scoped_filters
            ),
        }
        total_rows_by_table = {
            "research_bar_views": count_delta_table_rows(Path(output_paths["research_bar_views"])),
            "research_instrument_tree": count_delta_table_rows(
                Path(output_paths["research_instrument_tree"])
            ),
            "research_datasets": count_delta_table_rows(Path(output_paths["research_datasets"])),
        }
        primary_report = contour_reports[primary_contour]
        return {
            **primary_report,
            "success": True,
            "status": "PASS",
            "run_id": run_id,
            "dataset_version": dataset_version,
            "contours": [*contours],
            "contour_reports": contour_reports,
            "rows_by_table": rows_by_table,
            "total_rows_by_table": total_rows_by_table,
            "contract_check_errors": contract_errors,
            "spark_profile": {
                "app_name": spec.app_name,
                "master": spark_master,
                "delta_reader": spec.delta_reader,
                "delta_writer": spec.delta_writer,
                "sql_plan": build_research_l0_sql_plan(),
            },
        }
    finally:
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()
