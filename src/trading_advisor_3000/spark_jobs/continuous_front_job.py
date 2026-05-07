from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Callable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    delta_table_columns,
    has_delta_log,
)
from trading_advisor_3000.product_plane.research.continuous_front import (
    CONTINUOUS_FRONT_TABLES,
    continuous_front_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import ContinuousFrontPolicy

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session


@dataclass(frozen=True)
class ContinuousFrontSparkJobSpec:
    app_name: str = "ta3000-continuous-front-refresh"
    target_bars_table: str = "continuous_front_bars"
    target_roll_events_table: str = "continuous_front_roll_events"
    target_adjustment_ladder_table: str = "continuous_front_adjustment_ladder"
    target_qc_report_table: str = "continuous_front_qc_report"


def build_continuous_front_sql_plan() -> str:
    return """
WITH filtered_canonical_bars AS (
  SELECT contract_id, instrument_id, timeframe, ts, open, high, low, close, volume, open_interest
  FROM canonical_bars
  WHERE (:instrument_ids_empty OR instrument_id IN (:instrument_ids))
    AND (:timeframes_empty OR timeframe IN (:timeframes))
    AND (:start_ts IS NULL OR ts >= :start_ts)
    AND (:end_ts IS NULL OR ts <= :end_ts)
),
ranked_contract_bars AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY instrument_id, timeframe, ts
           ORDER BY open_interest DESC, volume DESC, contract_id DESC
         ) AS liquidity_rank
  FROM filtered_canonical_bars
),
confirmed_roll_decisions AS (
  SELECT *,
         COUNT(*) OVER (
           PARTITION BY instrument_id, timeframe, candidate_maturity_rank
           ORDER BY ts ROWS BETWEEN :confirmation_bars_minus_one PRECEDING AND CURRENT ROW
         ) AS confirmation_run
  FROM ranked_contract_bars
),
active_timeline AS (
  SELECT *,
         MAX(confirmed_candidate_maturity_rank) OVER (
           PARTITION BY instrument_id, timeframe
           ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS active_maturity_rank
  FROM confirmed_roll_decisions
)
SELECT *
FROM active_timeline
ORDER BY instrument_id, timeframe, ts
""".strip()


def _spark_type(type_name: str):
    from pyspark.sql import types as T  # type: ignore[import-not-found]

    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return T.StringType()
    if normalized == "timestamp":
        return T.TimestampType()
    if normalized == "date":
        return T.DateType()
    if normalized in {"bool", "boolean"}:
        return T.BooleanType()
    if normalized in {"double", "float"}:
        return T.DoubleType()
    if normalized in {"int", "integer"}:
        return T.IntegerType()
    if normalized in {"bigint", "long"}:
        return T.LongType()
    return T.StringType()


def _spark_schema(columns: dict[str, str]):
    from pyspark.sql import types as T  # type: ignore[import-not-found]

    return T.StructType([T.StructField(name, _spark_type(type_name), True) for name, type_name in columns.items()])


def _spark_sql_type(type_name: str) -> str:
    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return "string"
    if normalized == "timestamp":
        return "timestamp"
    if normalized == "date":
        return "date"
    if normalized in {"bool", "boolean"}:
        return "boolean"
    if normalized in {"double", "float"}:
        return "double"
    if normalized in {"int", "integer"}:
        return "int"
    if normalized in {"bigint", "long"}:
        return "long"
    return "string"


def _require_spark_native_policy(policy: ContinuousFrontPolicy) -> None:
    unsupported: list[str] = []
    if policy.roll_policy_mode not in {"calendar_expiry_v1", "liquidity_oi_v1", "liquidity_volume_oi_v1"}:
        unsupported.append(f"roll_policy_mode={policy.roll_policy_mode}")
    if policy.adjustment_mode != "additive":
        unsupported.append(f"adjustment_mode={policy.adjustment_mode}")
    if policy.gap_type != "close_to_close":
        unsupported.append(f"gap_type={policy.gap_type}")
    supported_reference_policies = {"decision_bar_close"}
    supported_switch_timings = {"next_tradable_bar_after_decision_watermark"}
    if policy.roll_policy_mode == "calendar_expiry_v1":
        supported_reference_policies.add("last_old_active_close_to_first_new_active_close")
        supported_switch_timings.add("first_active_bar_on_or_after_roll_session")
    if policy.reference_price_policy not in supported_reference_policies:
        unsupported.append(f"reference_price_policy={policy.reference_price_policy}")
    if policy.switch_timing not in supported_switch_timings:
        unsupported.append(f"switch_timing={policy.switch_timing}")
    if not policy.decision_uses_closed_bar:
        unsupported.append("decision_uses_closed_bar=False")
    if not policy.effective_after_watermark:
        unsupported.append("effective_after_watermark=False")
    if unsupported:
        raise RuntimeError("continuous_front Spark native contour does not support: " + ", ".join(unsupported))


def _metric_columns(policy: ContinuousFrontPolicy) -> tuple[str, str]:
    if policy.roll_policy_mode == "liquidity_volume_oi_v1":
        return "volume", "open_interest"
    return policy.primary_metric, policy.secondary_metric


def _policy_timestamp() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_filtered_bars(
    *,
    spark: object,
    canonical_bars_path: Path,
    instrument_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    bars = spark.read.format("delta").load(str(canonical_bars_path)).select(
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
    )
    if instrument_ids:
        bars = bars.where(F.col("instrument_id").isin(list(instrument_ids)))
    if timeframes:
        bars = bars.where(F.col("timeframe").isin(list(timeframes)))
    if start_ts:
        bars = bars.where(F.col("ts") >= F.lit(start_ts).cast("timestamp"))
    if end_ts:
        bars = bars.where(F.col("ts") <= F.lit(end_ts).cast("timestamp"))
    return bars


def _load_session_calendar(*, spark: object, canonical_session_calendar_path: Path):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    return spark.read.format("delta").load(str(canonical_session_calendar_path)).select(
        "instrument_id",
        "timeframe",
        "session_date",
        F.col("session_open_ts").cast("timestamp").alias("session_open_ts"),
        F.col("session_close_ts").cast("timestamp").alias("session_close_ts"),
    )


def _load_roll_map(*, spark: object, canonical_roll_map_path: Path):
    return spark.read.format("delta").load(str(canonical_roll_map_path)).select(
        "instrument_id",
        "session_date",
        "active_contract_id",
    )


def _with_maturity_columns(dataframe):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    root = F.upper(F.split(F.col("contract_id"), "@").getItem(0))
    month_code = F.regexp_extract(root, r"([FGHJKMNQUVXZ])(\d{1,2})(?:$|[._-])", 1)
    code_year = F.regexp_extract(root, r"([FGHJKMNQUVXZ])(\d{1,2})(?:$|[._-])", 2)
    numeric_month = F.regexp_extract(root, r"[-_](\d{1,2})[._](\d{1,2})(?:$|[._-])", 1)
    numeric_year = F.regexp_extract(root, r"[-_](\d{1,2})[._](\d{1,2})(?:$|[._-])", 2)
    month_from_code = (
        F.when(month_code == "F", F.lit(1))
        .when(month_code == "G", F.lit(2))
        .when(month_code == "H", F.lit(3))
        .when(month_code == "J", F.lit(4))
        .when(month_code == "K", F.lit(5))
        .when(month_code == "M", F.lit(6))
        .when(month_code == "N", F.lit(7))
        .when(month_code == "Q", F.lit(8))
        .when(month_code == "U", F.lit(9))
        .when(month_code == "V", F.lit(10))
        .when(month_code == "X", F.lit(11))
        .when(month_code == "Z", F.lit(12))
    )
    raw_year = F.when(code_year != "", code_year).otherwise(F.when(numeric_year != "", numeric_year))
    maturity_year = (
        F.when(F.length(raw_year) == 1, F.lit(2020) + raw_year.cast("int"))
        .when(raw_year.cast("int") < 70, F.lit(2000) + raw_year.cast("int"))
        .otherwise(F.lit(1900) + raw_year.cast("int"))
    )
    maturity_month = F.when(month_code != "", month_from_code).otherwise(numeric_month.cast("int"))
    return dataframe.withColumn("maturity_year", maturity_year).withColumn(
        "maturity_month",
        maturity_month,
    ).withColumn(
        "maturity_rank",
        F.when(
            F.col("maturity_year").isNotNull() & F.col("maturity_month").between(1, 12),
            F.col("maturity_year") * F.lit(12) + F.col("maturity_month"),
        ),
    )


def _active_timeline(*, bars, policy: ContinuousFrontPolicy):
    from pyspark.sql import Window, functions as F  # type: ignore[import-not-found]

    primary_metric, secondary_metric = _metric_columns(policy)
    if primary_metric not in {"open_interest", "volume"} or secondary_metric not in {"open_interest", "volume"}:
        raise RuntimeError(
            "continuous_front Spark native contour supports only open_interest/volume ranking metrics"
        )

    keyed = _with_maturity_columns(bars)
    by_ts = Window.partitionBy("instrument_id", "timeframe", "ts")
    series_order = Window.partitionBy("instrument_id", "timeframe").orderBy("ts")
    candidate_order = Window.partitionBy("instrument_id", "timeframe", "ts").orderBy(
        F.col(primary_metric).desc(),
        F.col(secondary_metric).desc(),
        F.col("contract_id").desc(),
    )
    ranked = (
        keyed.withColumn("metric_total", F.sum(F.greatest(F.col(primary_metric).cast("double"), F.lit(0.0))).over(by_ts))
        .withColumn("candidate_metric", F.greatest(F.col(primary_metric).cast("double"), F.lit(0.0)))
        .withColumn("candidate_share", F.when(F.col("metric_total") <= 0.0, F.lit(1.0)).otherwise(F.col("candidate_metric") / F.col("metric_total")))
        .withColumn("liquidity_rank", F.row_number().over(candidate_order))
        .withColumn("ts_index", F.dense_rank().over(series_order))
        .withColumn("series_input_row_count", F.count(F.lit(1)).over(Window.partitionBy("instrument_id", "timeframe")))
        .withColumn("first_maturity_rank", F.min("maturity_rank").over(Window.partitionBy("instrument_id", "timeframe")))
    )
    candidates = ranked.where(F.col("liquidity_rank") == 1).select(
        "instrument_id",
        "timeframe",
        "ts",
        "ts_index",
        "series_input_row_count",
        F.col("contract_id").alias("candidate_contract_id"),
        F.col("maturity_rank").alias("candidate_maturity_rank"),
        "candidate_metric",
        "candidate_share",
        "first_maturity_rank",
    )
    recent_series_window = series_order.rowsBetween(-(policy.confirmation_bars - 1), 0)
    candidates = candidates.withColumn(
        "confirmation_count",
        F.count("candidate_maturity_rank").over(recent_series_window),
    ).withColumn(
        "confirmation_distinct_maturity_count",
        F.size(F.array_distinct(F.collect_list("candidate_maturity_rank").over(recent_series_window))),
    ).withColumn(
        "confirmed_candidate_maturity_rank",
        F.when(
            (F.col("ts_index") > 1)
            & (F.col("candidate_share") >= F.lit(policy.candidate_share_min))
            & (F.col("confirmation_count") >= F.lit(policy.confirmation_bars))
            & (F.col("confirmation_distinct_maturity_count") == F.lit(1)),
            F.col("candidate_maturity_rank"),
        ),
    )
    timeline = candidates.withColumn(
        "effective_candidate_maturity_rank",
        F.lag("confirmed_candidate_maturity_rank").over(series_order),
    ).withColumn(
        "decision_ts",
        F.lag(F.when(F.col("confirmed_candidate_maturity_rank").isNotNull(), F.col("ts"))).over(series_order),
    )
    active_maturity = F.greatest(
        F.col("first_maturity_rank"),
        F.max("effective_candidate_maturity_rank").over(series_order.rowsBetween(Window.unboundedPreceding, 0)),
    )
    return timeline.withColumn("active_maturity_rank", active_maturity).select(
        "instrument_id",
        "timeframe",
        "ts",
        "ts_index",
        "series_input_row_count",
        "candidate_contract_id",
        "candidate_maturity_rank",
        "active_maturity_rank",
        "decision_ts",
    )


def _active_joined_from_calendar_roll_map(
    *,
    keyed_bars,
    session_calendar,
    roll_map,
):
    from pyspark.sql import Window, functions as F  # type: ignore[import-not-found]

    series_window = Window.partitionBy("instrument_id", "timeframe")
    series_order = series_window.orderBy("ts")
    timestamp_rows = (
        keyed_bars.withColumn("series_input_row_count", F.count(F.lit(1)).over(series_window))
        .select("instrument_id", "timeframe", "ts", "series_input_row_count")
        .dropDuplicates(["instrument_id", "timeframe", "ts"])
        .withColumn("ts_index", F.dense_rank().over(series_order))
    )
    calendar = session_calendar.select(
        F.col("instrument_id").alias("calendar_instrument_id"),
        F.col("timeframe").alias("calendar_timeframe"),
        F.col("session_date").alias("calendar_session_date"),
        "session_open_ts",
        "session_close_ts",
    ).dropDuplicates(["calendar_instrument_id", "calendar_timeframe", "calendar_session_date"])
    active_by_session = roll_map.select(
        F.col("instrument_id").alias("roll_instrument_id"),
        F.col("session_date").alias("roll_session_date"),
        F.col("active_contract_id").alias("expected_active_contract_id"),
    ).dropDuplicates(["roll_instrument_id", "roll_session_date"])
    tr = timestamp_rows.alias("tr")
    cal = F.broadcast(calendar).alias("cal")
    rm = F.broadcast(active_by_session).alias("rm")
    timeline = (
        tr.join(
            cal,
            (F.col("tr.instrument_id") == F.col("cal.calendar_instrument_id"))
            & (F.col("tr.timeframe") == F.col("cal.calendar_timeframe"))
            & (F.col("tr.ts") >= F.col("cal.session_open_ts"))
            & (F.col("tr.ts") <= F.col("cal.session_close_ts")),
            "left",
        )
        .join(
            rm,
            (F.col("tr.instrument_id") == F.col("rm.roll_instrument_id"))
            & (F.col("cal.calendar_session_date") == F.col("rm.roll_session_date")),
            "left",
        )
        .select(
            F.col("tr.instrument_id").alias("instrument_id"),
            F.col("tr.timeframe").alias("timeframe"),
            F.col("tr.ts").alias("ts"),
            F.col("tr.ts_index").alias("ts_index"),
            F.col("tr.series_input_row_count").alias("series_input_row_count"),
            F.col("rm.expected_active_contract_id").alias("expected_active_contract_id"),
        )
    )
    kb = keyed_bars.select(
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
        "maturity_rank",
    ).alias("kb")
    tl = timeline.alias("tl")
    active_joined = tl.join(
        kb,
        on=(
            (F.col("tl.instrument_id") == F.col("kb.instrument_id"))
            & (F.col("tl.timeframe") == F.col("kb.timeframe"))
            & (F.col("tl.ts") == F.col("kb.ts"))
            & (F.col("tl.expected_active_contract_id") == F.col("kb.contract_id"))
        ),
        how="left",
    ).select(
        F.col("tl.instrument_id").alias("instrument_id"),
        F.col("tl.timeframe").alias("timeframe"),
        F.col("tl.ts").alias("ts"),
        F.col("tl.ts_index").alias("ts_index"),
        F.col("tl.series_input_row_count").alias("series_input_row_count"),
        F.col("tl.expected_active_contract_id").alias("candidate_contract_id"),
        F.col("kb.maturity_rank").alias("candidate_maturity_rank"),
        F.col("kb.maturity_rank").alias("active_maturity_rank"),
        F.col("kb.contract_id").alias("active_contract_id"),
        F.col("kb.open").alias("native_open"),
        F.col("kb.high").alias("native_high"),
        F.col("kb.low").alias("native_low"),
        F.col("kb.close").alias("native_close"),
        F.col("kb.volume").alias("native_volume"),
        F.col("kb.open_interest").alias("native_open_interest"),
    )
    return active_joined.withColumn("decision_ts", F.lag("ts").over(series_order))


def _build_spark_native_tables(
    *,
    spark: object,
    canonical_bars_path: Path,
    canonical_session_calendar_path: Path,
    canonical_roll_map_path: Path,
    dataset_version: str,
    policy: ContinuousFrontPolicy,
    run_id: str,
    instrument_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
):
    from pyspark.sql import Window, functions as F  # type: ignore[import-not-found]

    created_at = _policy_timestamp()
    bars = _load_filtered_bars(
        spark=spark,
        canonical_bars_path=canonical_bars_path,
        instrument_ids=instrument_ids,
        timeframes=timeframes,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    keyed_bars = _with_maturity_columns(bars)
    if policy.roll_policy_mode == "calendar_expiry_v1":
        active_joined = _active_joined_from_calendar_roll_map(
            keyed_bars=keyed_bars,
            session_calendar=_load_session_calendar(
                spark=spark,
                canonical_session_calendar_path=canonical_session_calendar_path,
            ),
            roll_map=_load_roll_map(
                spark=spark,
                canonical_roll_map_path=canonical_roll_map_path,
            ),
        )
    else:
        timeline = _active_timeline(bars=bars, policy=policy)
        tl = timeline.alias("tl")
        kb = keyed_bars.select(
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
            "maturity_rank",
        ).alias("kb")
        active_joined = tl.join(
            kb,
            on=(
                (F.col("tl.instrument_id") == F.col("kb.instrument_id"))
                & (F.col("tl.timeframe") == F.col("kb.timeframe"))
                & (F.col("tl.ts") == F.col("kb.ts"))
                & (F.col("tl.active_maturity_rank") == F.col("kb.maturity_rank"))
            ),
            how="left",
        ).select(
            F.col("tl.instrument_id").alias("instrument_id"),
            F.col("tl.timeframe").alias("timeframe"),
            F.col("tl.ts").alias("ts"),
            F.col("tl.ts_index").alias("ts_index"),
            F.col("tl.series_input_row_count").alias("series_input_row_count"),
            F.col("tl.candidate_contract_id").alias("candidate_contract_id"),
            F.col("tl.candidate_maturity_rank").alias("candidate_maturity_rank"),
            F.col("tl.active_maturity_rank").alias("active_maturity_rank"),
            F.col("tl.decision_ts").alias("decision_ts"),
            F.col("kb.contract_id").alias("active_contract_id"),
            F.col("kb.open").alias("native_open"),
            F.col("kb.high").alias("native_high"),
            F.col("kb.low").alias("native_low"),
            F.col("kb.close").alias("native_close"),
            F.col("kb.volume").alias("native_volume"),
            F.col("kb.open_interest").alias("native_open_interest"),
        )
    series_order = Window.partitionBy("instrument_id", "timeframe").orderBy("ts")
    previous_active = F.lag("active_contract_id").over(series_order)
    timeline_with_rolls = active_joined.withColumn("previous_active_contract_id", previous_active).withColumn(
        "is_roll_bar",
        F.col("active_contract_id").isNotNull()
        & F.col("previous_active_contract_id").isNotNull()
        & (F.col("active_contract_id") != F.col("previous_active_contract_id")),
    )
    event_base = (
        timeline_with_rolls.where(F.col("is_roll_bar"))
        .select(
            "instrument_id",
            "timeframe",
            F.col("previous_active_contract_id").alias("old_contract_id"),
            F.col("active_contract_id").alias("new_contract_id"),
            F.col("decision_ts").alias("decision_ts"),
            F.col("ts").alias("effective_ts"),
        )
        .where(F.col("decision_ts").isNotNull())
    )
    old_refs = keyed_bars.select(
        F.col("instrument_id").alias("old_instrument_id"),
        F.col("timeframe").alias("old_timeframe"),
        F.col("ts").alias("old_ts"),
        F.col("contract_id").alias("old_contract_id_ref"),
        F.col("close").alias("old_reference_price"),
    )
    new_refs = keyed_bars.select(
        F.col("instrument_id").alias("new_instrument_id"),
        F.col("timeframe").alias("new_timeframe"),
        F.col("ts").alias("new_ts"),
        F.col("contract_id").alias("new_contract_id_ref"),
        F.col("close").alias("new_reference_price"),
    )
    event_order = Window.partitionBy("instrument_id", "timeframe").orderBy("effective_ts")
    eb = event_base.alias("eb")
    old_ref = old_refs.alias("old_ref")
    new_ref = new_refs.alias("new_ref")
    new_reference_ts = "effective_ts" if policy.reference_price_policy == "last_old_active_close_to_first_new_active_close" else "decision_ts"
    event_causality_watermark = (
        F.col("effective_ts") if policy.roll_policy_mode == "calendar_expiry_v1" else F.col("decision_ts")
    )
    roll_events = (
        eb.join(
            old_ref,
            (F.col("eb.instrument_id") == F.col("old_ref.old_instrument_id"))
            & (F.col("eb.timeframe") == F.col("old_ref.old_timeframe"))
            & (F.col("eb.decision_ts") == F.col("old_ref.old_ts"))
            & (F.col("eb.old_contract_id") == F.col("old_ref.old_contract_id_ref")),
            "left",
        )
        .join(
            new_ref,
            (F.col("eb.instrument_id") == F.col("new_ref.new_instrument_id"))
            & (F.col("eb.timeframe") == F.col("new_ref.new_timeframe"))
            & (F.col(f"eb.{new_reference_ts}") == F.col("new_ref.new_ts"))
            & (F.col("eb.new_contract_id") == F.col("new_ref.new_contract_id_ref")),
            "left",
        )
        .select(
            F.col("eb.instrument_id").alias("instrument_id"),
            F.col("eb.timeframe").alias("timeframe"),
            F.col("eb.old_contract_id").alias("old_contract_id"),
            F.col("eb.new_contract_id").alias("new_contract_id"),
            F.col("eb.decision_ts").alias("decision_ts"),
            F.col("eb.effective_ts").alias("effective_ts"),
            F.col("old_reference_price"),
            F.col("new_reference_price"),
        )
        .withColumn("roll_sequence", F.row_number().over(event_order))
        .withColumn("roll_event_id", F.concat_ws("-", F.lit("CFRSPARK"), F.lit(dataset_version), F.col("instrument_id"), F.col("timeframe"), F.format_string("%04d", F.col("roll_sequence"))))
        .withColumn("additive_gap", F.col("new_reference_price") - F.col("old_reference_price"))
        .withColumn("dataset_version", F.lit(dataset_version))
        .withColumn("roll_policy_version", F.lit(policy.roll_policy_version))
        .withColumn("adjustment_policy_version", F.lit(policy.adjustment_policy_version))
        .withColumn("first_new_bar_ts", F.col("effective_ts"))
        .withColumn("last_old_bar_ts", F.col("decision_ts"))
        .withColumn("ratio_gap", F.lit(None).cast("double"))
        .withColumn("old_reference_source", F.lit(policy.reference_price_policy))
        .withColumn("new_reference_source", F.lit(policy.reference_price_policy))
        .withColumn("roll_reason", F.lit(policy.roll_policy_mode))
        .withColumn("causality_watermark_ts", event_causality_watermark)
        .withColumn("created_at", F.lit(created_at))
    )
    ladder = (
        roll_events.select(
            "dataset_version",
            "roll_policy_version",
            "adjustment_policy_version",
            "instrument_id",
            "timeframe",
            "roll_event_id",
            "roll_sequence",
            "effective_ts",
            "additive_gap",
            "created_at",
        )
        .withColumn(
            "cumulative_offset_before",
            F.col("additive_gap"),
        )
        .withColumn("cumulative_offset_after", F.lit(0.0))
        .withColumn("ratio_gap", F.lit(None).cast("double"))
        .withColumn("ratio_factor_before", F.lit(None).cast("double"))
        .withColumn("ratio_factor_after", F.lit(None).cast("double"))
    )
    event_for_bars = roll_events.select(
        F.col("instrument_id").alias("event_instrument_id"),
        F.col("timeframe").alias("event_timeframe"),
        F.col("effective_ts").alias("event_effective_ts"),
        F.col("old_contract_id").alias("event_old_contract_id"),
        F.col("roll_event_id").alias("event_roll_event_id"),
        F.col("roll_sequence").alias("event_roll_sequence"),
    )
    bar_base = timeline_with_rolls.alias("bar").where(F.col("active_contract_id").isNotNull()).join(
        event_for_bars.alias("event"),
        (F.col("bar.instrument_id") == F.col("event.event_instrument_id"))
        & (F.col("bar.timeframe") == F.col("event.event_timeframe"))
        & (F.col("bar.ts") == F.col("event.event_effective_ts")),
        "left",
    )
    bar_order = Window.partitionBy("instrument_id", "timeframe").orderBy("ts")
    roll_epoch = F.sum(F.when(F.col("event_roll_sequence").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(
        bar_order.rowsBetween(Window.unboundedPreceding, 0)
    )
    bars_with_offsets = (
        bar_base.withColumn("roll_epoch", roll_epoch)
        .withColumn("cumulative_additive_offset", F.lit(0.0))
        .withColumn("roll_event_id", F.last("event_roll_event_id", True).over(bar_order.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("previous_contract_id", F.last("event_old_contract_id", True).over(bar_order.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("bars_since_roll", F.row_number().over(Window.partitionBy("instrument_id", "timeframe", "roll_epoch").orderBy("ts")) - F.lit(1))
    )
    bar_causality_watermark = (
        F.col("ts") if policy.roll_policy_mode == "calendar_expiry_v1" else F.coalesce(F.col("decision_ts"), F.col("ts"))
    )
    continuous_bars = (
        bars_with_offsets.select(
            F.lit(dataset_version).alias("dataset_version"),
            F.lit(policy.roll_policy_version).alias("roll_policy_version"),
            F.lit(policy.adjustment_policy_version).alias("adjustment_policy_version"),
            F.col("instrument_id"),
            F.col("timeframe"),
            F.col("ts"),
            F.col("active_contract_id"),
            "previous_contract_id",
            F.col("candidate_contract_id"),
            "roll_epoch",
            "roll_event_id",
            F.col("is_roll_bar"),
            F.col("is_roll_bar").alias("is_first_bar_after_roll"),
            "bars_since_roll",
            "native_open",
            "native_high",
            "native_low",
            "native_close",
            F.col("native_volume").cast("long").alias("native_volume"),
            F.col("native_open_interest").cast("long").alias("native_open_interest"),
            F.col("native_open").alias("continuous_open"),
            F.col("native_high").alias("continuous_high"),
            F.col("native_low").alias("continuous_low"),
            F.col("native_close").alias("continuous_close"),
            F.lit(policy.adjustment_mode).alias("adjustment_mode"),
            "cumulative_additive_offset",
            F.lit(None).cast("double").alias("ratio_factor"),
            F.lit(policy.price_space).alias("price_space"),
            bar_causality_watermark.alias("causality_watermark_ts"),
            F.col("series_input_row_count").cast("long").alias("input_row_count"),
            F.lit(created_at).alias("created_at"),
        )
    )
    qc_group = Window.partitionBy("instrument_id", "timeframe")
    base_qc = keyed_bars.withColumn(
        "duplicate_row_count",
        F.count(F.lit(1)).over(Window.partitionBy("contract_id", "instrument_id", "timeframe", "ts")),
    )
    qc_failures = base_qc.groupBy("instrument_id", "timeframe").agg(
        F.count(F.lit(1)).cast("long").alias("input_row_count"),
        F.sum(F.when(F.col("duplicate_row_count") > 1, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("duplicate_key_count"),
        F.sum(F.when((F.col("high") < F.greatest(F.col("open"), F.col("close"))) | (F.col("low") > F.least(F.col("open"), F.col("close"))), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("ohlc_error_count"),
        F.sum(F.when((F.col("volume") < 0) | (F.col("open_interest") < 0), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("negative_volume_oi_count"),
        F.sum(F.when(F.col("maturity_rank").isNull(), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("maturity_parse_error_count"),
    )
    missing_active = timeline_with_rolls.groupBy("instrument_id", "timeframe").agg(
        F.sum(F.when(F.col("active_contract_id").isNull(), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("missing_active_bar_count")
    )
    event_qc = roll_events.groupBy("instrument_id", "timeframe").agg(
        F.count(F.lit(1)).cast("long").alias("roll_event_count"),
        F.sum(F.when(F.col("old_reference_price").isNull() | F.col("new_reference_price").isNull(), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("missing_reference_price_count"),
        F.max(F.abs(F.col("additive_gap"))).alias("gap_abs_max"),
        F.avg(F.abs(F.col("additive_gap"))).alias("gap_abs_mean"),
    )
    output_counts = continuous_bars.groupBy("instrument_id", "timeframe").agg(
        F.count(F.lit(1)).cast("long").alias("output_row_count")
    )
    qc = (
        qc_failures.join(missing_active, ["instrument_id", "timeframe"], "left")
        .join(event_qc, ["instrument_id", "timeframe"], "left")
        .join(output_counts, ["instrument_id", "timeframe"], "left")
        .withColumn("dataset_version", F.lit(dataset_version))
        .withColumn("roll_policy_version", F.lit(policy.roll_policy_version))
        .withColumn("adjustment_policy_version", F.lit(policy.adjustment_policy_version))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("started_at", F.lit(created_at))
        .withColumn("completed_at", F.lit(_policy_timestamp()))
        .withColumn("timeline_error_count", F.lit(0).cast("long"))
        .withColumn("future_causality_violation_count", F.lit(0).cast("long"))
        .withColumn("missing_active_bar_count", F.coalesce("missing_active_bar_count", F.lit(0)).cast("long"))
        .withColumn("roll_event_count", F.coalesce("roll_event_count", F.lit(0)).cast("long"))
        .withColumn("missing_reference_price_count", F.coalesce("missing_reference_price_count", F.lit(0)).cast("long") + F.col("maturity_parse_error_count"))
        .withColumn("output_row_count", F.coalesce("output_row_count", F.lit(0)).cast("long"))
        .withColumn("gap_abs_max", F.coalesce("gap_abs_max", F.lit(0.0)))
        .withColumn("gap_abs_mean", F.coalesce("gap_abs_mean", F.lit(0.0)))
        .withColumn(
            "blocked_reason",
            F.when(F.col("missing_active_bar_count") > 0, F.lit("missing_active_bar_count"))
            .when(F.col("duplicate_key_count") > 0, F.lit("duplicate_key_count"))
            .when(F.col("ohlc_error_count") > 0, F.lit("ohlc_error_count"))
            .when(F.col("negative_volume_oi_count") > 0, F.lit("negative_volume_oi_count"))
            .when(F.col("missing_reference_price_count") > 0, F.lit("missing_reference_price_count")),
        )
        .withColumn("status", F.when(F.col("blocked_reason").isNotNull(), F.lit("BLOCKED")).otherwise(F.lit("PASS")))
    )
    return {
        "continuous_front_bars": continuous_bars,
        "continuous_front_roll_events": roll_events,
        "continuous_front_adjustment_ladder": ladder,
        "continuous_front_qc_report": qc,
    }


def _cast_dataframe_to_contract(dataframe, manifest_entry: dict[str, object]):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    columns = dict(manifest_entry["columns"])
    selected = []
    for column_name, type_name in columns.items():
        spark_type = _spark_sql_type(str(type_name))
        if column_name in dataframe.columns:
            selected.append(F.col(column_name).cast(spark_type).alias(column_name))
        else:
            selected.append(F.lit(None).cast(spark_type).alias(column_name))
    return dataframe.select(*selected)


def _write_empty_spark_delta_table(*, spark: object, table_path: Path, manifest_entry: dict[str, object]) -> None:
    dataframe = spark.createDataFrame([], schema=_spark_schema(dict(manifest_entry["columns"])))
    writer = dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    partition_by = list(manifest_entry.get("partition_by") or [])
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(table_path))


def _write_spark_dataframe_tables(
    *,
    spark: object,
    output_dir: Path,
    tables: dict[str, object],
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    contract = continuous_front_store_contract()
    output_paths: dict[str, str] = {}
    for table_name in CONTINUOUS_FRONT_TABLES:
        table_path = output_dir / f"{table_name}.delta"
        table_path.parent.mkdir(parents=True, exist_ok=True)
        if table_path.exists():
            shutil.rmtree(table_path)
        dataframe = tables.get(table_name)
        if dataframe is None:
            _write_empty_spark_delta_table(spark=spark, table_path=table_path, manifest_entry=dict(contract[table_name]))
        else:
            casted = _cast_dataframe_to_contract(dataframe, dict(contract[table_name]))
            writer = casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            partition_by = list(contract[table_name].get("partition_by") or [])
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(str(table_path))
        output_paths[table_name] = table_path.as_posix()
    return output_paths


def _validate_spark_promoted_contracts(output_paths: dict[str, str]) -> list[str]:
    errors: list[str] = []
    contract = continuous_front_store_contract()
    for table_name in CONTINUOUS_FRONT_TABLES:
        table_path = Path(output_paths[table_name])
        if not has_delta_log(table_path):
            errors.append(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
            continue
        expected_columns = list(dict(contract[table_name]["columns"]).keys())
        actual_columns = list(delta_table_columns(table_path))
        missing = [column for column in expected_columns if column not in actual_columns]
        extra = [column for column in actual_columns if column not in expected_columns]
        if missing or extra:
            details: list[str] = []
            if missing:
                details.append("missing columns: " + ", ".join(missing))
            if extra:
                details.append("extra columns: " + ", ".join(extra))
            errors.append(f"schema mismatch for `{table_name}` ({'; '.join(details)})")
    return errors


def _qc_rows_from_spark(qc_dataframe) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in qc_dataframe.toLocalIterator():
        payload = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        rows.append({str(key): value for key, value in payload.items()})
    return rows


def run_continuous_front_spark_job(
    *,
    canonical_bars_path: Path,
    canonical_session_calendar_path: Path,
    canonical_roll_map_path: Path,
    output_dir: Path,
    dataset_version: str,
    policy: ContinuousFrontPolicy | None = None,
    run_id: str = "continuous_front_refresh",
    instrument_ids: tuple[str, ...] = (),
    timeframes: tuple[str, ...] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    spec = ContinuousFrontSparkJobSpec()
    resolved_policy = policy or ContinuousFrontPolicy()
    _require_spark_native_policy(resolved_policy)
    spark_factory = spark_session_factory or _create_spark_session
    spark = spark_factory(spec.app_name, spark_master)
    try:
        for table_path in (canonical_bars_path, canonical_session_calendar_path, canonical_roll_map_path):
            if not has_delta_log(table_path):
                raise RuntimeError(f"missing canonical delta table: {table_path.as_posix()}")
        spark.read.format("delta").load(str(canonical_session_calendar_path)).select("instrument_id").count()
        spark.read.format("delta").load(str(canonical_roll_map_path)).select("instrument_id").count()
        tables = _build_spark_native_tables(
            spark=spark,
            canonical_bars_path=canonical_bars_path,
            canonical_session_calendar_path=canonical_session_calendar_path,
            canonical_roll_map_path=canonical_roll_map_path,
            dataset_version=dataset_version,
            policy=resolved_policy,
            run_id=run_id,
            instrument_ids=instrument_ids,
            timeframes=timeframes,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        staging_dir = output_dir / "_staging" / "continuous_front_spark" / run_id
        staged_output_paths = _write_spark_dataframe_tables(spark=spark, output_dir=staging_dir, tables=tables)
        qc_rows = _qc_rows_from_spark(tables["continuous_front_qc_report"])
        blocking_rows = [row for row in qc_rows if row.get("status") == "BLOCKED"]
        if blocking_rows:
            blocked = "; ".join(
                f"{row.get('instrument_id')}|{row.get('timeframe')}:{row.get('blocked_reason')}" for row in blocking_rows
            )
            raise RuntimeError(f"continuous_front Spark QC failed closed: {blocked}")

        output_paths = _write_spark_dataframe_tables(spark=spark, output_dir=output_dir, tables=tables)
        contract_errors = _validate_spark_promoted_contracts(output_paths)
        if contract_errors:
            raise RuntimeError("continuous_front Spark contract validation failed: " + "; ".join(contract_errors))

        rows_by_table = {table_name: count_delta_table_rows(Path(path)) for table_name, path in output_paths.items()}
        return {
            "success": True,
            "status": "PASS",
            "run_id": run_id,
            "dataset_version": dataset_version,
            "policy": resolved_policy.to_config_dict(),
            "output_paths": output_paths,
            "staged_output_paths": staged_output_paths,
            "rows_by_table": rows_by_table,
            "qc_rows": qc_rows,
            "contract_check_errors": contract_errors,
            "delta_manifest": continuous_front_store_contract(),
            "spark_profile": {
                "app_name": spec.app_name,
                "master": spark_master,
                "delta_reader": "spark",
                "delta_writer": "spark",
                "causal_roll_engine": "spark-native-window-batch",
                "sql_plan": build_continuous_front_sql_plan(),
            },
        }
    finally:
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()
