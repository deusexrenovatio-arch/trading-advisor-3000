from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    delta_table_columns,
    has_delta_log,
)
from trading_advisor_3000.product_plane.research.continuous_front_indicators import (
    input_projection as cf_input_projection,
)
from trading_advisor_3000.product_plane.research.continuous_front_indicators.contracts import (
    continuous_front_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.continuous_front_indicators.qc import (
    stable_hash,
)
from trading_advisor_3000.product_plane.runtime.stage_timings import (
    StageTimings,
    record_skipped_stage,
    record_stage_timing,
    stage_timer,
)

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session
from .research_bar_views_job import _scoped_delete_condition

ROW_HASH_VERSION = "continuous-front-indicator-row-hash-v2"

BAR_VIEW_INPUT_COLUMNS = (
    "dataset_version",
    "contour_id",
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
    "session_date",
    "session_open_ts",
    "session_close_ts",
    "active_contract_id",
    "roll_epoch",
    "is_roll_bar",
    "is_first_bar_after_roll",
    "bars_since_roll",
    "native_open",
    "native_high",
    "native_low",
    "native_close",
    "cumulative_additive_offset",
)

BASE_SOURCE_RESERVED_COLUMNS = (
    "dataset_version",
    "indicator_set_version",
    "profile_version",
    "contract_id",
    "instrument_id",
    "timeframe",
    "ts",
    "source_bars_hash",
    "source_dataset_bars_hash",
    "row_count",
    "warmup_span",
    "null_warmup_span",
    "created_at",
    "output_columns_hash",
)

DERIVED_SOURCE_RESERVED_COLUMNS = (
    "dataset_version",
    "indicator_set_version",
    "derived_indicator_set_version",
    "profile_version",
    "contract_id",
    "instrument_id",
    "timeframe",
    "ts",
    "source_bars_hash",
    "source_dataset_bars_hash",
    "source_indicators_hash",
    "source_indicator_profile_version",
    "source_indicator_output_columns_hash",
    "row_count",
    "warmup_span",
    "null_warmup_span",
    "created_at",
    "output_columns_hash",
)

INPUT_JOIN_COLUMNS = (
    "instrument_id",
    "timeframe",
    "ts",
    "ts_close",
    "session_date",
    "active_contract_id",
    "roll_epoch_id",
    "roll_seq",
    "bars_since_roll",
    "cumulative_additive_offset",
    "input_front_row_hash",
)


@dataclass(frozen=True)
class ContinuousFrontIndicatorSidecarSparkJobSpec:
    app_name: str = "ta3000-continuous-front-indicator-sidecar"
    delta_reader: str = "spark"
    delta_writer: str = "spark"
    hash_compatibility: str = "python_stable_hash_udf"


class ContinuousFrontIndicatorSidecarSparkUnavailable(RuntimeError):
    pass


def build_continuous_front_indicator_sidecar_sql_plan() -> str:
    return """
    research_bar_views + continuous_front_adjustment_ladder
      -> cf_indicator_input_frame
    cf_indicator_input_frame + research_indicator_frames
      -> continuous_front_indicator_frames
    cf_indicator_input_frame + continuous_front_indicator_frames
      + research_derived_indicator_frames
      -> continuous_front_derived_indicator_frames
    write:
      Delta scoped replace by dataset/rule/profile/policy partitions.
    """.strip()


def _spark_cast_type(type_name: str) -> str:
    normalized = type_name.strip().lower()
    if normalized in {"json", "string"}:
        return "string"
    if normalized in {"bigint", "long"}:
        return "bigint"
    if normalized in {"int", "integer"}:
        return "int"
    if normalized in {"bool", "boolean"}:
        return "boolean"
    if normalized in {"double", "float"}:
        return "double"
    if normalized in {"timestamp", "date"}:
        return normalized
    if normalized.startswith("array<"):
        return normalized
    return "string"


def _required_columns_present(
    *, table_path: Path, required_columns: Iterable[str], table_label: str
) -> None:
    if not has_delta_log(table_path):
        raise RuntimeError(f"missing Delta table: {table_path.as_posix()}")
    available = set(delta_table_columns(table_path))
    missing = sorted(set(required_columns) - available)
    if missing:
        raise ValueError(f"{table_label} missing: {', '.join(missing)}")


def _validate_sources_before_spark(
    *,
    materialized_output_dir: Path,
    indicator_value_columns: tuple[str, ...],
    derived_value_columns: tuple[str, ...],
    include_derived: bool,
) -> None:
    _required_columns_present(
        table_path=materialized_output_dir / "research_bar_views.delta",
        required_columns=BAR_VIEW_INPUT_COLUMNS,
        table_label="continuous-front sidecar input projection",
    )
    _required_columns_present(
        table_path=materialized_output_dir / "continuous_front_adjustment_ladder.delta",
        required_columns=(
            "dataset_version",
            "roll_policy_version",
            "adjustment_policy_version",
            "instrument_id",
            "timeframe",
            "roll_sequence",
            "additive_gap",
        ),
        table_label="continuous-front sidecar adjustment ladder",
    )
    _required_columns_present(
        table_path=materialized_output_dir / "research_indicator_frames.delta",
        required_columns=(*BASE_SOURCE_RESERVED_COLUMNS, *indicator_value_columns),
        table_label="continuous-front sidecar base indicator columns",
    )
    if include_derived:
        _required_columns_present(
            table_path=materialized_output_dir / "research_derived_indicator_frames.delta",
            required_columns=(*DERIVED_SOURCE_RESERVED_COLUMNS, *derived_value_columns),
            table_label="continuous-front sidecar derived indicator columns",
        )


def _normalize_hash_value(value: object) -> object:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_normalize_hash_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_hash_value(item) for key, item in value.items()}
    return value


def _normalize_numeric_hash_value(value: object) -> object:
    normalized = _normalize_hash_value(value)
    if isinstance(normalized, float) and normalized.is_integer():
        return int(normalized)
    return normalized


def _input_row_hash_from_payload(payload: object) -> str:
    row = payload.asDict(recursive=True) if hasattr(payload, "asDict") else dict(payload or {})
    hashed = {
        str(key): _normalize_hash_value(value)
        for key, value in row.items()
        if key not in {"created_at_utc", "input_front_row_hash"}
    }
    return stable_hash(hashed)


def _sidecar_row_hash_from_payload(payload: object, value_columns: tuple[str, ...]) -> str:
    row = payload.asDict(recursive=True) if hasattr(payload, "asDict") else dict(payload or {})
    hashed = {column: _normalize_numeric_hash_value(row.get(column)) for column in value_columns}
    hashed.update(
        {
            "dataset_version": _normalize_hash_value(row.get("dataset_version")),
            "instrument_id": _normalize_hash_value(row.get("instrument_id")),
            "timeframe": _normalize_hash_value(row.get("timeframe")),
            "ts": _normalize_hash_value(row.get("ts")),
            "_hash_version": ROW_HASH_VERSION,
        }
    )
    return stable_hash(hashed)


def _cast_to_sidecar_contract(
    dataframe,
    *,
    table_name: str,
    contract: dict[str, dict[str, object]],
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    columns = dict(contract[table_name]["columns"])
    existing = set(dataframe.columns)
    return dataframe.select(
        *[
            (
                F.col(column).cast(_spark_cast_type(str(type_name))).alias(column)
                if column in existing
                else F.lit(None).cast(_spark_cast_type(str(type_name))).alias(column)
            )
            for column, type_name in columns.items()
        ]
    )


def _write_sidecar_delta_table(
    *,
    dataframe,
    output_dir: Path,
    table_name: str,
    contract: dict[str, dict[str, object]],
    replace_scope: list[tuple[str, str, object]],
) -> None:
    table_path = output_dir / f"{table_name}.delta"
    casted = _cast_to_sidecar_contract(dataframe, table_name=table_name, contract=contract)
    partition_by = list(contract[table_name].get("partition_by") or [])
    table_path.parent.mkdir(parents=True, exist_ok=True)
    if not has_delta_log(table_path):
        writer = casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(str(table_path))
        return

    casted.limit(0).write.format("delta").mode("append").option("mergeSchema", "true").save(
        str(table_path)
    )

    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    DeltaTable.forPath(dataframe.sparkSession, str(table_path)).delete(
        _scoped_delete_condition(replace_scope)
    )
    casted.write.format("delta").mode("append").option("mergeSchema", "true").save(str(table_path))


def _scope_filters(
    *,
    dataset_version: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    indicator_set_version: str | None = None,
    derived_set_version: str | None = None,
    rule_set_version: str | None = None,
    source_canonical_version: str | None = None,
) -> list[tuple[str, str, object]]:
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("roll_policy_version", "=", roll_policy_version),
        ("adjustment_policy_version", "=", adjustment_policy_version),
    ]
    if source_canonical_version is not None:
        filters.append(("source_canonical_version", "=", source_canonical_version))
    if indicator_set_version is not None:
        filters.append(("indicator_set_version", "=", indicator_set_version))
    if derived_set_version is not None:
        filters.append(("derived_set_version", "=", derived_set_version))
    if rule_set_version is not None:
        filters.append(("rule_set_version", "=", rule_set_version))
    return filters


def _build_input_frame(
    *,
    spark,
    materialized_output_dir: Path,
    dataset_version: str,
    contour_id: str,
    source_canonical_version: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    created_at_utc: str,
):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]
    from pyspark.sql.types import StringType  # type: ignore[import-not-found]

    bars = (
        spark.read.format("delta")
        .load(str(materialized_output_dir / "research_bar_views.delta"))
        .where(
            (F.col("dataset_version") == F.lit(dataset_version))
            & (F.col("contour_id") == F.lit(contour_id))
        )
    )
    ladder = (
        spark.read.format("delta")
        .load(str(materialized_output_dir / "continuous_front_adjustment_ladder.delta"))
        .where(
            (F.col("dataset_version") == F.lit(dataset_version))
            & (F.col("roll_policy_version") == F.lit(roll_policy_version))
            & (F.col("adjustment_policy_version") == F.lit(adjustment_policy_version))
        )
        .select(
            "instrument_id",
            "timeframe",
            F.col("roll_sequence").cast("int").alias("roll_seq"),
            F.col("additive_gap").cast("double").alias("additive_gap"),
        )
    )
    offset_window = (
        Window.partitionBy("instrument_id", "timeframe")
        .orderBy("roll_seq")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    ladder_offsets = ladder.select(
        "instrument_id",
        "timeframe",
        "roll_seq",
        F.sum("additive_gap").over(offset_window).alias("__ladder_offset"),
    )
    zero_offsets = (
        bars.select("instrument_id", "timeframe")
        .distinct()
        .select(
            "instrument_id",
            "timeframe",
            F.lit(0).cast("int").alias("roll_seq"),
            F.lit(0.0).cast("double").alias("__ladder_offset"),
        )
    )
    offsets = zero_offsets.unionByName(ladder_offsets)
    bars_with_roll = bars.withColumn(
        "__roll_seq", F.coalesce(F.col("roll_epoch"), F.lit(0)).cast("int")
    ).alias("bars")
    offsets = offsets.alias("offsets")
    joined = bars_with_roll.join(
        offsets,
        on=[
            F.col("bars.instrument_id") == F.col("offsets.instrument_id"),
            F.col("bars.timeframe") == F.col("offsets.timeframe"),
            F.col("bars.__roll_seq") == F.col("offsets.roll_seq"),
        ],
        how="left",
    ).select("bars.*", F.col("offsets.__ladder_offset").alias("__ladder_offset"))
    missing_offsets = int(
        joined.where((F.col("__roll_seq") > F.lit(0)) & F.col("__ladder_offset").isNull()).count()
    )
    if missing_offsets:
        raise ValueError(
            "cf_indicator_input_frame missing adjustment ladder roll_sequence "
            f"for {missing_offsets} rolled rows"
        )

    bar_close_ts_udf = F.udf(
        lambda ts, timeframe: cf_input_projection._bar_close_ts(
            str(_normalize_hash_value(ts)), str(timeframe)
        ),
        StringType(),
    )
    with_prices = (
        joined.withColumn(
            "__native_open", F.coalesce(F.col("native_open"), F.col("open")).cast("double")
        )
        .withColumn("__native_high", F.coalesce(F.col("native_high"), F.col("high")).cast("double"))
        .withColumn("__native_low", F.coalesce(F.col("native_low"), F.col("low")).cast("double"))
        .withColumn(
            "__native_close", F.coalesce(F.col("native_close"), F.col("close")).cast("double")
        )
        .withColumn(
            "__offset",
            F.coalesce(
                F.col("__ladder_offset"), F.col("cumulative_additive_offset"), F.lit(0.0)
            ).cast("double"),
        )
        .withColumn("__open0", F.col("__native_open") - F.col("__offset"))
        .withColumn("__high0", F.col("__native_high") - F.col("__offset"))
        .withColumn("__low0", F.col("__native_low") - F.col("__offset"))
        .withColumn("__close0", F.col("__native_close") - F.col("__offset"))
    )
    price_window = Window.partitionBy("instrument_id", "timeframe").orderBy("ts", "contract_id")
    with_true_range = (
        with_prices.withColumn("__previous_close0", F.lag("__close0").over(price_window))
        .withColumn("__hl_range", F.col("__high0") - F.col("__low0"))
        .withColumn(
            "__true_range0",
            F.greatest(
                F.col("__hl_range"),
                F.when(F.col("__previous_close0").isNull(), F.col("__hl_range")).otherwise(
                    F.abs(F.col("__high0") - F.col("__previous_close0"))
                ),
                F.when(F.col("__previous_close0").isNull(), F.col("__hl_range")).otherwise(
                    F.abs(F.col("__low0") - F.col("__previous_close0"))
                ),
            ),
        )
        .withColumn(
            "__ts_close",
            bar_close_ts_udf(F.col("ts"), F.col("timeframe")).cast("timestamp"),
        )
    )
    selected = with_true_range.select(
        F.lit(dataset_version).alias("dataset_version"),
        F.lit(source_canonical_version).alias("source_canonical_version"),
        F.lit(roll_policy_version).alias("roll_policy_version"),
        F.lit(adjustment_policy_version).alias("adjustment_policy_version"),
        F.col("instrument_id"),
        F.col("timeframe"),
        F.col("ts"),
        F.col("__ts_close").alias("ts_close"),
        F.col("session_date"),
        F.coalesce(F.col("session_open_ts"), F.col("ts")).alias("session_open_ts"),
        F.coalesce(F.col("session_close_ts"), F.col("__ts_close")).alias("session_close_ts"),
        F.col("active_contract_id"),
        F.concat_ws("|", F.col("instrument_id"), F.col("timeframe"), F.col("__roll_seq")).alias(
            "roll_epoch_id"
        ),
        F.col("__roll_seq").alias("roll_seq"),
        F.col("is_roll_bar"),
        F.col("is_first_bar_after_roll"),
        F.coalesce(F.col("bars_since_roll"), F.lit(0)).cast("int").alias("bars_since_roll"),
        F.col("__native_open").alias("native_open"),
        F.col("__native_high").alias("native_high"),
        F.col("__native_low").alias("native_low"),
        F.col("__native_close").alias("native_close"),
        F.col("volume").cast("bigint").alias("native_volume"),
        F.col("open_interest").cast("bigint").alias("native_open_interest"),
        F.col("__offset").alias("cumulative_additive_offset"),
        F.col("__open0").alias("open0"),
        F.col("__high0").alias("high0"),
        F.col("__low0").alias("low0"),
        F.col("__close0").alias("close0"),
        ((F.col("__high0") + F.col("__low0")) / F.lit(2.0)).alias("hl2_0"),
        ((F.col("__high0") + F.col("__low0") + F.col("__close0")) / F.lit(3.0)).alias("hlc3_0"),
        F.col("__true_range0").alias("true_range0"),
        F.lit("contract_native").alias("price_space_native"),
        F.lit("causal_zero_anchor").alias("price_space_normalized"),
        F.col("__ts_close").alias("causality_watermark_ts"),
        F.lit(created_at_utc).cast("timestamp").alias("created_at_utc"),
    )
    input_hash_udf = F.udf(_input_row_hash_from_payload, StringType())
    return selected.withColumn(
        "input_front_row_hash",
        input_hash_udf(F.struct(*[F.col(column) for column in selected.columns])),
    )


def _build_base_sidecar_frame(
    *,
    spark,
    materialized_output_dir: Path,
    input_frame,
    dataset_version: str,
    contour_id: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    indicator_set_version: str,
    rule_set_version: str,
    adapter_hash: str,
    indicator_value_columns: tuple[str, ...],
    max_cross_contract_window_bars: int,
    created_at_utc: str,
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]
    from pyspark.sql.types import StringType  # type: ignore[import-not-found]

    base_source = (
        spark.read.format("delta")
        .load(str(materialized_output_dir / "research_indicator_frames.delta"))
        .where(
            (F.col("dataset_version") == F.lit(dataset_version))
            & (F.col("contour_id") == F.lit(contour_id))
            & (F.col("indicator_set_version") == F.lit(indicator_set_version))
        )
    )
    input_projection = input_frame.select(*[F.col(column) for column in INPUT_JOIN_COLUMNS])
    joined = base_source.alias("base").join(
        input_projection.alias("input"),
        [
            F.col("base.instrument_id").eqNullSafe(F.col("input.instrument_id")),
            F.col("base.timeframe").eqNullSafe(F.col("input.timeframe")),
            F.col("base.ts").eqNullSafe(F.col("input.ts")),
        ],
        "inner",
    )
    selected = joined.select(
        F.lit(dataset_version).alias("dataset_version"),
        F.lit(roll_policy_version).alias("roll_policy_version"),
        F.lit(adjustment_policy_version).alias("adjustment_policy_version"),
        F.lit(indicator_set_version).alias("indicator_set_version"),
        F.lit(rule_set_version).alias("rule_set_version"),
        F.col("base.instrument_id").alias("instrument_id"),
        F.col("base.timeframe").alias("timeframe"),
        F.col("base.ts").alias("ts"),
        F.col("input.ts_close").alias("ts_close"),
        F.col("input.session_date").alias("session_date"),
        F.col("input.active_contract_id").alias("active_contract_id"),
        F.col("input.roll_epoch_id").alias("roll_epoch_id"),
        F.col("input.roll_seq").alias("roll_seq"),
        F.col("input.cumulative_additive_offset").alias("cumulative_additive_offset"),
        F.col("input.input_front_row_hash").alias("source_input_row_hash"),
        F.lit(adapter_hash).alias("adapter_bundle_hash"),
        (
            (F.col("input.roll_seq") > F.lit(0))
            & (F.col("input.bars_since_roll") < F.lit(max_cross_contract_window_bars))
        ).alias("cross_contract_window_any"),
        *[F.col(f"base.{column}").alias(column) for column in indicator_value_columns],
        F.lit(created_at_utc).cast("timestamp").alias("created_at_utc"),
    )
    row_hash_udf = F.udf(
        lambda payload: _sidecar_row_hash_from_payload(payload, indicator_value_columns),
        StringType(),
    )
    return selected.withColumn(
        "indicator_row_hash",
        row_hash_udf(
            F.struct(
                *[
                    F.col(column)
                    for column in (
                        "dataset_version",
                        "instrument_id",
                        "timeframe",
                        "ts",
                        *indicator_value_columns,
                    )
                ]
            )
        ),
    ).withColumn("indicator_row_hash_version", F.lit(ROW_HASH_VERSION))


def _build_derived_sidecar_frame(
    *,
    spark,
    materialized_output_dir: Path,
    input_frame,
    base_sidecar_frame,
    dataset_version: str,
    contour_id: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    indicator_set_version: str,
    derived_set_version: str,
    rule_set_version: str,
    adapter_hash: str,
    derived_value_columns: tuple[str, ...],
    max_cross_contract_window_bars: int,
    created_at_utc: str,
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]
    from pyspark.sql.types import StringType  # type: ignore[import-not-found]

    derived_source = (
        spark.read.format("delta")
        .load(str(materialized_output_dir / "research_derived_indicator_frames.delta"))
        .where(
            (F.col("dataset_version") == F.lit(dataset_version))
            & (F.col("contour_id") == F.lit(contour_id))
            & (F.col("indicator_set_version") == F.lit(indicator_set_version))
            & (F.col("derived_indicator_set_version") == F.lit(derived_set_version))
        )
    )
    input_projection = input_frame.select(*[F.col(column) for column in INPUT_JOIN_COLUMNS])
    base_hash_projection = base_sidecar_frame.select(
        "instrument_id",
        "timeframe",
        "ts",
        "indicator_row_hash",
        "indicator_row_hash_version",
    )
    with_input = derived_source.alias("derived").join(
        input_projection.alias("input"),
        [
            F.col("derived.instrument_id").eqNullSafe(F.col("input.instrument_id")),
            F.col("derived.timeframe").eqNullSafe(F.col("input.timeframe")),
            F.col("derived.ts").eqNullSafe(F.col("input.ts")),
        ],
        "inner",
    )
    joined = with_input.alias("joined").join(
        base_hash_projection.alias("base_hash"),
        [
            F.col("joined.instrument_id").eqNullSafe(F.col("base_hash.instrument_id")),
            F.col("joined.timeframe").eqNullSafe(F.col("base_hash.timeframe")),
            F.col("joined.ts").eqNullSafe(F.col("base_hash.ts")),
        ],
        "inner",
    )
    selected = joined.select(
        F.lit(dataset_version).alias("dataset_version"),
        F.lit(roll_policy_version).alias("roll_policy_version"),
        F.lit(adjustment_policy_version).alias("adjustment_policy_version"),
        F.lit(indicator_set_version).alias("indicator_set_version"),
        F.lit(derived_set_version).alias("derived_set_version"),
        F.lit(rule_set_version).alias("rule_set_version"),
        F.col("joined.instrument_id").alias("instrument_id"),
        F.col("joined.timeframe").alias("timeframe"),
        F.col("joined.ts").alias("ts"),
        F.col("joined.ts_close").alias("ts_close"),
        F.col("joined.session_date").alias("session_date"),
        F.col("joined.active_contract_id").alias("active_contract_id"),
        F.col("joined.roll_epoch_id").alias("roll_epoch_id"),
        F.col("joined.roll_seq").alias("roll_seq"),
        F.col("joined.cumulative_additive_offset").alias("cumulative_additive_offset"),
        F.col("joined.input_front_row_hash").alias("source_input_row_hash"),
        F.col("base_hash.indicator_row_hash").alias("source_base_indicator_row_hash"),
        F.col("base_hash.indicator_row_hash_version").alias(
            "source_base_indicator_row_hash_version"
        ),
        F.lit(adapter_hash).alias("adapter_bundle_hash"),
        (
            (F.col("joined.roll_seq") > F.lit(0))
            & (F.col("joined.bars_since_roll") < F.lit(max_cross_contract_window_bars))
        ).alias("cross_contract_window_any"),
        *[F.col(f"joined.{column}").alias(column) for column in derived_value_columns],
        F.lit(created_at_utc).cast("timestamp").alias("created_at_utc"),
    )
    row_hash_udf = F.udf(
        lambda payload: _sidecar_row_hash_from_payload(payload, derived_value_columns),
        StringType(),
    )
    return selected.withColumn(
        "derived_row_hash",
        row_hash_udf(
            F.struct(
                *[
                    F.col(column)
                    for column in (
                        "dataset_version",
                        "instrument_id",
                        "timeframe",
                        "ts",
                        *derived_value_columns,
                    )
                ]
            )
        ),
    ).withColumn("derived_row_hash_version", F.lit(ROW_HASH_VERSION))


def run_continuous_front_indicator_sidecar_spark_job(
    *,
    materialized_output_dir: Path,
    output_dir: Path,
    dataset_version: str,
    contour_id: str,
    source_canonical_version: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    indicator_set_version: str,
    derived_set_version: str,
    rule_set_version: str,
    adapter_hash: str,
    indicator_value_columns: tuple[str, ...],
    derived_value_columns: tuple[str, ...],
    max_base_cross_contract_window_bars: int,
    max_derived_cross_contract_window_bars: int,
    created_at_utc: str,
    contract: dict[str, dict[str, object]] | None = None,
    include_derived: bool = True,
    rule_count: int = 0,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    stage_timings: StageTimings = {}
    stage_started = stage_timer()
    _validate_sources_before_spark(
        materialized_output_dir=materialized_output_dir,
        indicator_value_columns=indicator_value_columns,
        derived_value_columns=derived_value_columns,
        include_derived=include_derived,
    )
    resolved_contract = contract or continuous_front_indicator_store_contract()
    record_stage_timing(stage_timings, "validate_sources", stage_started)
    spec = ContinuousFrontIndicatorSidecarSparkJobSpec()
    spark_factory = spark_session_factory or _create_spark_session
    try:
        stage_started = stage_timer()
        spark = spark_factory(spec.app_name, spark_master)
        record_stage_timing(stage_timings, "start_spark", stage_started, master=spark_master)
    except Exception as exc:  # pragma: no cover - depends on host Spark runtime
        raise ContinuousFrontIndicatorSidecarSparkUnavailable(str(exc)) from exc
    try:
        stage_started = stage_timer()
        input_frame = _build_input_frame(
            spark=spark,
            materialized_output_dir=materialized_output_dir,
            dataset_version=dataset_version,
            contour_id=contour_id,
            source_canonical_version=source_canonical_version,
            roll_policy_version=roll_policy_version,
            adjustment_policy_version=adjustment_policy_version,
            created_at_utc=created_at_utc,
        ).cache()
        record_stage_timing(stage_timings, "build_input_frame", stage_started)

        stage_started = stage_timer()
        base_frame = _build_base_sidecar_frame(
            spark=spark,
            materialized_output_dir=materialized_output_dir,
            input_frame=input_frame,
            dataset_version=dataset_version,
            contour_id=contour_id,
            roll_policy_version=roll_policy_version,
            adjustment_policy_version=adjustment_policy_version,
            indicator_set_version=indicator_set_version,
            rule_set_version=rule_set_version,
            adapter_hash=adapter_hash,
            indicator_value_columns=indicator_value_columns,
            max_cross_contract_window_bars=max_base_cross_contract_window_bars,
            created_at_utc=created_at_utc,
        ).cache()
        record_stage_timing(stage_timings, "build_base_sidecar_frame", stage_started)

        derived_frame = None
        if include_derived:
            stage_started = stage_timer()
            derived_frame = _build_derived_sidecar_frame(
                spark=spark,
                materialized_output_dir=materialized_output_dir,
                input_frame=input_frame,
                base_sidecar_frame=base_frame,
                dataset_version=dataset_version,
                contour_id=contour_id,
                roll_policy_version=roll_policy_version,
                adjustment_policy_version=adjustment_policy_version,
                indicator_set_version=indicator_set_version,
                derived_set_version=derived_set_version,
                rule_set_version=rule_set_version,
                adapter_hash=adapter_hash,
                derived_value_columns=derived_value_columns,
                max_cross_contract_window_bars=max_derived_cross_contract_window_bars,
                created_at_utc=created_at_utc,
            ).cache()
            record_stage_timing(stage_timings, "build_derived_sidecar_frame", stage_started)
        else:
            record_skipped_stage(
                stage_timings,
                "build_derived_sidecar_frame",
                reason="derived sidecar not requested",
            )

        stage_started = stage_timer()
        _write_sidecar_delta_table(
            dataframe=input_frame,
            output_dir=output_dir,
            table_name="cf_indicator_input_frame",
            contract=resolved_contract,
            replace_scope=_scope_filters(
                dataset_version=dataset_version,
                roll_policy_version=roll_policy_version,
                adjustment_policy_version=adjustment_policy_version,
                source_canonical_version=source_canonical_version,
            ),
        )
        record_stage_timing(stage_timings, "write_input_frame", stage_started)

        stage_started = stage_timer()
        _write_sidecar_delta_table(
            dataframe=base_frame,
            output_dir=output_dir,
            table_name="continuous_front_indicator_frames",
            contract=resolved_contract,
            replace_scope=_scope_filters(
                dataset_version=dataset_version,
                roll_policy_version=roll_policy_version,
                adjustment_policy_version=adjustment_policy_version,
                indicator_set_version=indicator_set_version,
                rule_set_version=rule_set_version,
            ),
        )
        record_stage_timing(stage_timings, "write_base_sidecar", stage_started)
        if derived_frame is not None:
            stage_started = stage_timer()
            _write_sidecar_delta_table(
                dataframe=derived_frame,
                output_dir=output_dir,
                table_name="continuous_front_derived_indicator_frames",
                contract=resolved_contract,
                replace_scope=_scope_filters(
                    dataset_version=dataset_version,
                    roll_policy_version=roll_policy_version,
                    adjustment_policy_version=adjustment_policy_version,
                    indicator_set_version=indicator_set_version,
                    derived_set_version=derived_set_version,
                    rule_set_version=rule_set_version,
                ),
            )
            record_stage_timing(stage_timings, "write_derived_sidecar", stage_started)
        else:
            record_skipped_stage(
                stage_timings,
                "write_derived_sidecar",
                reason="derived sidecar not requested",
            )

        input_filters = _scope_filters(
            dataset_version=dataset_version,
            roll_policy_version=roll_policy_version,
            adjustment_policy_version=adjustment_policy_version,
            source_canonical_version=source_canonical_version,
        )
        base_filters = _scope_filters(
            dataset_version=dataset_version,
            roll_policy_version=roll_policy_version,
            adjustment_policy_version=adjustment_policy_version,
            indicator_set_version=indicator_set_version,
            rule_set_version=rule_set_version,
        )
        stage_started = stage_timer()
        rows_by_table = {
            "cf_indicator_input_frame": count_delta_table_rows(
                output_dir / "cf_indicator_input_frame.delta", filters=input_filters
            ),
            "indicator_roll_rules": int(rule_count),
            "continuous_front_indicator_frames": count_delta_table_rows(
                output_dir / "continuous_front_indicator_frames.delta", filters=base_filters
            ),
        }
        output_paths = {
            "cf_indicator_input_frame": (output_dir / "cf_indicator_input_frame.delta").as_posix(),
            "continuous_front_indicator_frames": (
                output_dir / "continuous_front_indicator_frames.delta"
            ).as_posix(),
        }
        if include_derived:
            derived_filters = _scope_filters(
                dataset_version=dataset_version,
                roll_policy_version=roll_policy_version,
                adjustment_policy_version=adjustment_policy_version,
                indicator_set_version=indicator_set_version,
                derived_set_version=derived_set_version,
                rule_set_version=rule_set_version,
            )
            rows_by_table["continuous_front_derived_indicator_frames"] = count_delta_table_rows(
                output_dir / "continuous_front_derived_indicator_frames.delta",
                filters=derived_filters,
            )
            output_paths["continuous_front_derived_indicator_frames"] = (
                output_dir / "continuous_front_derived_indicator_frames.delta"
            ).as_posix()
        record_stage_timing(
            stage_timings,
            "row_counts",
            stage_started,
            table_count=len(rows_by_table),
            row_count=sum(int(value) for value in rows_by_table.values()),
        )
        return {
            "success": True,
            "status": "PASS",
            "dataset_version": dataset_version,
            "contour_id": contour_id,
            "indicator_set_version": indicator_set_version,
            "derived_set_version": derived_set_version,
            "rule_set_version": rule_set_version,
            "include_derived": include_derived,
            "rows_by_table": rows_by_table,
            "output_paths": output_paths,
            "stage_timings": stage_timings,
            "delta_manifest": {
                name: resolved_contract[name]
                for name in (
                    "cf_indicator_input_frame",
                    "continuous_front_indicator_frames",
                    *(("continuous_front_derived_indicator_frames",) if include_derived else ()),
                )
            },
            "spark_profile": {
                "app_name": spec.app_name,
                "master": spark_master,
                "delta_reader": spec.delta_reader,
                "delta_writer": spec.delta_writer,
                "hash_compatibility": spec.hash_compatibility,
                "sql_plan": build_continuous_front_indicator_sidecar_sql_plan(),
            },
        }
    finally:
        for frame_name in ("derived_frame", "base_frame", "input_frame"):
            frame = locals().get(frame_name)
            unpersist = getattr(frame, "unpersist", None)
            if callable(unpersist):
                unpersist()
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()
