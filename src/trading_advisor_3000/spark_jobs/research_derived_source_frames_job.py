from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    delta_table_columns,
    has_delta_log,
)
from trading_advisor_3000.product_plane.research.datasets import (
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
    DERIVED_SOURCE_FRAME_DELTA,
    DERIVED_SOURCE_FRAME_TABLE,
    derived_source_indicator_columns_hash,
    research_derived_source_frame_store_contract,
)
from trading_advisor_3000.product_plane.runtime.stage_timings import (
    StageTimings,
    record_stage_timing,
    stage_timer,
)

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session
from .research_bar_views_job import (
    _delta_log_hash,
    _latest_delta_version,
    _scoped_delete_condition,
)


@dataclass(frozen=True)
class ResearchDerivedSourceFramesSparkJobSpec:
    app_name: str = "ta3000-research-derived-source-frames-l2"
    delta_reader: str = "spark"
    delta_writer: str = "spark"


def build_research_derived_source_frame_sql_plan() -> str:
    return """
    research_bar_views + research_indicator_frames
      -> research_derived_source_frames
    join:
      dataset_version + contour_id + series_mode + series_id
      + contract_id + instrument_id + timeframe + ts
    write:
      Delta scoped replace by dataset_version + contour_id
      + indicator_set_version + instrument_id + timeframe
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
    return "string"


def _cast_to_source_contract(dataframe, *, source_indicator_columns: tuple[str, ...]):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    columns = research_derived_source_frame_store_contract(
        source_indicator_columns=source_indicator_columns
    )[DERIVED_SOURCE_FRAME_TABLE]["columns"]
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


def _write_source_frame_table(
    *,
    dataframe,
    table_path: Path,
    replace_scope: list[tuple[str, str, object]],
    source_indicator_columns: tuple[str, ...],
) -> None:
    contract = research_derived_source_frame_store_contract(
        source_indicator_columns=source_indicator_columns
    )[DERIVED_SOURCE_FRAME_TABLE]
    casted = _cast_to_source_contract(dataframe, source_indicator_columns=source_indicator_columns)
    partition_by = list(contract.get("partition_by") or [])
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

    delta_table = DeltaTable.forPath(dataframe.sparkSession, str(table_path))
    delta_table.delete(_scoped_delete_condition(replace_scope))
    casted.write.format("delta").mode("append").option("mergeSchema", "true").save(str(table_path))


def _scope_filters(
    *,
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str | None = None,
    timeframes: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
) -> list[tuple[str, str, object]]:
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
    ]
    if indicator_set_version is not None:
        filters.append(("indicator_set_version", "=", indicator_set_version))
    scoped_timeframes = tuple(str(item).strip() for item in timeframes if str(item).strip())
    if scoped_timeframes:
        filters.append(("timeframe", "in", scoped_timeframes))
    scoped_instruments = tuple(
        str(item).strip() for item in dataset_instrument_ids if str(item).strip()
    )
    if scoped_instruments:
        filters.append(("instrument_id", "in", scoped_instruments))
    return filters


def run_research_derived_source_frames_spark_job(
    *,
    bar_views_path: Path,
    indicator_frames_path: Path,
    output_dir: Path,
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str,
    derived_profile_version: str,
    source_indicator_columns: tuple[str, ...],
    spark_master: str = DEFAULT_SPARK_MASTER,
    timeframes: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    stage_timings: StageTimings = {}
    stage_started = stage_timer()
    for table_path in (bar_views_path, indicator_frames_path):
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing Delta table: {table_path.as_posix()}")
    record_stage_timing(stage_timings, "validate_inputs", stage_started)

    stage_started = stage_timer()
    available_indicator_columns = set(delta_table_columns(indicator_frames_path))
    missing_indicator_columns = tuple(
        column for column in source_indicator_columns if column not in available_indicator_columns
    )
    if missing_indicator_columns:
        raise ValueError(
            "derived source-frame requires source indicator columns: "
            + ", ".join(missing_indicator_columns)
        )
    record_stage_timing(
        stage_timings,
        "source_column_check",
        stage_started,
        source_indicator_column_count=len(source_indicator_columns),
    )

    spec = ResearchDerivedSourceFramesSparkJobSpec()
    spark_factory = spark_session_factory or _create_spark_session
    stage_started = stage_timer()
    spark = spark_factory(spec.app_name, spark_master)
    record_stage_timing(stage_timings, "start_spark", stage_started, master=spark_master)
    output_path = output_dir / DERIVED_SOURCE_FRAME_DELTA
    try:
        from pyspark.sql import functions as F  # type: ignore[import-not-found]

        stage_started = stage_timer()
        bars = (
            spark.read.format("delta")
            .load(str(bar_views_path))
            .where(
                (F.col("dataset_version") == F.lit(dataset_version))
                & (F.col("contour_id") == F.lit(contour_id))
            )
        )
        indicators = (
            spark.read.format("delta")
            .load(str(indicator_frames_path))
            .where(
                (F.col("dataset_version") == F.lit(dataset_version))
                & (F.col("contour_id") == F.lit(contour_id))
                & (F.col("indicator_set_version") == F.lit(indicator_set_version))
            )
        )
        scoped_timeframes = tuple(str(item).strip() for item in timeframes if str(item).strip())
        scoped_instruments = tuple(
            str(item).strip() for item in dataset_instrument_ids if str(item).strip()
        )
        if scoped_timeframes:
            bars = bars.where(F.col("timeframe").isin(*scoped_timeframes))
            indicators = indicators.where(F.col("timeframe").isin(*scoped_timeframes))
        if scoped_instruments:
            bars = bars.where(F.col("instrument_id").isin(*scoped_instruments))
            indicators = indicators.where(F.col("instrument_id").isin(*scoped_instruments))
        join_keys = (
            "dataset_version",
            "contour_id",
            "series_mode",
            "series_id",
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
        )
        record_stage_timing(
            stage_timings,
            "load_scoped_sources",
            stage_started,
            timeframe_count=len(scoped_timeframes),
            instrument_count=len(scoped_instruments),
        )

        stage_started = stage_timer()
        duplicate_indicator_key_count = int(
            indicators.groupBy(*join_keys).count().where(F.col("count") > F.lit(1)).count()
        )
        if duplicate_indicator_key_count:
            raise ValueError(
                "derived source-frame requires one L1 row per join key; "
                f"duplicates={duplicate_indicator_key_count}"
            )

        indicator_projection = indicators.select(
            *[F.col(column) for column in join_keys],
            F.lit(1).alias("__indicator_present"),
            F.col("profile_version").alias("indicator_profile_version"),
            F.col("source_bars_hash").alias("indicator_source_bars_hash"),
            F.col("source_dataset_bars_hash").alias("indicator_source_dataset_bars_hash"),
            F.col("row_count").alias("indicator_row_count"),
            F.col("warmup_span").alias("indicator_warmup_span"),
            F.col("null_warmup_span").alias("indicator_null_warmup_span"),
            F.col("created_at").alias("indicator_created_at"),
            F.col("output_columns_hash").alias("indicator_output_columns_hash"),
            *[F.col(column) for column in source_indicator_columns],
        )
        joined = bars.alias("bar").join(
            indicator_projection.alias("indicator"),
            [
                F.col(f"bar.{column}").eqNullSafe(F.col(f"indicator.{column}"))
                for column in join_keys
            ],
            "left",
        )
        record_stage_timing(stage_timings, "build_join_frame", stage_started)

        stage_started = stage_timer()
        l0_row_count = int(bars.count())
        l1_row_count = int(indicators.count())
        joined_row_count = int(joined.count())
        missing_indicator_key_count = int(
            joined.where(F.col("indicator.__indicator_present").isNull()).count()
        )
        if joined_row_count != l0_row_count:
            raise ValueError(
                f"derived source-frame row count drift: l0={l0_row_count} joined={joined_row_count}"
            )
        if missing_indicator_key_count:
            raise ValueError(
                "derived source-frame requires L1 rows for every L0 key; "
                f"missing={missing_indicator_key_count}"
            )
        record_stage_timing(
            stage_timings,
            "join_quality_counts",
            stage_started,
            l0_row_count=l0_row_count,
            l1_row_count=l1_row_count,
            joined_row_count=joined_row_count,
            duplicate_indicator_key_count=duplicate_indicator_key_count,
            missing_indicator_key_count=missing_indicator_key_count,
        )

        stage_started = stage_timer()
        source_l0_delta_version = _latest_delta_version(bar_views_path)
        source_l1_delta_version = _latest_delta_version(indicator_frames_path)
        source_l0_delta_hash = _delta_log_hash(bar_views_path)
        source_l1_delta_hash = _delta_log_hash(indicator_frames_path)
        source_indicator_columns_hash = derived_source_indicator_columns_hash(
            source_indicator_columns
        )
        record_stage_timing(
            stage_timings,
            "source_fingerprint",
            stage_started,
            source_count=2,
        )

        stage_started = stage_timer()
        selected = joined.select(
            *[
                F.col(f"bar.{column}").alias(column)
                for column in research_dataset_store_contract()["research_bar_views"]["columns"]
            ],
            F.lit(indicator_set_version).alias("indicator_set_version"),
            F.lit(derived_profile_version).alias("derived_profile_version"),
            *[F.col(f"indicator.{column}").alias(column) for column in source_indicator_columns],
            F.col("indicator.indicator_profile_version").alias("indicator_profile_version"),
            F.col("indicator.indicator_source_bars_hash").alias("indicator_source_bars_hash"),
            F.col("indicator.indicator_source_dataset_bars_hash").alias(
                "indicator_source_dataset_bars_hash"
            ),
            F.col("indicator.indicator_row_count").alias("indicator_row_count"),
            F.col("indicator.indicator_warmup_span").alias("indicator_warmup_span"),
            F.col("indicator.indicator_null_warmup_span").alias("indicator_null_warmup_span"),
            F.col("indicator.indicator_created_at").alias("indicator_created_at"),
            F.col("indicator.indicator_output_columns_hash").alias("indicator_output_columns_hash"),
            F.lit(source_indicator_columns_hash).alias("source_indicator_columns_hash"),
            F.lit(source_l0_delta_version).cast("bigint").alias("source_l0_delta_version"),
            F.lit(source_l1_delta_version).cast("bigint").alias("source_l1_delta_version"),
            F.lit(source_l0_delta_hash).alias("source_l0_delta_hash"),
            F.lit(source_l1_delta_hash).alias("source_l1_delta_hash"),
            F.lit(l0_row_count).cast("bigint").alias("l0_row_count"),
            F.lit(l1_row_count).cast("bigint").alias("l1_row_count"),
            F.lit(joined_row_count).cast("bigint").alias("joined_row_count"),
            F.lit(duplicate_indicator_key_count)
            .cast("bigint")
            .alias("duplicate_indicator_key_count"),
            F.lit(missing_indicator_key_count).cast("bigint").alias("missing_indicator_key_count"),
            F.lit("").alias("source_bars_hash"),
            F.lit("").alias("source_indicators_hash"),
            F.current_timestamp().alias("source_frame_created_at"),
        )
        record_stage_timing(stage_timings, "select_source_frame", stage_started)

        stage_started = stage_timer()
        _write_source_frame_table(
            dataframe=selected,
            table_path=output_path,
            replace_scope=_scope_filters(
                dataset_version=dataset_version,
                contour_id=contour_id,
                indicator_set_version=indicator_set_version,
                timeframes=scoped_timeframes,
                dataset_instrument_ids=scoped_instruments,
            ),
            source_indicator_columns=source_indicator_columns,
        )
        record_stage_timing(stage_timings, "write_source_frame", stage_started)

        scoped_filters = _scope_filters(
            dataset_version=dataset_version,
            contour_id=contour_id,
            indicator_set_version=indicator_set_version,
            timeframes=scoped_timeframes,
            dataset_instrument_ids=scoped_instruments,
        )
        stage_started = stage_timer()
        rows_by_table = {
            DERIVED_SOURCE_FRAME_TABLE: count_delta_table_rows(output_path, filters=scoped_filters)
        }
        record_stage_timing(
            stage_timings,
            "row_counts",
            stage_started,
            row_count=sum(rows_by_table.values()),
        )
        return {
            "success": True,
            "status": "PASS",
            "dataset_version": dataset_version,
            "contour_id": contour_id,
            "indicator_set_version": indicator_set_version,
            "derived_profile_version": derived_profile_version,
            "timeframes": scoped_timeframes,
            "dataset_instrument_ids": scoped_instruments,
            "source_indicator_columns_hash": source_indicator_columns_hash,
            "source_delta_versions": {
                "research_bar_views": source_l0_delta_version,
                "research_indicator_frames": source_l1_delta_version,
            },
            "source_delta_hashes": {
                "research_bar_views": source_l0_delta_hash,
                "research_indicator_frames": source_l1_delta_hash,
            },
            "rows_by_table": rows_by_table,
            "row_counts": {
                "l0_row_count": l0_row_count,
                "l1_row_count": l1_row_count,
                "joined_row_count": joined_row_count,
                "duplicate_indicator_key_count": duplicate_indicator_key_count,
                "missing_indicator_key_count": missing_indicator_key_count,
            },
            "output_paths": {DERIVED_SOURCE_FRAME_TABLE: output_path.as_posix()},
            "stage_timings": stage_timings,
            "delta_manifest": research_derived_source_frame_store_contract(
                source_indicator_columns=source_indicator_columns
            ),
            "spark_profile": {
                "app_name": spec.app_name,
                "master": spark_master,
                "delta_reader": spec.delta_reader,
                "delta_writer": spec.delta_writer,
                "sql_plan": build_research_derived_source_frame_sql_plan(),
            },
        }
    finally:
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()
