from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from trading_advisor_3000.product_plane.contracts import Timeframe

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


def _count_jsonl_rows(path: Path) -> int:
    rows = 0
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows += 1
    return rows


def run_moex_canonicalization_spark_job(
    *,
    normalized_source_path: Path,
    selected_source_intervals_path: Path,
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

            joined = (
                source_df.join(
                    selected_df,
                    on=["contract_id", "instrument_id", "source_interval"],
                    how="inner",
                )
                .withColumn(
                    "bucket_seconds",
                    (
                        F.floor(
                            F.unix_timestamp(F.col("ts_open"))
                            / (F.col("target_minutes") * F.lit(60))
                        )
                        * (F.col("target_minutes") * F.lit(60))
                    ).cast("long"),
                )
                .withColumn(
                    "bucket_ts",
                    F.to_timestamp(F.from_unixtime(F.col("bucket_seconds"))),
                )
            )

            partition_columns = ["contract_id", "instrument_id", "timeframe", "bucket_ts"]
            first_window = window.partitionBy(*partition_columns).orderBy(
                F.col("ts_open").asc(),
                F.col("ts_close").asc(),
            )
            last_window = window.partitionBy(*partition_columns).orderBy(
                F.col("ts_open").desc(),
                F.col("ts_close").desc(),
            )

            annotated = (
                joined.withColumn("rn_first", F.row_number().over(first_window))
                .withColumn("rn_last", F.row_number().over(last_window))
            )

            aggregated = annotated.groupBy(*partition_columns).agg(
                F.max(F.when(F.col("rn_first") == 1, F.col("open"))).alias("open"),
                F.max(F.col("high")).alias("high"),
                F.min(F.col("low")).alias("low"),
                F.max(F.when(F.col("rn_last") == 1, F.col("close"))).alias("close"),
                F.sum(F.col("volume")).cast("long").alias("volume"),
                F.max(F.when(F.col("rn_last") == 1, F.col("open_interest"))).cast("long").alias("open_interest"),
                F.max(F.when(F.col("rn_last") == 1, F.col("source_provider"))).alias("source_provider"),
                F.max(F.when(F.col("rn_last") == 1, F.col("source_timeframe"))).alias("source_timeframe"),
                F.max(F.when(F.col("rn_last") == 1, F.col("source_interval"))).cast("int").alias("selected_source_interval"),
                F.max(F.when(F.col("rn_last") == 1, F.col("source_run_id"))).alias("source_run_id"),
                F.max(F.when(F.col("rn_last") == 1, F.col("source_ingest_run_id"))).alias("source_ingest_run_id"),
                F.count(F.lit(1)).cast("int").alias("source_row_count"),
                F.min(F.col("ts_open")).alias("source_ts_open_first"),
                F.max(F.when(F.col("rn_last") == 1, F.col("ts_close"))).alias("source_ts_close_last"),
                F.max(F.col("open_interest_imputed").cast("int")).cast("int").alias("open_interest_imputed"),
            )

            bars_df = aggregated.select(
                "contract_id",
                "instrument_id",
                "timeframe",
                F.col("bucket_ts").alias("ts"),
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
                F.col("bucket_ts").alias("ts"),
                "source_provider",
                "source_timeframe",
                F.col("selected_source_interval").alias("source_interval"),
                "source_run_id",
                "source_ingest_run_id",
                "source_row_count",
                "source_ts_open_first",
                "source_ts_close_last",
                "open_interest_imputed",
                F.lit(str(build_run_id).strip()).alias("build_run_id"),
                F.to_timestamp(F.lit(str(built_at_utc).strip())).alias("built_at_utc"),
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
            "build_run_id": str(build_run_id).strip(),
            "built_at_utc": str(built_at_utc).strip(),
            "source_rows": source_row_count,
            "selected_source_interval_rows": selected_interval_row_count,
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
