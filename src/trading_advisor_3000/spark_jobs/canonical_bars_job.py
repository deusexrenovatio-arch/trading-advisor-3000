"""Phase2a Spark proof job.

This module remains useful as a tested Spark pattern, but it is not the
authoritative MOEX historical refresh job. The accepted route is documented in
docs/architecture/product-plane/moex-historical-route-decision.md.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    has_delta_log,
    read_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.schemas import (
    historical_data_delta_schema_manifest,
)

DEFAULT_SPARK_MASTER = "local[2]"
DEFAULT_SPARK_RUNTIME_ROOT = "/tmp/ta3000-spark-runtime"
SPARK_DRIVER_MEMORY_ENV = "TA3000_SPARK_DRIVER_MEMORY"
SPARK_EXECUTOR_MEMORY_ENV = "TA3000_SPARK_EXECUTOR_MEMORY"
SPARK_DRIVER_MAX_RESULT_SIZE_ENV = "TA3000_SPARK_DRIVER_MAX_RESULT_SIZE"
SPARK_SQL_SHUFFLE_PARTITIONS_ENV = "TA3000_SPARK_SQL_SHUFFLE_PARTITIONS"
SPARK_CONFIG_ENV_OVERRIDES = {
    SPARK_DRIVER_MEMORY_ENV: "spark.driver.memory",
    SPARK_EXECUTOR_MEMORY_ENV: "spark.executor.memory",
    SPARK_DRIVER_MAX_RESULT_SIZE_ENV: "spark.driver.maxResultSize",
    SPARK_SQL_SHUFFLE_PARTITIONS_ENV: "spark.sql.shuffle.partitions",
}


@dataclass(frozen=True)
class SparkJobSpec:
    app_name: str
    source_table: str
    source_session_intervals_table: str
    target_bars_table: str
    target_instruments_table: str
    target_contracts_table: str
    target_session_calendar_table: str
    target_roll_map_table: str


def default_spec() -> SparkJobSpec:
    return SparkJobSpec(
        app_name="ta3000-historical-data-canonical-bars",
        source_table="raw_market_backfill",
        source_session_intervals_table="canonical_session_intervals",
        target_bars_table="canonical_bars",
        target_instruments_table="canonical_instruments",
        target_contracts_table="canonical_contracts",
        target_session_calendar_table="canonical_session_calendar",
        target_roll_map_table="canonical_roll_map",
    )


def build_bars_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_bars_table}
SELECT contract_id, instrument_id, timeframe, ts_open AS ts,
       open, high, low, close, volume, open_interest
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY contract_id, timeframe, ts_open
               ORDER BY ts_close DESC
           ) AS rn
    FROM {spec.source_table}
) source
WHERE rn = 1
""".strip()


def build_instruments_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_instruments_table}
SELECT DISTINCT instrument_id
FROM {spec.source_table}
""".strip()


def build_contracts_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_contracts_table}
SELECT
  contract_id,
  MIN(instrument_id) AS instrument_id,
  MIN(ts_open) AS first_seen_ts,
  MAX(ts_close) AS last_seen_ts
FROM {spec.source_table}
GROUP BY contract_id
""".strip()


def build_session_calendar_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_session_calendar_table}
WITH session_timeframes AS (
  SELECT DISTINCT instrument_id, timeframe, to_date(ts_open) AS session_date
  FROM {spec.source_table}
)
SELECT
  scope.instrument_id,
  scope.timeframe,
  scope.session_date,
  MIN(intervals.expected_open_ts) AS session_open_ts,
  MAX(intervals.expected_close_ts) AS session_close_ts,
  MIN(intervals.session_class) AS session_class
FROM session_timeframes scope
JOIN {spec.source_session_intervals_table} intervals
  ON scope.instrument_id = intervals.instrument_id
 AND scope.session_date = intervals.session_date
GROUP BY scope.instrument_id, scope.timeframe, scope.session_date
""".strip()


def build_roll_map_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_roll_map_table}
SELECT
  instrument_id,
  session_date,
  contract_id AS active_contract_id,
  'max_open_interest_then_latest_ts_close' AS reason
FROM (
  SELECT
    instrument_id,
    contract_id,
    to_date(ts_open) AS session_date,
    open_interest,
    ts_close,
    ROW_NUMBER() OVER (
      PARTITION BY instrument_id, to_date(ts_open)
      ORDER BY open_interest DESC, ts_close DESC
    ) AS rn
  FROM {spec.source_table}
) ranked
WHERE rn = 1
""".strip()


def build_sql_plan(spec: SparkJobSpec | None = None) -> str:
    return build_bars_sql_plan(spec)


def _ensure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    for candidate in (
        Path("/usr/lib/jvm/default-java"),
        Path("/usr/lib/jvm/java-21-openjdk-amd64"),
        Path("/usr/lib/jvm/java-17-openjdk-amd64"),
    ):
        if candidate.exists():
            os.environ["JAVA_HOME"] = str(candidate)
            return
    if shutil.which("java"):
        return
    try:
        import jdk4py  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in runtime env
        raise RuntimeError(
            "JAVA_HOME is not set and `jdk4py` is not installed. "
            "Install Spark runtime dependencies (`pip install -e .[spark,dev]`) "
            "or set JAVA_HOME explicitly."
        ) from exc
    os.environ["JAVA_HOME"] = str(jdk4py.JAVA_HOME)


def _ensure_hadoop_home_bin_on_path() -> None:
    hadoop_home = os.environ.get("HADOOP_HOME", "").strip()
    if not hadoop_home:
        return

    hadoop_bin = Path(hadoop_home) / "bin"
    if not hadoop_bin.is_dir():
        return

    path_value = os.environ.get("PATH", "")
    path_entries = [entry for entry in path_value.split(os.pathsep) if entry]
    normalized_entries = {os.path.normcase(os.path.normpath(entry)) for entry in path_entries}
    hadoop_bin_text = str(hadoop_bin)
    normalized_hadoop_bin = os.path.normcase(os.path.normpath(hadoop_bin_text))
    if normalized_hadoop_bin in normalized_entries:
        return

    os.environ["PATH"] = (
        hadoop_bin_text if not path_value else hadoop_bin_text + os.pathsep + path_value
    )


def _load_spark_modules() -> tuple[Any, Any, Any, Any]:
    try:
        from pyspark.sql import SparkSession, Window  # type: ignore[import-not-found]
        from pyspark.sql import functions as F
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in runtime env
        raise RuntimeError(
            "Spark runtime is unavailable: install `pyspark` and rerun the phase-04 Spark proof."
        ) from exc
    try:
        from delta import configure_spark_with_delta_pip  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in runtime env
        raise RuntimeError(
            "Spark Delta writer is unavailable: install `delta-spark` or use the "
            "Docker/Linux proof profile."
        ) from exc
    return SparkSession, Window, F, configure_spark_with_delta_pip


def _spark_runtime_dirs() -> dict[str, str]:
    runtime_root = os.environ.get("TA3000_SPARK_RUNTIME_ROOT", "").strip()
    if not runtime_root:
        return {}

    runtime_path = Path(runtime_root)
    ivy_path = runtime_path / ".ivy2"
    local_dir_path = runtime_path / "local"
    ivy_path.mkdir(parents=True, exist_ok=True)
    local_dir_path.mkdir(parents=True, exist_ok=True)
    return {
        "runtime_root": runtime_path.as_posix(),
        "ivy": ivy_path.as_posix(),
        "local_dir": local_dir_path.as_posix(),
    }


def _spark_config_overrides_from_env() -> dict[str, str]:
    overrides: dict[str, str] = {}
    for env_name, spark_config_key in SPARK_CONFIG_ENV_OVERRIDES.items():
        value = os.environ.get(env_name, "").strip()
        if value:
            overrides[spark_config_key] = value
    return overrides


def _create_spark_session(app_name: str, master: str) -> Any:
    _ensure_java_home()
    _ensure_hadoop_home_bin_on_path()
    spark_session, _, _, configure = _load_spark_modules()
    runtime_dirs = _spark_runtime_dirs()
    builder = (
        spark_session.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    if runtime_dirs:
        builder = builder.config("spark.jars.ivy", runtime_dirs["ivy"]).config(
            "spark.local.dir", runtime_dirs["local_dir"]
        )
    for spark_config_key, value in _spark_config_overrides_from_env().items():
        builder = builder.config(spark_config_key, value)
    return configure(builder).getOrCreate()


def _write_delta_dataframe(
    *,
    dataframe: Any,
    table_path: Path,
    manifest_entry: dict[str, object],
) -> None:
    partition_by = list(manifest_entry.get("partition_by") or [])
    target_file_count = int(manifest_entry.get("target_file_count") or 0)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    if table_path.exists():
        shutil.rmtree(table_path)
    if target_file_count > 0:
        dataframe = dataframe.coalesce(target_file_count)
    writer = dataframe.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(table_path))


def validate_spark_output_contract(
    *,
    output_paths: dict[str, str],
    delta_schema_manifest: dict[str, dict[str, object]],
) -> list[str]:
    errors: list[str] = []
    for table_name, path_text in output_paths.items():
        path = Path(path_text)
        if table_name not in delta_schema_manifest:
            errors.append(f"missing schema manifest entry for `{table_name}`")
            continue
        if not has_delta_log(path):
            errors.append(f"missing `_delta_log` for `{table_name}` at {path.as_posix()}")
            continue
        expected_columns = set((delta_schema_manifest[table_name].get("columns") or {}).keys())
        row_count = count_delta_table_rows(path)
        for row_index, row in enumerate(read_delta_table_rows(path, limit=row_count), start=1):
            keys = set(row.keys())
            missing = sorted(expected_columns - keys)
            extra = sorted(keys - expected_columns)
            if missing:
                errors.append(f"{table_name} row {row_index} missing columns: {', '.join(missing)}")
            if extra:
                errors.append(
                    f"{table_name} row {row_index} has unsupported columns: {', '.join(extra)}"
                )
    return errors


def run_canonical_bars_spark_job(
    *,
    source_path: Path,
    output_dir: Path,
    whitelist_contracts: set[str],
    spec: SparkJobSpec | None = None,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    spec = spec or default_spec()
    manifest = historical_data_delta_schema_manifest()
    output_dir.mkdir(parents=True, exist_ok=True)

    spark_session_factory = spark_session_factory or _create_spark_session
    spark = spark_session_factory(spec.app_name, spark_master)
    _, window, F, _ = _load_spark_modules()

    try:
        source_df = spark.read.json(str(source_path))
        source_rows = int(source_df.count())

        if whitelist_contracts:
            source_df = source_df.where(F.col("contract_id").isin(sorted(whitelist_contracts)))
        whitelisted_rows = int(source_df.count())

        raw_columns = list((manifest["raw_market_backfill"].get("columns") or {}).keys())
        source_df = source_df.select(*raw_columns)

        bars_window = window.partitionBy("contract_id", "timeframe", "ts_open").orderBy(
            F.col("ts_close").desc()
        )
        dedup_df = (
            source_df.withColumn("_rn", F.row_number().over(bars_window))
            .where(F.col("_rn") == 1)
            .drop("_rn")
        )

        bars_df = dedup_df.select(
            "contract_id",
            "instrument_id",
            "timeframe",
            F.col("ts_open").alias("ts"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
        ).orderBy("contract_id", "timeframe", "ts")

        instruments_df = source_df.select("instrument_id").distinct().orderBy("instrument_id")

        contracts_df = (
            source_df.groupBy("contract_id")
            .agg(
                F.min("instrument_id").alias("instrument_id"),
                F.min("ts_open").alias("first_seen_ts"),
                F.max("ts_close").alias("last_seen_ts"),
            )
            .orderBy("contract_id")
        )

        session_intervals_df = (
            source_df.select(
                "instrument_id",
                F.to_date("ts_open").alias("session_date"),
            )
            .distinct()
            .select(
                "instrument_id",
                "session_date",
                F.concat_ws(
                    "-",
                    F.col("instrument_id"),
                    F.date_format(F.col("session_date"), "yyyy-MM-dd"),
                    F.lit("regular-1"),
                ).alias("interval_id"),
                F.lit(1).alias("interval_seq"),
                F.to_timestamp(
                    F.concat(
                        F.date_format(F.col("session_date"), "yyyy-MM-dd"),
                        F.lit("T10:00:00Z"),
                    )
                ).alias("expected_open_ts"),
                F.to_timestamp(
                    F.concat(
                        F.date_format(F.col("session_date"), "yyyy-MM-dd"),
                        F.lit("T18:45:00Z"),
                    )
                ).alias("expected_close_ts"),
                F.lit("regular").alias("session_class"),
                F.lit("regular_trading").alias("interval_type"),
                F.lit("sample-official-session-fixture-v1").alias("policy_id"),
                F.lit("sample-official-session-fixture").alias("source_id"),
                F.lit("sha256:sample-fixture").alias("source_document_hash"),
            )
            .orderBy("instrument_id", "session_date", "interval_seq")
        )

        session_calendar_df = (
            source_df.select(
                "instrument_id",
                "timeframe",
                F.to_date("ts_open").alias("session_date"),
            )
            .distinct()
            .join(
                session_intervals_df.groupBy("instrument_id", "session_date").agg(
                    F.min("expected_open_ts").alias("session_open_ts"),
                    F.max("expected_close_ts").alias("session_close_ts"),
                    F.min("session_class").alias("session_class"),
                ),
                ["instrument_id", "session_date"],
                "inner",
            )
            .orderBy("instrument_id", "timeframe", "session_date")
        )

        roll_window = window.partitionBy("instrument_id", F.to_date("ts_open")).orderBy(
            F.col("open_interest").desc(),
            F.col("ts_close").desc(),
        )
        roll_map_df = (
            source_df.withColumn("session_date", F.to_date("ts_open"))
            .withColumn("_rn", F.row_number().over(roll_window))
            .where(F.col("_rn") == 1)
            .select(
                "instrument_id",
                "session_date",
                F.col("contract_id").alias("active_contract_id"),
                F.lit("max_open_interest_then_latest_ts_close").alias("reason"),
            )
            .orderBy("instrument_id", "session_date")
        )

        output_paths = {
            "raw_market_backfill": (output_dir / "raw_market_backfill.delta").as_posix(),
            "canonical_bars": (output_dir / "canonical_bars.delta").as_posix(),
            "canonical_instruments": (output_dir / "canonical_instruments.delta").as_posix(),
            "canonical_contracts": (output_dir / "canonical_contracts.delta").as_posix(),
            "canonical_session_intervals": (
                output_dir / "canonical_session_intervals.delta"
            ).as_posix(),
            "canonical_session_calendar": (
                output_dir / "canonical_session_calendar.delta"
            ).as_posix(),
            "canonical_roll_map": (output_dir / "canonical_roll_map.delta").as_posix(),
        }
        dataframes_by_table = {
            "raw_market_backfill": source_df.orderBy(
                "contract_id", "timeframe", "ts_open", "ts_close"
            ),
            "canonical_bars": bars_df,
            "canonical_instruments": instruments_df,
            "canonical_contracts": contracts_df,
            "canonical_session_intervals": session_intervals_df,
            "canonical_session_calendar": session_calendar_df,
            "canonical_roll_map": roll_map_df,
        }
        for table_name, dataframe in dataframes_by_table.items():
            _write_delta_dataframe(
                dataframe=dataframe,
                table_path=Path(output_paths[table_name]),
                manifest_entry=dict(manifest[table_name]),
            )

        contract_errors = validate_spark_output_contract(
            output_paths=output_paths,
            delta_schema_manifest=manifest,
        )
        if contract_errors:
            raise RuntimeError(
                "Spark output contract validation failed: " + "; ".join(contract_errors)
            )

        rows_by_table = {
            table_name: count_delta_table_rows(Path(path_text))
            for table_name, path_text in output_paths.items()
        }

        return {
            "source_rows": source_rows,
            "whitelisted_rows": whitelisted_rows,
            "canonical_rows": rows_by_table["canonical_bars"],
            "output_paths": output_paths,
            "rows_by_table": rows_by_table,
            "delta_schema_manifest": manifest,
            "contract_check_errors": contract_errors,
            "spark_profile": {
                "app_name": spec.app_name,
                "master": spark_master,
                "java_home": os.environ.get("JAVA_HOME", ""),
                "delta_writer": "spark",
            },
        }
    finally:
        try:
            spark.stop()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
