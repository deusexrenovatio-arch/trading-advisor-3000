from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
import os
import shutil

from trading_advisor_3000.app.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
)
from trading_advisor_3000.app.data_plane.schemas import phase2a_delta_schema_manifest


DEFAULT_SPARK_MASTER = "local[2]"
DEFAULT_SPARK_RUNTIME_ROOT = "/tmp/ta3000-spark-runtime"


@dataclass(frozen=True)
class SparkJobSpec:
    app_name: str
    source_table: str
    target_bars_table: str
    target_instruments_table: str
    target_contracts_table: str
    target_session_calendar_table: str
    target_roll_map_table: str


def default_spec() -> SparkJobSpec:
    return SparkJobSpec(
        app_name="ta3000-phase2a-canonical-bars",
        source_table="raw_market_backfill",
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
SELECT contract_id, instrument_id, timeframe, ts_open AS ts, open, high, low, close, volume, open_interest
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
SELECT
  instrument_id,
  timeframe,
  to_date(ts_open) AS session_date,
  MIN(ts_open) AS session_open_ts,
  MAX(ts_close) AS session_close_ts
FROM {spec.source_table}
GROUP BY instrument_id, timeframe, to_date(ts_open)
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
            "Install Spark runtime dependencies (`pip install -e .[spark,dev]`) or set JAVA_HOME explicitly."
        ) from exc
    os.environ["JAVA_HOME"] = str(jdk4py.JAVA_HOME)


def _load_spark_modules() -> tuple[Any, Any, Any, Any]:
    try:
        from pyspark.sql import SparkSession, Window, functions as F  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in runtime env
        raise RuntimeError(
            "Spark runtime is unavailable: install `pyspark` and rerun the phase-04 Spark proof."
        ) from exc
    try:
        from delta import configure_spark_with_delta_pip  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in runtime env
        raise RuntimeError(
            "Spark Delta writer is unavailable: install `delta-spark` or use the Docker/Linux proof profile."
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


def _create_spark_session(app_name: str, master: str) -> Any:
    _ensure_java_home()
    spark_session, _, _, configure = _load_spark_modules()
    runtime_dirs = _spark_runtime_dirs()
    builder = (
        spark_session.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    if runtime_dirs:
        builder = (
            builder.config("spark.jars.ivy", runtime_dirs["ivy"])
            .config("spark.local.dir", runtime_dirs["local_dir"])
        )
    return configure(builder).getOrCreate()


def _write_delta_dataframe(
    *,
    dataframe: Any,
    table_path: Path,
    manifest_entry: dict[str, object],
) -> None:
    partition_by = list(manifest_entry.get("partition_by") or [])
    table_path.parent.mkdir(parents=True, exist_ok=True)
    if table_path.exists():
        shutil.rmtree(table_path)
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
        rows = read_delta_table_rows(path)
        expected_columns = set((delta_schema_manifest[table_name].get("columns") or {}).keys())
        for index, row in enumerate(rows, start=1):
            keys = set(row.keys())
            missing = sorted(expected_columns - keys)
            extra = sorted(keys - expected_columns)
            if missing:
                errors.append(f"{table_name} row {index} missing columns: {', '.join(missing)}")
            if extra:
                errors.append(f"{table_name} row {index} has unsupported columns: {', '.join(extra)}")
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
    manifest = phase2a_delta_schema_manifest()
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

        bars_window = window.partitionBy("contract_id", "timeframe", "ts_open").orderBy(F.col("ts_close").desc())
        dedup_df = source_df.withColumn("_rn", F.row_number().over(bars_window)).where(F.col("_rn") == 1).drop("_rn")

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

        contracts_df = source_df.groupBy("contract_id").agg(
            F.min("instrument_id").alias("instrument_id"),
            F.min("ts_open").alias("first_seen_ts"),
            F.max("ts_close").alias("last_seen_ts"),
        ).orderBy("contract_id")

        session_calendar_df = source_df.groupBy(
            "instrument_id",
            "timeframe",
            F.to_date("ts_open").alias("session_date"),
        ).agg(
            F.min("ts_open").alias("session_open_ts"),
            F.max("ts_close").alias("session_close_ts"),
        ).orderBy("instrument_id", "timeframe", "session_date")

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
            "canonical_session_calendar": (output_dir / "canonical_session_calendar.delta").as_posix(),
            "canonical_roll_map": (output_dir / "canonical_roll_map.delta").as_posix(),
        }
        dataframes_by_table = {
            "raw_market_backfill": source_df.orderBy("contract_id", "timeframe", "ts_open", "ts_close"),
            "canonical_bars": bars_df,
            "canonical_instruments": instruments_df,
            "canonical_contracts": contracts_df,
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
            raise RuntimeError("Spark output contract validation failed: " + "; ".join(contract_errors))

        rows_by_table = {
            table_name: len(read_delta_table_rows(Path(path_text)))
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
