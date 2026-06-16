from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    has_delta_log,
)
from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    MOEX_CONTRACT_ECONOMICS_MODEL_VERSION,
    MOEX_DEFAULT_RADIUS_PCT,
    MOEX_FX_OR_USD_LINKED_ASSETS,
    MOEX_MARGIN_BUFFER_POLICY_VERSION,
    moex_economics_store_contract,
)

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session


@dataclass(frozen=True)
class MoexContractEconomicsSparkJobSpec:
    app_name: str = "ta3000-moex-contract-economics"
    delta_reader: str = "spark_delta"
    delta_writer: str = "spark_delta"
    model_version: str = MOEX_CONTRACT_ECONOMICS_MODEL_VERSION
    buffer_policy_version: str = MOEX_MARGIN_BUFFER_POLICY_VERSION


_CANONICAL_MERGE_KEYS = {
    "canonical_fx_rates": ("rate_date", "fx_pair", "clearing_type"),
    "canonical_asset_risk_parameters": ("assetcode", "risk_session_date"),
    "canonical_contract_economics": ("contract_id", "economics_session_date", "clearing_type"),
}


def _spark_sql_type(type_name: str) -> str:
    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return "STRING"
    if normalized == "timestamp":
        return "TIMESTAMP"
    if normalized == "date":
        return "DATE"
    if normalized == "int":
        return "INT"
    if normalized == "bigint":
        return "BIGINT"
    if normalized == "double":
        return "DOUBLE"
    if normalized == "boolean":
        return "BOOLEAN"
    raise ValueError(f"unsupported economics contract type: {type_name}")


def _column_or_json(dataframe, column_name: str, *json_names: str):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    candidates = []
    if column_name in dataframe.columns:
        candidates.append(F.col(column_name).cast("string"))
    raw_payload_json = (
        F.col("raw_payload_json") if "raw_payload_json" in dataframe.columns else F.lit("{}")
    )
    for json_name in (column_name, *json_names):
        candidates.append(F.get_json_object(raw_payload_json, f"$.{json_name}"))
    return F.coalesce(*candidates)


def _optional_column(dataframe, column_name: str, spark_type: str = "string"):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    if column_name in dataframe.columns:
        return F.col(column_name).cast(spark_type)
    return F.lit(None).cast(spark_type)


def _cast_to_contract(dataframe, table_name: str):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    selected = []
    for column_name, type_name in moex_economics_store_contract()[table_name]["columns"].items():
        spark_type = _spark_sql_type(str(type_name))
        if column_name in dataframe.columns:
            selected.append(F.col(column_name).cast(spark_type).alias(column_name))
        else:
            selected.append(F.lit(None).cast(spark_type).alias(column_name))
    return dataframe.select(*selected)


def _write_spark_delta_table(dataframe, *, table_path: Path, table_name: str) -> None:
    contract = moex_economics_store_contract()[table_name]
    casted = _cast_to_contract(dataframe, table_name)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    partition_by = list(contract.get("partition_by") or [])
    if not has_delta_log(table_path):
        writer = casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(str(table_path))
        return
    if casted.limit(1).count() == 0:
        return

    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    key_columns = _CANONICAL_MERGE_KEYS[table_name]
    condition = " AND ".join(f"target.{column} <=> source.{column}" for column in key_columns)
    (
        DeltaTable.forPath(casted.sparkSession, str(table_path))
        .alias("target")
        .merge(casted.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _json_safe_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return value


def _contract_specs_frame(spark: object, table_path: Path):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    raw = spark.read.format("delta").load(str(table_path))
    assetcode = F.upper(_column_or_json(raw, "assetcode", "asset_code", "ASSETCODE"))
    base = (
        raw.withColumn("contract_id", _column_or_json(raw, "moex_secid", "SECID", "secid"))
        .withColumn("moex_secid", F.col("contract_id"))
        .withColumn("assetcode", assetcode)
        .withColumn("instrument_id", F.concat(F.lit("FUT_"), F.col("assetcode")))
        .withColumn(
            "trade_date",
            F.to_date(
                F.coalesce(
                    _optional_column(raw, "trade_date"),
                    _column_or_json(raw, "tradedate"),
                )
            ),
        )
        .withColumn(
            "min_step",
            _column_or_json(raw, "min_step", "MINSTEP", "minstep").cast("double"),
        )
        .withColumn(
            "lot_volume",
            _column_or_json(raw, "lot_volume", "LOTVOLUME", "lotvolume").cast("double"),
        )
        .withColumn(
            "official_step_price",
            _column_or_json(raw, "official_step_price", "STEPPRICE", "stepprice").cast("double"),
        )
        .withColumn(
            "official_initial_margin",
            _column_or_json(raw, "official_initial_margin", "INITIALMARGIN", "initialmargin").cast(
                "double"
            ),
        )
        .withColumn(
            "last_settle_price",
            _column_or_json(raw, "last_settle_price", "LASTSETTLEPRICE", "lastsettleprice").cast(
                "double"
            ),
        )
        .withColumn(
            "quote_currency",
            F.upper(
                F.coalesce(
                    _column_or_json(raw, "quote_currency", "CURRENCYID", "currencyid"),
                    F.when(
                        F.col("assetcode").isin(
                            "BR",
                            "BRM",
                            "GOLD",
                            "GOLDM",
                            "GL",
                            "SILV",
                            "SILVM",
                            "NG",
                            "NGM",
                            "RTS",
                            "RTSM",
                            "SPYF",
                            "NASD",
                            "SI",
                            "ED",
                        ),
                        F.lit("USD"),
                    ).otherwise(F.lit("RUB")),
                )
            ),
        )
        .withColumn(
            "expiration_date",
            F.to_date(
                F.coalesce(
                    _optional_column(raw, "last_trade_date"),
                    _optional_column(raw, "last_del_date"),
                    _column_or_json(raw, "MATDATE", "matdate"),
                )
            ),
        )
        .withColumn("contract_source_hash", F.col("source_document_hash"))
        .where(
            F.col("contract_id").isNotNull()
            & F.col("assetcode").isNotNull()
            & F.col("trade_date").isNotNull()
        )
    )
    rank_window = Window.partitionBy("assetcode", "trade_date").orderBy(
        F.col("expiration_date").asc_nulls_last(), F.col("contract_id").asc()
    )
    return base.withColumn("maturity_rank", F.dense_rank().over(rank_window))


def _fx_rates_frame(spark: object, table_path: Path):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    raw = spark.read.format("delta").load(str(table_path))
    parsed = (
        raw.withColumn("fx_pair", F.upper(_column_or_json(raw, "fx_pair", "secid", "SECID")))
        .withColumn(
            "rate_date",
            F.to_date(
                F.coalesce(
                    _optional_column(raw, "trade_date"),
                    _column_or_json(raw, "tradedate", "TRADEDATE"),
                )
            ),
        )
        .withColumn(
            "clearing_type",
            F.lower(F.coalesce(_column_or_json(raw, "clearing_type", "clearing"), F.lit("mc"))),
        )
        .withColumn("rate_to_rub", _column_or_json(raw, "rate", "RATE").cast("double"))
        .withColumn("base_currency", F.split(F.col("fx_pair"), "/").getItem(0))
        .withColumn("quote_currency", F.lit("RUB"))
        .withColumn("source_id", F.col("source_id"))
        .withColumn("source_document_hash", F.col("source_document_hash"))
        .withColumn("model_version", F.lit(MOEX_CONTRACT_ECONOMICS_MODEL_VERSION))
        .withColumn("created_at", F.current_timestamp())
        .where(
            F.col("base_currency").isNotNull()
            & F.col("rate_date").isNotNull()
            & F.col("rate_to_rub").isNotNull()
        )
    )
    rub_rates = (
        parsed.select("rate_date", "clearing_type")
        .distinct()
        .withColumn("fx_pair", F.lit("RUB/RUB"))
        .withColumn("base_currency", F.lit("RUB"))
        .withColumn("quote_currency", F.lit("RUB"))
        .withColumn("rate_to_rub", F.lit(1.0))
        .withColumn("source_id", F.lit("policy_identity_rate"))
        .withColumn("source_document_hash", F.lit("policy_identity_rate"))
        .withColumn("model_version", F.lit(MOEX_CONTRACT_ECONOMICS_MODEL_VERSION))
        .withColumn("created_at", F.current_timestamp())
    )
    return parsed.select(
        "rate_date",
        "fx_pair",
        "base_currency",
        "quote_currency",
        "clearing_type",
        "rate_to_rub",
        "source_id",
        "source_document_hash",
        "model_version",
        "created_at",
    ).unionByName(rub_rates)


def _asset_risk_parameters_frame(spark: object, limits_path: Path, staticparams_path: Path):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    limits_raw = spark.read.format("delta").load(str(limits_path))
    static_raw = spark.read.format("delta").load(str(staticparams_path))
    limits = (
        limits_raw.withColumn(
            "assetcode",
            F.upper(_column_or_json(limits_raw, "assetcode", "ASSETCODE")),
        )
        .withColumn(
            "risk_session_date",
            F.to_date(
                F.coalesce(
                    _optional_column(limits_raw, "trade_date"),
                    _column_or_json(limits_raw, "tradedate", "TRADEDATE"),
                )
            ),
        )
        .withColumn("mr1", _column_or_json(limits_raw, "mr1", "MR1").cast("double"))
        .withColumn("mr2", _column_or_json(limits_raw, "mr2", "MR2").cast("double"))
        .withColumn("mr3", _column_or_json(limits_raw, "mr3", "MR3").cast("double"))
        .withColumn("source_limits_hash", F.col("source_document_hash"))
        .where(F.col("assetcode").isNotNull() & F.col("risk_session_date").isNotNull())
    )
    static = (
        static_raw.withColumn(
            "assetcode",
            F.upper(_column_or_json(static_raw, "assetcode", "ASSETCODE")),
        )
        .withColumn(
            "risk_session_date",
            F.to_date(
                F.coalesce(
                    _optional_column(static_raw, "trade_date"),
                    _column_or_json(static_raw, "tradedate", "TRADEDATE"),
                )
            ),
        )
        .withColumn(
            "source_radius_pct",
            _column_or_json(static_raw, "radius_pct", "radius", "RADIUS").cast("double"),
        )
        .withColumn("source_staticparams_hash", F.col("source_document_hash"))
        .where(F.col("assetcode").isNotNull() & F.col("risk_session_date").isNotNull())
    )
    joined = limits.alias("limits").join(
        static.alias("static"),
        (F.col("limits.assetcode") == F.col("static.assetcode"))
        & (F.col("limits.risk_session_date") == F.col("static.risk_session_date")),
        "full",
    )
    return joined.select(
        F.coalesce(F.col("limits.assetcode"), F.col("static.assetcode")).alias("assetcode"),
        F.coalesce(F.col("limits.risk_session_date"), F.col("static.risk_session_date")).alias(
            "risk_session_date"
        ),
        F.col("limits.mr1").alias("mr1"),
        F.col("limits.mr2").alias("mr2"),
        F.col("limits.mr3").alias("mr3"),
        F.coalesce(F.col("static.source_radius_pct"), F.lit(MOEX_DEFAULT_RADIUS_PCT)).alias(
            "radius_pct"
        ),
        F.when(F.col("static.source_radius_pct").isNull(), F.lit("policy_default"))
        .otherwise(F.lit("source"))
        .alias("radius_source"),
        F.col("limits.source_limits_hash").alias("source_limits_hash"),
        F.col("static.source_staticparams_hash").alias("source_staticparams_hash"),
        F.lit(MOEX_CONTRACT_ECONOMICS_MODEL_VERSION).alias("model_version"),
        F.current_timestamp().alias("created_at"),
    )


def _with_effective_sessions(contract_specs, *, canonical_session_calendar_path: Path | None):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    fallback_session_date = F.date_add(F.col("trade_date"), 1)
    if canonical_session_calendar_path is None:
        return (
            contract_specs.withColumn("effective_session_date", fallback_session_date)
            .withColumn(
                "effective_from_ts",
                F.to_timestamp(
                    F.concat(F.col("effective_session_date").cast("string"), F.lit(" 00:00:00"))
                ),
            )
            .withColumn("effective_session_source", F.lit("calendar_day_fallback"))
        )

    spark = contract_specs.sparkSession
    calendar = (
        spark.read.format("delta")
        .load(str(canonical_session_calendar_path))
        .select(
            F.col("instrument_id").alias("calendar_instrument_id"),
            F.col("session_date").alias("calendar_session_date"),
            F.col("session_open_ts").alias("calendar_session_open_ts"),
        )
        .groupBy("calendar_instrument_id", "calendar_session_date")
        .agg(F.min("calendar_session_open_ts").alias("calendar_session_open_ts"))
    )
    source_dates = contract_specs.select("instrument_id", "trade_date").distinct()
    session_window = Window.partitionBy(
        "instrument_id",
        "trade_date",
    ).orderBy(F.col("calendar_session_date").asc())
    next_sessions = (
        source_dates.join(
            calendar,
            (F.col("instrument_id") == F.col("calendar_instrument_id"))
            & (F.col("calendar_session_date") > F.col("trade_date")),
            "left",
        )
        .withColumn("__session_rank", F.row_number().over(session_window))
        .where(F.col("__session_rank") == F.lit(1))
        .select(
            F.col("instrument_id").alias("__session_instrument_id"),
            F.col("trade_date").alias("__source_trade_date"),
            F.col("calendar_session_date").alias("__effective_session_date"),
            F.col("calendar_session_open_ts").alias("__effective_from_ts"),
        )
    )
    return (
        contract_specs.join(
            next_sessions,
            (F.col("instrument_id") == F.col("__session_instrument_id"))
            & (F.col("trade_date") == F.col("__source_trade_date")),
            "left",
        )
        .withColumn(
            "effective_session_date",
            F.coalesce(F.col("__effective_session_date"), fallback_session_date),
        )
        .withColumn(
            "effective_from_ts",
            F.coalesce(
                F.col("__effective_from_ts"),
                F.to_timestamp(
                    F.concat(F.col("effective_session_date").cast("string"), F.lit(" 00:00:00"))
                ),
            ),
        )
        .withColumn(
            "effective_session_source",
            F.when(
                F.col("__effective_session_date").isNotNull(), F.lit("canonical_session_calendar")
            ).otherwise(F.lit("calendar_day_fallback")),
        )
        .drop(
            "__session_instrument_id",
            "__source_trade_date",
            "__effective_session_date",
            "__effective_from_ts",
        )
    )


def _economics_frame(
    contract_specs,
    fx_rates,
    risk_parameters,
    *,
    canonical_session_calendar_path: Path | None = None,
):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    contract_specs = _with_effective_sessions(
        contract_specs,
        canonical_session_calendar_path=canonical_session_calendar_path,
    )
    fx_priority = (
        F.when(F.col("clearing_type") == F.lit("tc"), F.lit(1))
        .when(F.col("clearing_type") == F.lit("mc"), F.lit(2))
        .otherwise(F.lit(3))
    )
    fx_for_economics = (
        fx_rates.withColumn("__fx_priority", fx_priority)
        .withColumn(
            "__fx_rank",
            F.row_number().over(
                Window.partitionBy("base_currency", "rate_date").orderBy(
                    F.col("__fx_priority").asc(), F.col("clearing_type").asc()
                )
            ),
        )
        .where(F.col("__fx_rank") == F.lit(1))
        .drop("__fx_priority", "__fx_rank")
    )
    joined = (
        contract_specs.alias("spec")
        .join(
            risk_parameters.alias("risk"),
            (F.col("spec.assetcode") == F.col("risk.assetcode"))
            & (F.col("spec.trade_date") == F.col("risk.risk_session_date")),
            "left",
        )
        .join(
            fx_for_economics.alias("fx"),
            (F.col("spec.quote_currency") == F.col("fx.base_currency"))
            & (F.col("spec.trade_date") == F.col("fx.rate_date")),
            "left",
        )
    )
    tick_value_currency = F.col("spec.min_step") * F.col("spec.lot_volume")
    step_price_rub = tick_value_currency * F.col("fx.rate_to_rub")
    margin_formula_base = (
        F.col("spec.last_settle_price")
        * (step_price_rub / F.col("spec.min_step"))
        * F.col("risk.mr1")
    )
    margin_radius_adjusted = margin_formula_base * (
        F.lit(1.0) + F.col("risk.radius_pct") / F.lit(100.0)
    )
    margin_required_no_buffer = F.greatest(
        F.coalesce(F.col("spec.official_initial_margin"), F.lit(0.0)),
        margin_radius_adjusted,
    )
    days_to_expiry = F.datediff(F.col("spec.expiration_date"), F.col("spec.trade_date"))
    far_contract = (F.col("spec.maturity_rank") >= F.lit(3)) | (days_to_expiry > F.lit(120))
    linked_assetcodes = sorted(MOEX_FX_OR_USD_LINKED_ASSETS)
    usd_linked = (F.upper(F.col("spec.quote_currency")) != F.lit("RUB")) | F.upper(
        F.col("spec.assetcode")
    ).isin(linked_assetcodes)
    margin_buffer_pct = (
        F.when(far_contract, F.lit(0.30)).when(usd_linked, F.lit(0.05)).otherwise(F.lit(0.01))
    )
    base = (
        joined.withColumn("tick_value_currency", tick_value_currency)
        .withColumn("step_price_rub", step_price_rub)
        .withColumn("margin_formula_base", margin_formula_base)
        .withColumn("margin_radius_adjusted", margin_radius_adjusted)
        .withColumn("margin_required_no_buffer", margin_required_no_buffer)
        .withColumn("margin_buffer_pct", margin_buffer_pct)
        .withColumn("days_to_expiry", days_to_expiry.cast("int"))
        .withColumn(
            "source_flags_json",
            F.to_json(
                F.struct(
                    F.col("risk.radius_source").alias("radius_source"),
                    F.col("fx.fx_pair").alias("fx_pair"),
                    F.col("spec.effective_session_source").alias("effective_session_source"),
                )
            ),
        )
        .withColumn(
            "source_document_hashes_json",
            F.to_json(
                F.struct(
                    F.col("spec.contract_source_hash").alias("contract"),
                    F.col("fx.source_document_hash").alias("fx"),
                    F.col("risk.source_limits_hash").alias("limits"),
                    F.col("risk.source_staticparams_hash").alias("staticparams"),
                )
            ),
        )
        .withColumn("__contract_id_for_interval", F.col("spec.contract_id"))
        .withColumn("__trade_date_for_interval", F.col("spec.trade_date"))
    )
    interval_window = Window.partitionBy("__contract_id_for_interval").orderBy(
        F.col("effective_from_ts").asc(), F.col("__trade_date_for_interval").asc()
    )
    return base.withColumn(
        "effective_to_ts", F.lead("effective_from_ts").over(interval_window)
    ).select(
        F.col("spec.contract_id").alias("contract_id"),
        F.col("spec.instrument_id").alias("instrument_id"),
        F.col("spec.moex_secid").alias("moex_secid"),
        F.col("spec.assetcode").alias("assetcode"),
        F.col("spec.trade_date").alias("economics_session_date"),
        F.col("spec.effective_session_date").alias("effective_session_date"),
        F.coalesce(F.col("fx.clearing_type"), F.lit("mc")).alias("clearing_type"),
        "effective_from_ts",
        "effective_to_ts",
        F.col("spec.min_step").alias("min_step"),
        F.col("spec.lot_volume").alias("lot_volume"),
        F.col("spec.quote_currency").alias("quote_currency"),
        F.col("fx.rate_to_rub").alias("fx_rate_to_rub"),
        "tick_value_currency",
        "step_price_rub",
        F.col("spec.official_step_price").alias("official_step_price"),
        F.col("spec.official_initial_margin").alias("official_initial_margin"),
        F.col("spec.last_settle_price").alias("last_settle_price"),
        F.col("risk.mr1").alias("mr1"),
        F.col("risk.radius_pct").alias("radius_pct"),
        F.col("risk.radius_source").alias("radius_source"),
        "margin_formula_base",
        "margin_radius_adjusted",
        "margin_required_no_buffer",
        "margin_buffer_pct",
        (F.col("margin_required_no_buffer") * (F.lit(1.0) + F.col("margin_buffer_pct"))).alias(
            "margin_required_estimate"
        ),
        F.col("spec.maturity_rank").cast("int").alias("maturity_rank"),
        F.col("days_to_expiry").alias("days_to_expiry"),
        F.col("spec.expiration_date").alias("expiration_date"),
        F.lit(MOEX_CONTRACT_ECONOMICS_MODEL_VERSION).alias("model_version"),
        F.lit(MOEX_MARGIN_BUFFER_POLICY_VERSION).alias("buffer_policy_version"),
        F.lit("estimated").alias("model_quality"),
        "source_flags_json",
        "source_document_hashes_json",
        F.current_timestamp().alias("created_at"),
    )


def _fail_closed_on_missing_inputs(economics) -> dict[str, int]:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    missing = economics.agg(
        F.sum(F.when(F.col("min_step").isNull() | (F.col("min_step") <= 0), 1).otherwise(0)).alias(
            "MINSTEP"
        ),
        F.sum(
            F.when(F.col("lot_volume").isNull() | (F.col("lot_volume") <= 0), 1).otherwise(0)
        ).alias("LOTVOLUME"),
        F.sum(
            F.when(
                F.col("fx_rate_to_rub").isNull() | (F.col("fx_rate_to_rub") <= 0),
                1,
            ).otherwise(0)
        ).alias("FX"),
        F.sum(F.when(F.col("mr1").isNull() | (F.col("mr1") <= 0), 1).otherwise(0)).alias("MR1"),
        F.sum(
            F.when(
                F.col("last_settle_price").isNull() | (F.col("last_settle_price") <= 0),
                1,
            ).otherwise(0)
        ).alias("LASTSETTLEPRICE"),
    ).collect()[0]
    missing_counts = {key: int(missing[key] or 0) for key in missing.asDict()}
    offenders = [f"{key}={value}" for key, value in missing_counts.items() if value]
    if offenders:
        raise RuntimeError(
            "MOEX contract economics missing required inputs: " + ", ".join(offenders)
        )
    return missing_counts


def run_moex_contract_economics_spark_job(
    *,
    raw_contract_specs_path: Path,
    raw_fx_rates_path: Path,
    raw_rms_limits_path: Path | None = None,
    raw_rms_staticparams_path: Path | None = None,
    raw_asset_risk_parameters_path: Path | None = None,
    output_dir: Path,
    canonical_session_calendar_path: Path | None = None,
    run_id: str = "contract-economics",
    report_path: Path | None = None,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    if raw_asset_risk_parameters_path is not None and (
        raw_rms_limits_path is None or raw_rms_staticparams_path is None
    ):
        raw_rms_limits_path = raw_asset_risk_parameters_path
        raw_rms_staticparams_path = raw_asset_risk_parameters_path
    if raw_rms_limits_path is None or raw_rms_staticparams_path is None:
        raise ValueError(
            "MOEX contract economics Spark job requires raw_rms_limits_path and "
            "raw_rms_staticparams_path"
        )
    for table_path in (
        raw_contract_specs_path,
        raw_fx_rates_path,
        raw_rms_limits_path,
        raw_rms_staticparams_path,
    ):
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing MOEX economics raw Delta table: {table_path.as_posix()}")
    if canonical_session_calendar_path is not None and not has_delta_log(
        canonical_session_calendar_path
    ):
        raise RuntimeError(
            "missing canonical session calendar Delta table: "
            f"{canonical_session_calendar_path.as_posix()}"
        )

    spec = MoexContractEconomicsSparkJobSpec()
    spark_factory = spark_session_factory or _create_spark_session
    spark = spark_factory(spec.app_name, spark_master)
    try:
        contract_specs = _contract_specs_frame(spark, raw_contract_specs_path)
        fx_rates = _fx_rates_frame(spark, raw_fx_rates_path)
        risk_parameters = _asset_risk_parameters_frame(
            spark,
            raw_rms_limits_path,
            raw_rms_staticparams_path,
        )
        economics = _economics_frame(
            contract_specs,
            fx_rates,
            risk_parameters,
            canonical_session_calendar_path=canonical_session_calendar_path,
        )
        missing_counts = _fail_closed_on_missing_inputs(economics)

        output_dir.mkdir(parents=True, exist_ok=True)
        output_paths = {
            "canonical_fx_rates": output_dir / "canonical_fx_rates.delta",
            "canonical_asset_risk_parameters": output_dir / "canonical_asset_risk_parameters.delta",
            "canonical_contract_economics": output_dir / "canonical_contract_economics.delta",
        }
        _write_spark_delta_table(
            fx_rates,
            table_path=output_paths["canonical_fx_rates"],
            table_name="canonical_fx_rates",
        )
        _write_spark_delta_table(
            risk_parameters,
            table_path=output_paths["canonical_asset_risk_parameters"],
            table_name="canonical_asset_risk_parameters",
        )
        _write_spark_delta_table(
            economics,
            table_path=output_paths["canonical_contract_economics"],
            table_name="canonical_contract_economics",
        )

        from pyspark.sql import functions as F  # type: ignore[import-not-found]

        row_counts = {
            table_name: count_delta_table_rows(table_path)
            for table_name, table_path in output_paths.items()
        }
        defaulted_radius_rows = int(
            economics.where(F.col("radius_source") == "policy_default").count()
        )
        official_rows = int(
            economics.where(
                F.col("official_initial_margin") >= F.col("margin_radius_adjusted")
            ).count()
        )
        formula_rows = int(economics.count()) - official_rows
        affected_rows = [
            {key: _json_safe_value(value) for key, value in row.asDict().items()}
            for row in economics.select("instrument_id", "economics_session_date")
            .distinct()
            .orderBy("instrument_id", "economics_session_date")
            .collect()
        ]
        report = {
            "status": "PASS",
            "mode": "moex_contract_economics_spark_job",
            "run_id": run_id,
            "row_counts": row_counts,
            "missing_economics_rows": 0,
            "missing_required_input_counts": missing_counts,
            "defaulted_radius_rows": defaulted_radius_rows,
            "official_margin_dominates_rows": official_rows,
            "formula_margin_dominates_rows": formula_rows,
            "affected_downstream_partitions": affected_rows,
            "canonical_session_calendar_path": canonical_session_calendar_path.as_posix()
            if canonical_session_calendar_path is not None
            else "",
            "output_paths": {
                table_name: table_path.as_posix() for table_name, table_path in output_paths.items()
            },
            "runtime_profile": {
                "app_name": spec.app_name,
                "master": spark_master,
                "transform_runtime": "spark_sql_delta",
                "delta_reader": spec.delta_reader,
                "delta_writer": spec.delta_writer,
                "model_version": spec.model_version,
                "buffer_policy_version": spec.buffer_policy_version,
            },
        }
        if report_path is not None:
            report["report_path"] = report_path.as_posix()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        return report
    finally:
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()
