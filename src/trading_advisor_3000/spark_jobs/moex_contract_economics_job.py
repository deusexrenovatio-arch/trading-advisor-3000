from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    has_delta_log,
)
from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    MOEX_CONTRACT_ECONOMICS_MODEL_VERSION,
    MOEX_DEFAULT_RADIUS_PCT,
    MOEX_FX_OR_USD_LINKED_ASSETS,
    MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES,
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


MOEX_STEP_PRICE_RULE_VALIDATION_P95_LIMIT = 0.001


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


def _latest_snapshot_by_key(dataframe, *, key_columns: tuple[str, ...]):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    order_columns = [
        column_name
        for column_name in ("update_time", "fetched_at_utc", "source_document_hash")
        if column_name in dataframe.columns
    ]
    row_fingerprint = F.sha2(
        F.to_json(
            F.struct(
                *[
                    F.col(column_name).cast("string").alias(column_name)
                    for column_name in sorted(dataframe.columns)
                ]
            )
        ),
        256,
    )
    order_by = [F.col(column_name).desc_nulls_last() for column_name in order_columns]
    order_by.append(F.col("__snapshot_row_fingerprint").desc_nulls_last())
    snapshot_window = Window.partitionBy(*key_columns).orderBy(*order_by)
    return (
        dataframe.withColumn("__snapshot_row_fingerprint", row_fingerprint)
        .withColumn("__snapshot_rank", F.row_number().over(snapshot_window))
        .where(F.col("__snapshot_rank") == F.lit(1))
        .drop("__snapshot_rank", "__snapshot_row_fingerprint")
    )


def _assert_unique_by_key(
    dataframe,
    *,
    table_name: str,
    key_columns: tuple[str, ...],
    frame_role: str,
) -> None:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    duplicate_count_column = "__duplicate_key_count"
    duplicates = (
        dataframe.groupBy(*[F.col(column_name) for column_name in key_columns])
        .count()
        .withColumnRenamed("count", duplicate_count_column)
        .where(F.col(duplicate_count_column) > F.lit(1))
        .orderBy(*[F.col(column_name).asc_nulls_first() for column_name in key_columns])
        .limit(5)
        .collect()
    )
    if not duplicates:
        return

    samples = []
    for row in duplicates:
        payload = row.asDict()
        samples.append(
            {column_name: _json_safe_value(payload.get(column_name)) for column_name in key_columns}
            | {"count": int(payload[duplicate_count_column])}
        )
    raise RuntimeError(
        "MOEX economics "
        f"{frame_role} for {table_name} violates merge-key uniqueness "
        f"on {', '.join(key_columns)}; duplicate key samples: "
        f"{json.dumps(samples, ensure_ascii=False, sort_keys=True)}"
    )


def _write_spark_delta_table(dataframe, *, table_path: Path, table_name: str) -> None:
    contract = moex_economics_store_contract()[table_name]
    casted = _cast_to_contract(dataframe, table_name)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    partition_by = list(contract.get("partition_by") or [])
    key_columns = _CANONICAL_MERGE_KEYS[table_name]
    _assert_unique_by_key(
        casted,
        table_name=table_name,
        key_columns=key_columns,
        frame_role="source",
    )
    if not has_delta_log(table_path):
        writer = casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(str(table_path))
        return

    casted = casted.cache()
    try:
        if casted.limit(1).count() == 0:
            return

        from delta.tables import DeltaTable  # type: ignore[import-not-found]

        _assert_unique_by_key(
            casted.sparkSession.read.format("delta").load(str(table_path)),
            table_name=table_name,
            key_columns=key_columns,
            frame_role="target",
        )
        condition = " AND ".join(f"target.{column} <=> source.{column}" for column in key_columns)
        (
            DeltaTable.forPath(casted.sparkSession, str(table_path))
            .alias("target")
            .merge(casted.alias("source"), condition)
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    finally:
        casted.unpersist()


def _json_safe_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
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
            "history_value_rub",
            F.get_json_object(F.col("raw_payload_json"), "$.history.VALUE").cast("double"),
        )
        .withColumn(
            "history_volume",
            F.get_json_object(F.col("raw_payload_json"), "$.history.VOLUME").cast("double"),
        )
        .withColumn(
            "history_waprice",
            F.get_json_object(F.col("raw_payload_json"), "$.history.WAPRICE").cast("double"),
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
                        F.col("assetcode").isin(sorted(MOEX_FX_OR_USD_LINKED_ASSETS)),
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


def _contract_parameter_regimes_frame(spark: object):
    values_sql = ",\n".join(
        "("
        + ", ".join(
            (
                f"'{rule.rule_id}'",
                f"'{rule.assetcode}'",
                f"DATE '{rule.effective_from}'",
                f"DATE '{rule.effective_to}'",
                str(rule.min_step),
                str(rule.lot_volume),
                f"'{rule.quote_currency}'",
                str(rule.tick_value_quote),
                f"'{rule.source_document_id}'",
            )
        )
        + ")"
        for rule in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES
    )
    return spark.sql(
        f"""
        SELECT *
        FROM VALUES
        {values_sql}
        AS regimes(
            rule_id,
            rule_assetcode,
            rule_effective_from,
            rule_effective_to,
            rule_min_step,
            rule_lot_volume,
            rule_quote_currency,
            rule_tick_value_quote,
            rule_source_document_id
        )
        """
    )


def _fx_rates_frame(spark: object, table_path: Path):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    raw = spark.read.format("delta").load(str(table_path))
    parsed_source = (
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
        .withColumn("source_document_id", F.col("source_document_id"))
        .withColumn("source_document_hash", F.col("source_document_hash"))
        .withColumn("fetched_at_utc", _optional_column(raw, "fetched_at_utc", "timestamp"))
        .where(
            F.col("base_currency").isNotNull()
            & F.col("rate_date").isNotNull()
            & F.col("rate_to_rub").isNotNull()
        )
        .where(F.col("fx_pair") != F.lit("RUB/RUB"))
    )
    parsed = _latest_snapshot_by_key(
        parsed_source,
        key_columns=("rate_date", "fx_pair", "clearing_type"),
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
    )
    return (
        parsed.select(
            "rate_date",
            "fx_pair",
            "base_currency",
            "quote_currency",
            "clearing_type",
            "rate_to_rub",
            "source_id",
            "source_document_hash",
        )
        .unionByName(rub_rates)
        .withColumn("model_version", F.lit(MOEX_CONTRACT_ECONOMICS_MODEL_VERSION))
        .withColumn("created_at", F.current_timestamp())
    )


def _asset_risk_parameters_frame(spark: object, limits_path: Path, staticparams_path: Path):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    def _source_update_text(dataframe):
        return F.coalesce(
            _optional_column(dataframe, "update_time"),
            _column_or_json(dataframe, "updatetime", "UPDATETIME", "update_time", "UPDATE_TIME"),
        )

    def _latest_snapshot(frame, source_hash_column: str):
        snapshot_window = Window.partitionBy("assetcode", "risk_session_date").orderBy(
            F.col("__source_update_ts").desc_nulls_last(),
            F.col("__source_update_text").desc_nulls_last(),
            F.col(source_hash_column).desc_nulls_last(),
        )
        return (
            frame.withColumn("__source_row_rank", F.row_number().over(snapshot_window))
            .where(F.col("__source_row_rank") == F.lit(1))
            .drop("__source_update_text", "__source_update_ts", "__source_row_rank")
        )

    limits_raw = spark.read.format("delta").load(str(limits_path))
    static_raw = spark.read.format("delta").load(str(staticparams_path))
    limits = _latest_snapshot(
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
        .withColumn("__source_update_text", _source_update_text(limits_raw))
        .withColumn("__source_update_ts", F.to_timestamp(F.col("__source_update_text")))
        .where(F.col("assetcode").isNotNull() & F.col("risk_session_date").isNotNull()),
        "source_limits_hash",
    )
    static = _latest_snapshot(
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
        .withColumn("__source_update_text", _source_update_text(static_raw))
        .withColumn("__source_update_ts", F.to_timestamp(F.col("__source_update_text")))
        .where(F.col("assetcode").isNotNull() & F.col("risk_session_date").isNotNull()),
        "source_staticparams_hash",
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
        .withColumn(
            "__effective_session_candidate_count",
            F.count(F.lit(1)).over(
                Window.partitionBy(
                    "__session_instrument_id",
                    "__effective_session_date",
                )
            ),
        )
    )
    joined = contract_specs.join(
        next_sessions,
        (F.col("instrument_id") == F.col("__session_instrument_id"))
        & (F.col("trade_date") == F.col("__source_trade_date")),
        "left",
    )
    calendar_gap_fallback = F.col("__effective_session_date").isNotNull() & (
        F.col("__effective_session_candidate_count") > F.lit(1)
    )
    return (
        joined.withColumn(
            "effective_session_date",
            F.when(calendar_gap_fallback, fallback_session_date).otherwise(
                F.coalesce(F.col("__effective_session_date"), fallback_session_date)
            ),
        )
        .withColumn(
            "effective_from_ts",
            F.when(
                calendar_gap_fallback,
                F.to_timestamp(
                    F.concat(F.col("effective_session_date").cast("string"), F.lit(" 00:00:00"))
                ),
            ).otherwise(
                F.coalesce(
                    F.col("__effective_from_ts"),
                    F.to_timestamp(
                        F.concat(
                            F.col("effective_session_date").cast("string"),
                            F.lit(" 00:00:00"),
                        )
                    ),
                )
            ),
        )
        .withColumn(
            "effective_session_source",
            F.when(calendar_gap_fallback, F.lit("calendar_gap_fallback"))
            .when(
                F.col("__effective_session_date").isNotNull(), F.lit("canonical_session_calendar")
            )
            .otherwise(F.lit("calendar_day_fallback")),
        )
        .drop(
            "__session_instrument_id",
            "__source_trade_date",
            "__effective_session_date",
            "__effective_from_ts",
            "__effective_session_candidate_count",
        )
    )


def _with_latest_margin_calibration(economics):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    valid_calibration = (
        (F.col("official_initial_margin") > F.lit(0.0))
        & (F.col("margin_radius_adjusted") > F.lit(0.0))
        & F.col("assetcode").isNotNull()
        & F.col("maturity_rank").isNotNull()
    )
    calibration_as_of = economics.where(valid_calibration).agg(
        F.max("economics_session_date").alias("__calibration_as_of_date")
    )
    calibration_lookup = (
        economics.crossJoin(calibration_as_of)
        .where(
            valid_calibration
            & (F.col("economics_session_date") == F.col("__calibration_as_of_date"))
        )
        .select(
            F.upper(F.col("assetcode")).alias("__calibration_assetcode"),
            F.col("maturity_rank").cast("int").alias("__calibration_rank"),
            F.col("__calibration_as_of_date"),
            (F.col("official_initial_margin") / F.col("margin_radius_adjusted")).alias(
                "__calibration_factor"
            ),
        )
        .groupBy(
            "__calibration_assetcode",
            "__calibration_rank",
            "__calibration_as_of_date",
        )
        .agg(
            F.count(F.lit(1)).alias("__margin_calibration_pair_rows"),
            F.min("__calibration_factor").alias("__calibration_factor"),
        )
    )
    candidates = (
        economics.alias("target")
        .join(
            F.broadcast(calibration_lookup).alias("calibration"),
            F.upper(F.col("target.assetcode")) == F.col("calibration.__calibration_assetcode"),
            "left",
        )
        .select(
            "target.*",
            F.col("calibration.__calibration_assetcode"),
            F.col("calibration.__calibration_rank"),
            F.col("calibration.__calibration_as_of_date"),
            F.col("calibration.__calibration_factor"),
            F.col("calibration.__margin_calibration_pair_rows"),
        )
    )
    choice_window = Window.partitionBy(
        "contract_id",
        "economics_session_date",
        "clearing_type",
    ).orderBy(
        F.abs(F.col("maturity_rank") - F.col("__calibration_rank")).asc_nulls_last(),
        F.col("__calibration_rank").asc_nulls_last(),
    )
    selected = (
        candidates.withColumn(
            "__margin_calibration_choice",
            F.row_number().over(choice_window),
        )
        .where(F.col("__margin_calibration_choice") == F.lit(1))
        .drop("__margin_calibration_choice")
    )
    calibration_source = (
        F.when(F.col("__calibration_factor").isNull(), F.lit(None).cast("string"))
        .when(
            F.col("maturity_rank") == F.col("__calibration_rank"),
            F.lit("latest_official_asset_rank"),
        )
        .otherwise(F.lit("latest_official_nearest_rank"))
    )
    margin_required = (
        F.when(
            F.col("official_initial_margin") > F.lit(0.0),
            F.col("official_initial_margin"),
        )
        .when(
            (F.col("margin_radius_adjusted") > F.lit(0.0))
            & (F.col("__calibration_factor") > F.lit(0.0)),
            F.col("margin_radius_adjusted") * F.col("__calibration_factor"),
        )
        .otherwise(F.lit(None).cast("double"))
    )
    model_quality = (
        F.when(
            F.col("official_initial_margin") > F.lit(0.0),
            F.lit("official_initial_margin"),
        )
        .when(
            calibration_source == F.lit("latest_official_asset_rank"),
            F.lit("calibrated_asset_rank"),
        )
        .when(
            calibration_source == F.lit("latest_official_nearest_rank"),
            F.lit("calibrated_nearest_rank"),
        )
        .otherwise(F.lit("unavailable"))
    )
    return (
        selected.withColumn(
            "margin_calibration_factor",
            F.col("__calibration_factor").cast("double"),
        )
        .withColumn(
            "margin_calibration_as_of_date",
            F.col("__calibration_as_of_date").cast("date"),
        )
        .withColumn(
            "margin_calibration_rank",
            F.col("__calibration_rank").cast("int"),
        )
        .withColumn("margin_calibration_source", calibration_source)
        .withColumn("margin_required_no_buffer", margin_required)
        .withColumn("margin_buffer_pct", F.lit(0.0).cast("double"))
        .withColumn("margin_required_estimate", margin_required)
        .withColumn("model_quality", model_quality)
        .drop(
            "__calibration_assetcode",
            "__calibration_rank",
            "__calibration_as_of_date",
            "__calibration_factor",
        )
    )


def _filter_non_session_contract_specs(
    contract_specs,
    fx_rates,
    risk_parameters,
    *,
    canonical_session_calendar_path: Path | None,
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    fx_dates = (
        fx_rates.where(F.col("rate_to_rub").isNotNull() & (F.col("rate_to_rub") > 0))
        .select(F.col("rate_date").alias("__fx_support_date"))
        .distinct()
    )
    risk_dates = (
        risk_parameters.where(F.col("mr1").isNotNull() & (F.col("mr1") > 0))
        .select(F.col("risk_session_date").alias("__risk_support_date"))
        .distinct()
    )
    support_dates = (
        fx_dates.join(
            risk_dates,
            F.col("__fx_support_date") == F.col("__risk_support_date"),
            "inner",
        )
        .select(F.col("__fx_support_date").alias("__economics_support_date"))
        .distinct()
    )
    filtered = contract_specs.join(
        support_dates,
        F.col("trade_date") == F.col("__economics_support_date"),
        "left",
    )

    if canonical_session_calendar_path is not None:
        spark = contract_specs.sparkSession
        session_dates = (
            spark.read.format("delta")
            .load(str(canonical_session_calendar_path))
            .select(F.col("session_date").alias("__calendar_session_date"))
            .where(F.col("__calendar_session_date").isNotNull())
            .distinct()
        )
        session_bounds = session_dates.agg(
            F.min("__calendar_session_date").alias("__min_calendar_session_date"),
            F.max("__calendar_session_date").alias("__max_calendar_session_date"),
        )
        filtered = filtered.join(
            session_dates,
            F.col("trade_date") == F.col("__calendar_session_date"),
            "left",
        ).crossJoin(session_bounds)
    else:
        filtered = (
            filtered.withColumn("__calendar_session_date", F.lit(None).cast("date"))
            .withColumn("__min_calendar_session_date", F.lit(None).cast("date"))
            .withColumn("__max_calendar_session_date", F.lit(None).cast("date"))
        )

    weekend_without_support = F.col("__economics_support_date").isNull() & F.dayofweek(
        F.col("trade_date")
    ).isin(1, 7)
    known_calendar_non_session_without_support = (
        F.col("__economics_support_date").isNull()
        & F.col("__calendar_session_date").isNull()
        & F.col("__min_calendar_session_date").isNotNull()
        & (F.col("trade_date") >= F.col("__min_calendar_session_date"))
        & (F.col("trade_date") <= F.col("__max_calendar_session_date"))
    )
    return filtered.where(
        ~(weekend_without_support | known_calendar_non_session_without_support)
    ).drop(
        "__economics_support_date",
        "__calendar_session_date",
        "__min_calendar_session_date",
        "__max_calendar_session_date",
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

    contract_specs = _filter_non_session_contract_specs(
        contract_specs,
        fx_rates,
        risk_parameters,
        canonical_session_calendar_path=canonical_session_calendar_path,
    )
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
    risk_for_economics = (
        contract_specs.select("contract_id", "trade_date", "assetcode")
        .distinct()
        .alias("spec_risk")
        .join(
            risk_parameters.alias("risk"),
            (F.col("spec_risk.assetcode") == F.col("risk.assetcode"))
            & (F.col("risk.risk_session_date") <= F.col("spec_risk.trade_date")),
            "left",
        )
        .withColumn(
            "__risk_rank",
            F.row_number().over(
                Window.partitionBy(
                    F.col("spec_risk.contract_id"),
                    F.col("spec_risk.trade_date"),
                    F.col("spec_risk.assetcode"),
                ).orderBy(F.col("risk.risk_session_date").desc_nulls_last())
            ),
        )
        .where(F.col("__risk_rank") == F.lit(1))
        .select(
            F.col("spec_risk.contract_id").alias("__risk_contract_id"),
            F.col("spec_risk.trade_date").alias("__risk_contract_trade_date"),
            F.col("spec_risk.assetcode").alias("__risk_contract_assetcode"),
            F.col("risk.assetcode").alias("assetcode"),
            F.col("risk.risk_session_date").alias("risk_session_date"),
            F.col("risk.mr1").alias("mr1"),
            F.col("risk.mr2").alias("mr2"),
            F.col("risk.mr3").alias("mr3"),
            F.col("risk.radius_pct").alias("radius_pct"),
            F.col("risk.radius_source").alias("radius_source"),
            F.col("risk.source_limits_hash").alias("source_limits_hash"),
            F.col("risk.source_staticparams_hash").alias("source_staticparams_hash"),
        )
    )
    contract_parameter_regimes = _contract_parameter_regimes_frame(contract_specs.sparkSession)
    main_clearing_fx = fx_rates.where(F.col("clearing_type") == F.lit("mc"))
    joined = (
        contract_specs.alias("spec")
        .join(
            risk_for_economics.alias("risk"),
            (F.col("spec.contract_id") == F.col("risk.__risk_contract_id"))
            & (F.col("spec.trade_date") == F.col("risk.__risk_contract_trade_date"))
            & (F.col("spec.assetcode") == F.col("risk.__risk_contract_assetcode")),
            "left",
        )
        .join(
            fx_for_economics.alias("fx"),
            (F.col("spec.quote_currency") == F.col("fx.base_currency"))
            & (F.col("spec.trade_date") == F.col("fx.rate_date")),
            "left",
        )
        .join(
            contract_parameter_regimes.alias("rule"),
            (F.col("spec.assetcode") == F.col("rule.rule_assetcode"))
            & (F.col("spec.trade_date") >= F.col("rule.rule_effective_from"))
            & (F.col("spec.trade_date") <= F.col("rule.rule_effective_to")),
            "left",
        )
        .join(
            main_clearing_fx.alias("step_fx"),
            (F.col("rule.rule_quote_currency") == F.col("step_fx.base_currency"))
            & (F.col("spec.trade_date") == F.col("step_fx.rate_date")),
            "left",
        )
    )
    resolved_quote_currency = F.coalesce(
        F.col("rule.rule_quote_currency"), F.col("spec.quote_currency")
    )
    resolved_fx_rate = F.when(
        F.col("rule.rule_id").isNotNull(), F.col("step_fx.rate_to_rub")
    ).otherwise(F.col("fx.rate_to_rub"))
    rule_min_step_matches = F.col("spec.min_step").isNotNull() & (
        F.abs(F.col("spec.min_step") - F.col("rule.rule_min_step")) <= F.lit(1e-12)
    )
    rule_lot_volume_matches = F.col("spec.lot_volume").isNotNull() & (
        F.abs(F.col("spec.lot_volume") - F.col("rule.rule_lot_volume")) <= F.lit(1e-12)
    )
    rule_parameters_match = rule_min_step_matches & rule_lot_volume_matches
    rule_step_price_rub = F.when(
        rule_parameters_match,
        F.col("rule.rule_tick_value_quote") * F.col("step_fx.rate_to_rub"),
    ).otherwise(F.lit(None).cast("double"))
    step_price_rub = F.when(
        F.col("spec.official_step_price") > F.lit(0.0),
        F.col("spec.official_step_price"),
    ).otherwise(rule_step_price_rub)
    step_price_source = (
        F.when(F.col("spec.official_step_price") > F.lit(0.0), F.lit("moex_stepprice"))
        .when(rule_step_price_rub > F.lit(0.0), F.lit("contract_parameter_regime"))
        .otherwise(F.lit("unavailable"))
    )
    turnover_implied_step_price_rub = F.when(
        (F.col("spec.history_value_rub") > F.lit(0.0))
        & (F.col("spec.history_volume") > F.lit(0.0))
        & (F.col("spec.history_waprice") > F.lit(0.0)),
        (
            F.col("spec.history_value_rub")
            / (F.col("spec.history_volume") * F.col("spec.history_waprice"))
            * F.col("spec.min_step")
        ),
    ).otherwise(F.lit(None).cast("double"))
    rule_validation_relative_error = F.when(
        (rule_step_price_rub > F.lit(0.0)) & turnover_implied_step_price_rub.isNotNull(),
        F.abs(turnover_implied_step_price_rub / rule_step_price_rub - F.lit(1.0)),
    ).otherwise(F.lit(None).cast("double"))
    tick_value_currency = F.when(
        F.upper(resolved_quote_currency) == F.lit("RUB"),
        step_price_rub,
    ).otherwise(step_price_rub / resolved_fx_rate)
    linked_assetcodes = sorted(MOEX_FX_OR_USD_LINKED_ASSETS)
    currency_radius_applies = (F.upper(resolved_quote_currency) != F.lit("RUB")) | F.upper(
        F.col("spec.assetcode")
    ).isin(linked_assetcodes)
    applied_radius_pct = F.when(currency_radius_applies, F.col("risk.radius_pct")).otherwise(
        F.lit(0.0)
    )
    applied_radius_source = F.when(
        currency_radius_applies,
        F.col("risk.radius_source"),
    ).otherwise(F.lit("not_applicable_pure_rub"))
    margin_formula_base = (
        F.col("spec.last_settle_price")
        * (step_price_rub / F.col("spec.min_step"))
        * F.col("risk.mr1")
    )
    margin_radius_adjusted = margin_formula_base * (F.lit(1.0) + applied_radius_pct / F.lit(100.0))
    margin_required_no_buffer = F.lit(None).cast("double")
    days_to_expiry = F.datediff(F.col("spec.expiration_date"), F.col("spec.trade_date"))
    margin_buffer_pct = F.lit(0.0).cast("double")
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
                    applied_radius_source.alias("radius_source"),
                    F.col("risk.risk_session_date").cast("string").alias("risk_session_date"),
                    F.coalesce(F.col("step_fx.fx_pair"), F.col("fx.fx_pair")).alias("fx_pair"),
                    step_price_source.alias("step_price_source"),
                    F.col("rule.rule_id").alias("step_price_rule_id"),
                    F.col("rule.rule_source_document_id").alias(
                        "step_price_rule_source_document_id"
                    ),
                    F.col("rule.rule_min_step").alias("step_price_rule_min_step"),
                    F.col("rule.rule_lot_volume").alias("step_price_rule_lot_volume"),
                    F.col("rule.rule_tick_value_quote").alias("step_price_rule_tick_value_quote"),
                    F.col("spec.effective_session_source").alias("effective_session_source"),
                )
            ),
        )
        .withColumn(
            "source_document_hashes_json",
            F.to_json(
                F.struct(
                    F.col("spec.contract_source_hash").alias("contract"),
                    F.coalesce(
                        F.col("step_fx.source_document_hash"),
                        F.col("fx.source_document_hash"),
                    ).alias("fx"),
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
    uncalibrated = base.withColumn(
        "effective_to_ts", F.lead("effective_from_ts").over(interval_window)
    ).select(
        F.col("spec.contract_id").alias("contract_id"),
        F.col("spec.instrument_id").alias("instrument_id"),
        F.col("spec.moex_secid").alias("moex_secid"),
        F.col("spec.assetcode").alias("assetcode"),
        F.col("spec.trade_date").alias("economics_session_date"),
        F.col("spec.effective_session_date").alias("effective_session_date"),
        F.when(F.col("rule.rule_id").isNotNull(), F.lit("mc"))
        .otherwise(F.coalesce(F.col("fx.clearing_type"), F.lit("mc")))
        .alias("clearing_type"),
        "effective_from_ts",
        "effective_to_ts",
        F.col("spec.min_step").alias("min_step"),
        F.col("spec.lot_volume").alias("lot_volume"),
        resolved_quote_currency.alias("quote_currency"),
        resolved_fx_rate.alias("fx_rate_to_rub"),
        "tick_value_currency",
        "step_price_rub",
        F.col("spec.official_step_price").alias("official_step_price"),
        F.col("spec.official_initial_margin").alias("official_initial_margin"),
        F.col("spec.last_settle_price").alias("last_settle_price"),
        F.col("risk.mr1").alias("mr1"),
        applied_radius_pct.alias("radius_pct"),
        applied_radius_source.alias("radius_source"),
        "margin_formula_base",
        "margin_radius_adjusted",
        "margin_required_no_buffer",
        "margin_buffer_pct",
        F.col("margin_required_no_buffer").alias("margin_required_estimate"),
        F.col("spec.maturity_rank").cast("int").alias("maturity_rank"),
        F.col("days_to_expiry").alias("days_to_expiry"),
        F.col("spec.expiration_date").alias("expiration_date"),
        F.lit(MOEX_CONTRACT_ECONOMICS_MODEL_VERSION).alias("model_version"),
        F.lit(MOEX_MARGIN_BUFFER_POLICY_VERSION).alias("buffer_policy_version"),
        F.lit("unavailable").alias("model_quality"),
        "source_flags_json",
        "source_document_hashes_json",
        step_price_source.alias("__step_price_source"),
        F.col("rule.rule_id").alias("__step_price_rule_id"),
        F.col("rule.rule_assetcode").alias("__step_price_rule_assetcode"),
        F.col("rule.rule_quote_currency").alias("__step_price_rule_quote_currency"),
        F.col("rule.rule_min_step").alias("__step_price_rule_min_step"),
        F.col("rule.rule_lot_volume").alias("__step_price_rule_lot_volume"),
        F.col("rule.rule_tick_value_quote").alias("__step_price_rule_tick_value_quote"),
        rule_min_step_matches.alias("__step_price_rule_min_step_matches"),
        rule_lot_volume_matches.alias("__step_price_rule_lot_volume_matches"),
        rule_validation_relative_error.alias("__step_price_rule_validation_relative_error"),
        F.current_timestamp().alias("created_at"),
    )
    return _with_latest_margin_calibration(uncalibrated)


def _missing_input_counts(economics) -> dict[str, int]:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    fields = {
        "MINSTEP": "min_step",
        "LOTVOLUME": "lot_volume",
        "FX": "fx_rate_to_rub",
        "MR1": "mr1",
        "LASTSETTLEPRICE": "last_settle_price",
        "STEPPRICE": "step_price_rub",
        "OFFICIAL_STEPPRICE": "official_step_price",
        "INITIALMARGIN": "official_initial_margin",
    }
    missing = economics.agg(
        *(
            F.sum(
                F.when(
                    F.col(column_name).isNull() | (F.col(column_name) <= F.lit(0)),
                    1,
                ).otherwise(0)
            ).alias(field_name)
            for field_name, column_name in fields.items()
        )
    ).collect()[0]
    return {key: int(missing[key] or 0) for key in missing.asDict()}


def _fail_closed_on_missing_inputs(
    economics,
) -> tuple[dict[str, int], dict[str, int]]:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    historical_missing_counts = _missing_input_counts(economics)
    latest_session_date = economics.agg(F.max("economics_session_date").alias("latest")).collect()[
        0
    ]["latest"]
    if latest_session_date is None:
        raise RuntimeError("MOEX contract economics has no source rows")
    latest_missing_counts = _missing_input_counts(
        economics.where(F.col("economics_session_date") == F.lit(latest_session_date))
    )
    offenders = [
        f"{key}={value}"
        for key, value in latest_missing_counts.items()
        if value and key != "OFFICIAL_STEPPRICE"
    ]
    if offenders:
        raise RuntimeError(
            "MOEX contract economics latest session missing required inputs: "
            + ", ".join(offenders)
        )
    return latest_missing_counts, historical_missing_counts


def _fail_closed_on_contract_parameter_regime_mismatches(economics) -> int:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    mismatches = economics.where(
        F.col("__step_price_rule_id").isNotNull()
        & (
            ~F.coalesce(F.col("__step_price_rule_min_step_matches"), F.lit(False))
            | ~F.coalesce(F.col("__step_price_rule_lot_volume_matches"), F.lit(False))
        )
    )
    mismatch_count = int(mismatches.count())
    if mismatch_count == 0:
        return 0

    def _number_text(value: object) -> str:
        return "null" if value is None else str(float(value))

    details: list[str] = []
    for row in (
        mismatches.select(
            "__step_price_rule_id",
            "__step_price_rule_min_step",
            "__step_price_rule_lot_volume",
            "__step_price_rule_min_step_matches",
            "__step_price_rule_lot_volume_matches",
            "min_step",
            "lot_volume",
        )
        .limit(20)
        .collect()
    ):
        fields: list[str] = []
        if not bool(row["__step_price_rule_min_step_matches"]):
            fields.append(
                "MINSTEP expected="
                f"{_number_text(row['__step_price_rule_min_step'])} "
                f"observed={_number_text(row['min_step'])}"
            )
        if not bool(row["__step_price_rule_lot_volume_matches"]):
            fields.append(
                "LOTVOLUME expected="
                f"{_number_text(row['__step_price_rule_lot_volume'])} "
                f"observed={_number_text(row['lot_volume'])}"
            )
        details.append(f"{row['__step_price_rule_id']}: " + ", ".join(fields))
    raise RuntimeError(
        f"MOEX contract parameter regime mismatch rows={mismatch_count}: " + "; ".join(details)
    )


def _fail_closed_on_step_price_rule_validation(economics) -> list[dict[str, object]]:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    raw_profiles = [
        row.asDict()
        for row in economics.where(
            F.col("__step_price_rule_id").isNotNull()
            & (F.col("__step_price_source") == F.lit("contract_parameter_regime"))
        )
        .groupBy(
            "__step_price_rule_id",
            "__step_price_rule_assetcode",
            "__step_price_rule_quote_currency",
            "__step_price_rule_min_step",
            "__step_price_rule_lot_volume",
            "__step_price_rule_tick_value_quote",
        )
        .agg(
            F.count(F.lit(1)).alias("rule_rows"),
            F.count("__step_price_rule_validation_relative_error").alias("validation_rows"),
            F.percentile_approx(
                "__step_price_rule_validation_relative_error",
                0.95,
                10_000,
            ).alias("validation_p95_relative_error"),
            F.max("__step_price_rule_validation_relative_error").alias(
                "validation_max_relative_error"
            ),
        )
        .orderBy("__step_price_rule_id")
        .collect()
    ]
    profiles = [
        {
            "rule_id": raw["__step_price_rule_id"],
            "assetcode": raw["__step_price_rule_assetcode"],
            "quote_currency": raw["__step_price_rule_quote_currency"],
            "min_step": _json_safe_value(raw["__step_price_rule_min_step"]),
            "lot_volume": _json_safe_value(raw["__step_price_rule_lot_volume"]),
            "tick_value_quote": _json_safe_value(raw["__step_price_rule_tick_value_quote"]),
            "rule_rows": int(raw["rule_rows"]),
            "validation_rows": int(raw["validation_rows"]),
            "validation_p95_relative_error": _json_safe_value(raw["validation_p95_relative_error"]),
            "validation_max_relative_error": _json_safe_value(raw["validation_max_relative_error"]),
        }
        for raw in raw_profiles
    ]
    offenders = [
        str(profile["rule_id"])
        for profile in profiles
        if int(profile["validation_rows"] or 0) == 0
        or float(profile["validation_p95_relative_error"] or 0.0)
        > MOEX_STEP_PRICE_RULE_VALIDATION_P95_LIMIT
    ]
    if offenders:
        raise RuntimeError("MOEX step-price rule validation failed: " + ", ".join(offenders))
    return profiles


def _fail_closed_on_invalid_intervals(economics) -> dict[str, int]:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    zero_or_negative = int(
        economics.where(
            F.col("effective_to_ts").isNotNull()
            & (F.col("effective_to_ts") <= F.col("effective_from_ts"))
        ).count()
    )
    duplicate_rows = int(
        economics.groupBy("contract_id", "effective_from_ts")
        .count()
        .where(F.col("count") > F.lit(1))
        .agg(F.coalesce(F.sum("count"), F.lit(0)).alias("rows"))
        .collect()[0]["rows"]
    )
    interval_counts = {
        "zero_or_negative_effective_intervals": zero_or_negative,
        "duplicate_effective_from_rows": duplicate_rows,
    }
    offenders = [f"{key}={value}" for key, value in interval_counts.items() if value]
    if offenders:
        raise RuntimeError("MOEX contract economics invalid intervals: " + ", ".join(offenders))
    return interval_counts


def _margin_calibration_report(economics) -> dict[str, object]:
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    counts = economics.agg(
        F.max("margin_calibration_as_of_date").alias("margin_calibration_as_of_date"),
        F.sum(
            F.when(F.col("model_quality") == F.lit("official_initial_margin"), 1).otherwise(0)
        ).alias("official_margin_rows"),
        F.sum(
            F.when(
                F.col("model_quality").isin(
                    "calibrated_asset_rank",
                    "calibrated_nearest_rank",
                ),
                1,
            ).otherwise(0)
        ).alias("calibrated_margin_rows"),
        F.sum(
            F.when(
                F.col("margin_calibration_source") == F.lit("latest_official_asset_rank"),
                1,
            ).otherwise(0)
        ).alias("exact_rank_calibration_rows"),
        F.sum(
            F.when(
                F.col("margin_calibration_source") == F.lit("latest_official_nearest_rank"),
                1,
            ).otherwise(0)
        ).alias("nearest_rank_calibration_rows"),
        F.sum(
            F.when(
                (
                    F.col("official_initial_margin").isNull()
                    | (F.col("official_initial_margin") <= F.lit(0.0))
                )
                & (F.col("margin_radius_adjusted") > F.lit(0.0))
                & F.col("margin_calibration_factor").isNull(),
                1,
            ).otherwise(0)
        ).alias("margin_calibration_unavailable_rows"),
        F.sum(F.when(F.abs(F.col("margin_buffer_pct")) > F.lit(1e-12), 1).otherwise(0)).alias(
            "nonzero_margin_buffer_rows"
        ),
        F.sum(F.when(F.col("margin_calibration_factor") <= F.lit(0.0), 1).otherwise(0)).alias(
            "nonpositive_margin_calibration_rows"
        ),
    ).collect()[0]
    duplicate_pairs = int(
        economics.where(F.col("__margin_calibration_pair_rows") > F.lit(1))
        .select(
            "assetcode",
            "margin_calibration_rank",
            "margin_calibration_as_of_date",
        )
        .distinct()
        .count()
    )
    report = {
        key: _json_safe_value(value) if key == "margin_calibration_as_of_date" else int(value or 0)
        for key, value in counts.asDict().items()
    }
    report["duplicate_margin_calibration_pairs"] = duplicate_pairs
    offenders = {
        key: int(report[key])
        for key in (
            "duplicate_margin_calibration_pairs",
            "nonpositive_margin_calibration_rows",
            "nonzero_margin_buffer_rows",
        )
        if int(report[key]) > 0
    }
    if offenders:
        raise RuntimeError(
            "MOEX margin calibration quality failure: "
            + ", ".join(f"{key}={value}" for key, value in offenders.items())
        )
    return report


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
        contract_parameter_regime_mismatch_rows = (
            _fail_closed_on_contract_parameter_regime_mismatches(economics)
        )
        missing_counts, historical_missing_counts = _fail_closed_on_missing_inputs(economics)
        step_price_rule_profiles = _fail_closed_on_step_price_rule_validation(economics)
        interval_counts = _fail_closed_on_invalid_intervals(economics)
        margin_calibration_report = _margin_calibration_report(economics)

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
        official_rows = int(margin_calibration_report["official_margin_rows"])
        formula_rows = int(margin_calibration_report["calibrated_margin_rows"])
        unavailable_economics_rows = int(
            economics.where(F.col("model_quality") == F.lit("unavailable")).count()
        )
        step_price_unavailable_rows = int(economics.where(F.col("step_price_rub").isNull()).count())
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
            "missing_economics_rows": unavailable_economics_rows,
            "missing_required_input_counts": missing_counts,
            "historical_missing_input_counts": historical_missing_counts,
            "unavailable_economics_rows": unavailable_economics_rows,
            "step_price_unavailable_rows": step_price_unavailable_rows,
            "step_price_rule_profiles": step_price_rule_profiles,
            "contract_parameter_regime_mismatch_rows": (contract_parameter_regime_mismatch_rows),
            "step_price_rule_validation_p95_limit": (MOEX_STEP_PRICE_RULE_VALIDATION_P95_LIMIT),
            **interval_counts,
            "defaulted_radius_rows": defaulted_radius_rows,
            "official_margin_dominates_rows": official_rows,
            "formula_margin_dominates_rows": formula_rows,
            **margin_calibration_report,
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
