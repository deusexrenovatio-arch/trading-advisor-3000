"""Isolated typed MOEX quote materialization; no production publication."""

from __future__ import annotations

import json
from functools import reduce
from pathlib import Path
from typing import Any

from .canonical_bars_job import _create_spark_session

_SCHEMAS = Path(__file__).resolve().parents[1] / "product_plane" / "contracts" / "schemas"
_KEYS = {
    "raw_quote_candles": ("source_run_id", "series_id", "source_begin"),
    "quote_series": ("series_id",),
    "quote_bars": ("series_id", "timeframe", "ts_open"),
}


def _authority(name: str) -> dict[str, Any]:
    return json.loads((_SCHEMAS / f"market_quote_{name}.v1.json").read_text("utf-8"))


def _spark_schema(authority: dict[str, Any]):
    from pyspark.sql import types as T

    types = {
        "string": T.StringType,
        "number": T.DoubleType,
        "integer": T.LongType,
        "boolean": T.BooleanType,
    }
    fields = []
    for name, field in authority["properties"].items():
        declared = field["type"]
        variants = declared if isinstance(declared, list) else [declared]
        concrete = [item for item in variants if item != "null"]
        # All-null fields still need a persisted physical type in Delta.
        physical = types[concrete[0]]() if concrete else T.LongType()
        fields.append(T.StructField(name, physical, "null" in variants))
    return T.StructType(fields)


def _reject(frame, condition, reason: str) -> None:
    if frame.where(condition).limit(1).count():
        raise ValueError(reason)


def _deduplicate(frame, keys: tuple[str, ...], label: str):
    from pyspark.sql import functions as F

    unique = frame.dropDuplicates()
    _reject(unique.groupBy(*keys).count(), F.col("count") > 1, f"conflicting {label} keys")
    return unique


def _merge(frame, path: Path, keys: tuple[str, ...]) -> int:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if not (path / "_delta_log").exists():
        if path.exists() and any(path.iterdir()):
            raise ValueError(f"refusing non-Delta target: {path}")
        frame.write.format("delta").mode("errorifexists").save(str(path))
    else:
        target = frame.sparkSession.read.format("delta").load(str(path))
        _reject(target.groupBy(*keys).count(), F.col("count") > 1, "duplicate target keys")
        same = reduce(
            lambda left, right: left & right,
            [F.col(f"s.{name}").eqNullSafe(F.col(f"t.{name}")) for name in frame.columns],
        )
        changed = frame.alias("s").join(target.alias("t"), same, "left_anti")
        if changed.limit(1).count():
            condition = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
            (
                DeltaTable.forPath(frame.sparkSession, str(path))
                .alias("t")
                .merge(changed.alias("s"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
    return int(DeltaTable.forPath(frame.sparkSession, str(path)).history(1).first()["version"])


def _preflight_targets(frames: dict[str, Any], output: Path) -> None:
    from pyspark.sql import functions as F

    from trading_advisor_3000.product_plane.data_plane.moex.quote_pilot import validate_pilot_path

    for name, frame in frames.items():
        path = validate_pilot_path(output / f"{name}.delta")
        if not (path / "_delta_log").exists():
            if path.exists():
                raise ValueError(f"refusing non-Delta target: {path}")
            continue
        target = frame.sparkSession.read.format("delta").load(str(path))
        if frame.schema != target.schema:
            # Delta makes persisted fields nullable; nullability is not a wire type.
            source_types = {field.name: field.dataType for field in frame.schema}
            target_types = {field.name: field.dataType for field in target.schema}
            if source_types != target_types:
                raise ValueError(f"pilot target schema drift: {name}")
        _reject(
            target.groupBy(*_KEYS[name]).count(),
            F.col("count") > 1,
            f"duplicate target keys: {name}",
        )
        if name == "raw_quote_candles":
            mismatch = reduce(
                lambda left, right: left | right,
                [
                    ~F.col(f"s.{column}").eqNullSafe(F.col(f"t.{column}"))
                    for column in frame.columns
                    if column not in _KEYS[name]
                ],
            )
            _reject(
                frame.alias("s").join(target.alias("t"), list(_KEYS[name])),
                mismatch,
                "immutable source candle changed; acquire a new source_run_id",
            )
        if name == "quote_series":
            mutable = {"metadata_asof", "metadata_json", "aliases_json"}
            mismatch = reduce(
                lambda left, right: left | right,
                [
                    ~F.col(f"s.{column}").eqNullSafe(F.col(f"t.{column}"))
                    for column in frame.columns
                    if column not in mutable | {"series_id"}
                ],
            )
            _reject(
                frame.alias("s").join(target.alias("t"), "series_id"),
                mismatch,
                "existing quote series identity changed",
            )


def _normalized_frames(spark, acquisition: Path, metadata: dict[str, Any]):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from trading_advisor_3000.product_plane.contracts.market_quotes import validate_quote_series

    descriptors = [
        json.loads(line)
        for line in (acquisition / "series.jsonl").read_text("utf-8").splitlines()
        if line.strip()
    ]
    if not descriptors:
        raise ValueError("empty quote series scope")
    for descriptor in descriptors:
        validate_quote_series(descriptor)
    series = _deduplicate(
        spark.createDataFrame(descriptors, _spark_schema(_authority("series"))),
        _KEYS["quote_series"],
        "quote series",
    ).cache()
    envelope_names = ("series_id", "timeframe", "source_run_id", "fetched_at", "payload_json")
    envelope_schema = T.StructType([T.StructField(name, T.StringType()) for name in envelope_names])
    source = (
        spark.read.schema(envelope_schema)
        .option("mode", "FAILFAST")
        .json(str(acquisition / "candles.jsonl"))
    )
    payload_schema = T.StructType(
        [
            T.StructField(name, T.DoubleType())
            for name in ("open", "high", "low", "close", "volume", "value")
        ]
        + [T.StructField(name, T.StringType()) for name in ("begin", "end")]
    )
    source = source.withColumn("payload", F.from_json("payload_json", payload_schema))
    _reject(
        source,
        F.col("series_id").isNull()
        | (F.col("timeframe") != "1d")
        | F.col("timeframe").isNull()
        | ~F.col("source_run_id").eqNullSafe(F.lit(metadata["run_id"]))
        | F.to_timestamp("fetched_at").isNull()
        | ~F.col("fetched_at").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$"),
        "invalid quote envelope",
    )
    _reject(
        source.join(series.select("series_id"), "series_id", "left_anti"),
        F.lit(True),
        "unmatched quote series",
    )
    raw = source.select(
        *envelope_names,
        F.col("payload.begin").alias("source_begin"),
        F.col("payload.end").alias("source_end"),
    ).cache()
    raw = _deduplicate(raw, _KEYS["raw_quote_candles"], "raw candle").cache()
    _reject(
        series.join(raw.select("series_id").distinct(), "series_id", "left_anti"),
        F.lit(True),
        "series has no source candles",
    )
    parsed = (
        source.join(series.select("series_id", "asset_class"), "series_id")
        .select(
            *envelope_names,
            "asset_class",
            "payload.*",
            F.to_timestamp("payload.begin", "yyyy-MM-dd HH:mm:ss").alias("local_open"),
            F.to_timestamp("payload.end", "yyyy-MM-dd HH:mm:ss").alias("local_close"),
        )
        .cache()
    )
    numeric_invalid = F.lit(False)
    for name in ("open", "high", "low", "close", "volume", "value"):
        value = F.col(name)
        supplied = F.get_json_object("payload_json", f"$.{name}").isNotNull()
        bad = F.isnan(value) | (F.abs(value) == F.lit(float("inf")))
        bad = bad | (value.isNull() & supplied)
        if name in {"open", "high", "low", "close"}:
            bad = bad | value.isNull()
        else:
            bad = bad | (value < 0)
        numeric_invalid = numeric_invalid | bad
    _reject(parsed, numeric_invalid, "invalid quote numeric value")
    _reject(
        parsed,
        (F.col("low") > F.least("open", "close"))
        | (F.col("high") < F.greatest("open", "close"))
        | (F.col("low") > F.col("high")),
        "invalid quote OHLC bounds",
    )
    _reject(
        parsed,
        F.col("local_open").isNull()
        | F.col("local_close").isNull()
        | (F.col("local_close") < F.col("local_open"))
        | (F.to_date("local_open") != F.to_date("local_close"))
        | ~F.to_date("local_open").between(metadata["date_from"], metadata["date_till"]),
        "quote timestamp outside closed source dates",
    )
    utc_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    index = F.col("asset_class") == "index"
    bar_values = {
        "schema_version": F.lit("market_quote_bar.v1"),
        "ts_open": F.date_format(F.to_utc_timestamp("local_open", "Europe/Moscow"), utc_format),
        "ts_close": F.date_format(F.to_utc_timestamp("local_close", "Europe/Moscow"), utc_format),
        "source_begin": F.col("begin"),
        "source_end": F.col("end"),
        "volume": F.when(index, F.lit(None).cast("double")).otherwise(F.col("volume")),
        "turnover": F.when(index, F.lit(None).cast("double")).otherwise(F.col("value")),
        "open_interest": F.lit(None).cast("long"),
        "volume_status": F.when(index, "not_applicable")
        .when(F.col("volume").isNull(), "missing")
        .otherwise("reported"),
    }
    bars = parsed.select(
        *[
            bar_values.get(field.name, F.col(field.name)).cast(field.dataType).alias(field.name)
            for field in _spark_schema(_authority("bar"))
        ]
    )
    return {
        "raw_quote_candles": raw,
        "quote_series": series,
        "quote_bars": _deduplicate(bars, _KEYS["quote_bars"], "canonical candle").cache(),
    }


def run_moex_quote_pilot_spark_job(
    *, acquisition_dir: Path, output_root: Path, run_id: str, spark_master: str = "local[2]"
) -> dict[str, Any]:
    from trading_advisor_3000.product_plane.data_plane.moex.quote_pilot import (
        evidence_hashes,
        load_acquisition,
        validate_pilot_path,
        validate_run_id,
    )

    acquisition = validate_pilot_path(acquisition_dir)
    output = validate_pilot_path(output_root)
    report_path = validate_pilot_path(output / "reports" / f"{validate_run_id(run_id)}.json")
    if output == acquisition or output in acquisition.parents or acquisition in output.parents:
        raise ValueError("acquisition and materialization roots must not overlap")
    if report_path.exists():
        raise FileExistsError(f"immutable pilot report already exists: {report_path}")
    metadata = load_acquisition(acquisition)
    hashes = evidence_hashes(acquisition)
    output.mkdir(parents=True, exist_ok=True)
    lock = validate_pilot_path(output / ".writer.lock")
    with lock.open("x", encoding="utf-8") as handle:
        handle.write(run_id)
    spark = None
    frames: dict[str, Any] = {}
    try:
        spark = _create_spark_session("ta3000-moex-quote-pilot", spark_master)
        frames = _normalized_frames(spark, acquisition, metadata)
        input_counts = {name: int(frame.count()) for name, frame in frames.items()}
        if not input_counts["quote_bars"]:
            raise ValueError("empty quote candle scope")
        if evidence_hashes(acquisition) != hashes:
            raise ValueError("source evidence changed during normalization")
        _preflight_targets(frames, output)
        tables = {}
        for name, frame in frames.items():
            path = validate_pilot_path(output / f"{name}.delta")
            version = _merge(frame, path, _KEYS[name])
            pinned = spark.read.format("delta").option("versionAsOf", version).load(str(path))
            tables[name] = {
                "path": path.as_posix(),
                "version": version,
                "row_count": int(pinned.count()),
            }
        if evidence_hashes(acquisition) != hashes:
            raise ValueError("source evidence changed before report publication")
        report = {
            "schema_version": "moex_quote_pilot_report.v1",
            "status": "PASS",
            "run_id": run_id,
            "acquisition_dir": acquisition.as_posix(),
            "source_run_id": metadata["run_id"],
            "date_from": metadata["date_from"],
            "date_till": metadata["date_till"],
            "source_evidence": hashes,
            "scope": {
                "date_from": metadata["date_from"],
                "date_till": metadata["date_till"],
                "series": [
                    row.asDict()
                    for row in frames["quote_series"]
                    .select(
                        "series_id", "security_id", "asset_class", "board", "underlying_security_id"
                    )
                    .orderBy("series_id")
                    .collect()
                ],
            },
            "tables": tables,
            "row_counts": input_counts,
            "per_series_counts": {
                row["series_id"]: int(row["count"])
                for row in frames["quote_bars"].groupBy("series_id").count().collect()
            },
            "timestamp_policy": "source Europe/Moscow; UTC ISO; ISS inclusive end preserved",
            "spark_master": spark_master,
            "delta_writer": "spark",
            "report_path": report_path.as_posix(),
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("x", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.write("\n")
        return report
    finally:
        for frame in frames.values():
            frame.unpersist()
        if spark is not None:
            spark.stop()
        lock.unlink(missing_ok=True)
