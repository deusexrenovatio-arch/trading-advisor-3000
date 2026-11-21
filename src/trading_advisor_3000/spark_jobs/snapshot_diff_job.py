"""Explicit read-only recovery proof between two immutable Data boundaries."""

from __future__ import annotations

import json
from datetime import timedelta

from trading_advisor_3000.product_plane.common.data_snapshots import (
    open_pinned_table,
)
from trading_advisor_3000.product_plane.common.data_snapshots import (
    recovery_iso as iso,
)
from trading_advisor_3000.product_plane.common.data_snapshots import (
    recovery_stamp as stamp,
)

from .canonical_bars_job import _create_spark_session

# Semantic effect starts, never ingestion/publication timestamps. Comparing both
# directions includes deleted rows and the old start when an interval end changes.
EFFECT_COLUMNS = {
    "canonical_bars": "ts",
    "canonical_bar_provenance": "bar_start_ts",
    "canonical_session_calendar": "session_open_ts",
    "canonical_closed_session_dates": "session_date",
    "canonical_session_intervals": "expected_open_ts",
    "canonical_roll_map": "session_date",
    "canonical_contract_economics": "effective_from_ts",
    "canonical_fx_rates": "rate_date",
    "canonical_asset_risk_parameters": "risk_session_date",
    "raw_moex_history": "ts_open",
    "raw_moex_daily_open_interest": "trade_date",
    "raw_moex_session_schedule": "trade_date",
    "raw_moex_contract_securities": "trade_date",
    "prepared_raw_moex_contract_securities": "trade_date",
    "canonical_contract_parameters": "trade_date",
    "raw_moex_indicative_fx_rates": "trade_date",
    "raw_moex_rms_limits": "trade_date",
    "raw_moex_rms_staticparams": "trade_date",
}


def _hash(value):
    import hashlib

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _actions(table):
    protocol = table.protocol()
    if (
        protocol.min_reader_version > 1
        or protocol.min_writer_version > 2
        or protocol.reader_features
        or protocol.writer_features
        or any(
            key.startswith(
                ("delta.columnMapping", "delta.enableDeletion", "delta.enableRowTracking")
            )
            for key in table.metadata().configuration
        )
    ):
        raise ValueError("recovery does not support enhanced Delta table features")
    return {row["path"]: row for row in table.get_add_actions(flatten=True).to_pylist()}


def _read_partition_union(spark, reference, actions, selected):
    from pyspark.sql import functions as F

    frame = (
        spark.read.format("delta")
        .option("versionAsOf", reference["delta_version"])
        .load(reference["table_path"])
    )
    for column, values in selected.items():
        if not values or None in values:
            raise ValueError("recovery requires explicit nonnull native partition values")
        frame = frame.where(F.col(column).isin(sorted(values)))
    files = [
        row
        for row in actions.values()
        if all(row["partition." + col] in values for col, values in selected.items())
    ]
    if any(row["num_records"] is None for row in files):
        raise ValueError("recovery requires complete native row statistics")
    expected = sum(row["num_records"] for row in files)
    actual = frame.count()
    if actual != expected:
        raise ValueError("recovery native reader did not cover the complete partition union")
    return frame, actual


def verify_cf_retained_rows(research, checkpoint, windows, *, spark_session_factory=None):
    """Compare all rows outside the admitted suffix at the two native output versions."""
    from pyspark.sql import functions as F

    if not windows or len({(w["start_ts"], w["end_ts"]) for w in windows}) != 1:
        raise ValueError("CF recovery requires one proven time range across its Data domain")
    lower = stamp(windows[0]["start_ts"]).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=7)
    upper = stamp(windows[0]["end_ts"])
    factory = spark_session_factory or _create_spark_session
    spark = factory("ta3000-cf-retained-rows", "local[2]")
    try:
        pairs = spark.createDataFrame(
            [(w["instrument_id"], w["timeframe"]) for w in windows], ["instrument_id", "timeframe"]
        ).withColumn("_admitted", F.lit(True))
        clocks = {
            "continuous_front_bars": ("ts",),
            "continuous_front_roll_events": (
                "decision_ts",
                "effective_ts",
                "last_old_bar_ts",
                "first_new_bar_ts",
            ),
            "continuous_front_adjustment_ladder": ("effective_ts",),
            "continuous_front_qc_report": (),
        }
        for name, times in clocks.items():
            refs = (research["tables"][name], checkpoint[name])
            if any(
                refs[0][k] != refs[1][k]
                for k in ("table_id", "table_path", "schema_hash", "partition_columns")
            ):
                raise ValueError("CF recovery output contract differs: " + name)
            frames = []
            for ref in refs:
                open_pinned_table(ref)
                frame = (
                    spark.read.format("delta")
                    .option("versionAsOf", ref["delta_version"])
                    .load(ref["table_path"])
                )
                covered = F.col("_admitted") & (
                    F.col("dataset_version") == research["dataset_version"]
                )
                for column in times:
                    covered = (
                        covered & (F.col(column) >= F.lit(lower)) & (F.col(column) < F.lit(upper))
                    )
                retained = (
                    frame.join(F.broadcast(pairs), ["instrument_id", "timeframe"], "left")
                    .where(~F.coalesce(covered, F.lit(False)))
                    .select(*frame.columns)
                )
                # Only this reviewed consumer kernel ignores these two producer bookkeeping fields.
                if (
                    name == "continuous_front_bars"
                    and research["parameters"]["semantic_compute_contract"]["digest"]
                    == "47df71fdf1fad0d6d866f5ca5c78ed638673194859e4af8173eab68d925e7dc8"
                ):
                    retained = retained.drop("input_row_count", "created_at")
                frames.append(retained)
            left, right = frames
            if left.exceptAll(right).unionByName(right.exceptAll(left)).limit(1).count():
                raise ValueError("CF checkpoint changed retained Research inputs: " + name)
    finally:
        spark.stop()


def compare_snapshot_tables(
    *, source_tables, target_tables, timeframes, not_before, spark_session_factory=None
):
    """No writes. Full changed partitions preserve Delta semantics and duplicates."""
    from pyspark.sql import functions as F

    if set(source_tables) != set(target_tables) or "canonical_bars" not in source_tables:
        raise ValueError("recovery requires an unchanged Data table set")
    lower = stamp(not_before)
    factory = spark_session_factory or _create_spark_session
    spark = factory("ta3000-data-scope-recovery", "local[2]")
    results, starts, ends = {}, [], []
    try:
        for name, reference in target_tables.items():
            previous = source_tables[name]
            if any(
                previous.get(key) != reference.get(key)
                for key in (
                    "table_path",
                    "table_id",
                    "schema_hash",
                    "layout_version",
                    "partition_columns",
                )
            ):
                raise ValueError("recovery Data contract changed: " + name)
            old, new = open_pinned_table(previous), open_pinned_table(reference)
            before, after = _actions(old), _actions(new)
            record = {
                "source_ref_sha256": _hash(previous),
                "target_ref_sha256": _hash(reference),
                "mode": "identity",
                "read_rows_before": 0,
                "read_rows_after": 0,
                "changed_rows": 0,
                "start_ts": None,
                "end_ts": None,
            }
            if before != after:
                selected = {column: set() for column in old.metadata().partition_columns}
                if list(selected) != list(new.metadata().partition_columns):
                    raise ValueError("recovery partition contract changed")
                for actions, other in ((before, after), (after, before)):
                    for path, action in actions.items():
                        if action != other.get(path):
                            for column in selected:
                                selected[column].add(action["partition." + column])
                left, nleft = _read_partition_union(spark, previous, before, selected)
                right, nright = _read_partition_union(spark, reference, after, selected)
                if left.schema != right.schema:
                    raise ValueError("recovery reader schema changed")
                columns = left.columns
                delta = (
                    left.select(*columns)
                    .exceptAll(right.select(*columns))
                    .unionByName(right.select(*columns).exceptAll(left.select(*columns)))
                )
                effect = EFFECT_COLUMNS.get(name)
                if effect is None or effect not in columns:
                    if delta.limit(1).count():
                        raise ValueError("unproven recovery effect time: " + name)
                    count, start, end = 0, None, None
                else:
                    instant = F.col(effect).cast("timestamp")
                    row = delta.agg(
                        F.count("*").alias("n"),
                        F.sum(F.when(instant.isNull(), 1).otherwise(0)).alias("invalid"),
                        F.min(instant).alias("start"),
                        F.max(instant).alias("end"),
                    ).first()
                    if row["invalid"]:
                        raise ValueError("null or unparseable recovery effect time: " + name)
                    count, start, end = row["n"], row["start"], row["end"]
                if count:
                    if stamp(start) < lower:
                        raise ValueError("recovery change precedes explicit lower bound: " + name)
                    starts.append(stamp(start))
                    ends.append(stamp(end))
                record.update(
                    mode="native_diff",
                    read_rows_before=nleft,
                    read_rows_after=nright,
                    changed_rows=count,
                    start_ts=iso(start) if count else None,
                    end_ts=iso(end) if count else None,
                )
            results[name] = record
        # Both domains are required: deleted pairs and old aliases remain dirty.
        frames = [
            spark.read.format("delta")
            .option("versionAsOf", tables["canonical_bars"]["delta_version"])
            .load(tables["canonical_bars"]["table_path"])
            .select("instrument_id", "timeframe")
            for tables in (source_tables, target_tables)
        ]
        pairs = (
            frames[0]
            .unionByName(frames[1])
            .where(F.col("timeframe").isin(list(timeframes)))
            .distinct()
            .collect()
        )
        if not pairs or any(not row["instrument_id"] or not row["timeframe"] for row in pairs):
            raise ValueError("recovery has no proven canonical domain")
        actions = _actions(open_pinned_table(source_tables["canonical_bars"]))
        prefix = sum(
            row["num_records"]
            for row in actions.values()
            if row["num_records"]
            and row.get("max.ts") is not None
            and stamp(row["max.ts"]) < lower - timedelta(days=7)
        )
        if not prefix:
            raise ValueError("recovery cannot prove a retained historical prefix")
        start = min(starts) if starts else None
        end = max(ends) + timedelta(days=1) if ends else None
        windows = (
            [
                {
                    "instrument_id": row["instrument_id"],
                    "timeframe": row["timeframe"],
                    "start_ts": iso(start),
                    "end_ts": iso(end),
                }
                for row in sorted(pairs, key=lambda row: (row["instrument_id"], row["timeframe"]))
            ]
            if start
            else []
        )
        return {
            "tables": results,
            "windows": windows,
            "preserved_prefix_rows": prefix,
            "spark_version": spark.version,
        }
    finally:
        spark.stop()
