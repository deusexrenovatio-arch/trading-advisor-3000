"""Spark/Delta writer for immutable, unpublished native observation candidates."""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from uuid import uuid4

from trading_advisor_3000.product_plane.common.data_snapshots import (
    atomic_json,
    capture_table,
    create_snapshot,
    save_snapshot,
)
from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract

from ..native_observations import (
    completed_candidate,
    observation_metadata,
    validate_candidate_graph,
)
from .observation_acquisition import canonical_bytes, pin, pinned_bytes, verify_source_bundle
from .observation_validation import compare_source_to_physical


def exact_decimal_lexeme(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("unsupported/nonfinite numeric source value")
    with localcontext() as context:
        context.prec = max(50, len(value.as_tuple().digits) + 12)
        try:
            exact = value.quantize(Decimal("0.0000000001"))
        except InvalidOperation as exc:
            raise ValueError("source decimal cannot fit decimal(38,10)") from exc
    if exact != value or value.copy_abs() >= Decimal(10) ** 28:
        raise ValueError("source decimal would round or overflow")
    return str(value)


def intake_records(root: Path, source: dict, assignments: dict):
    """Bounded network parsing only; Spark owns analytical transforms/aggregation."""
    bindings = {b["source_symbol"]: b for b in assignments["bindings"]}
    numbers = ["open", "high", "low", "close", "volume", "open_interest"]
    for page in source["pages"]:
        payload = json.loads(
            pinned_bytes(root, page["body"]),
            parse_float=Decimal,
            parse_int=Decimal,
            parse_constant=str,
        )
        block, binding = payload["candles"], bindings[page["source_symbol"]]
        columns = block["columns"]
        for ordinal, values in enumerate(block["data"]):
            fields = dict(zip(columns, values)) if isinstance(values, list) else {}
            reason = (
                None if isinstance(values, list) and len(values) == len(columns) else "row_width"
            )
            begin, end = fields.get("begin"), fields.get("end")
            try:
                if any(
                    not isinstance(key, str)
                    or not re.fullmatch(
                        r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", key
                    )
                    for key in [begin, end]
                ):
                    raise ValueError("source_key_type")
                datetime.strptime(begin, "%Y-%m-%d %H:%M:%S")
                datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
                if (
                    begin > end
                    or not source["window"]["date_from"]
                    <= begin[:10]
                    <= source["window"]["date_till"]
                ):
                    raise ValueError("source_key_window")
            except (TypeError, ValueError):
                reason = reason or "source_key"
            row = {
                "namespace": assignments["namespace"]["namespace"],
                "asset_id": binding["asset_id"],
                "instrument_id": binding["instrument_id"],
                "provider": binding["context"]["provider"],
                "venue": binding["context"]["venue"],
                "engine": binding["engine"],
                "market": binding["context"]["market"],
                "board": binding["context"]["board"],
                "source_symbol": page["source_symbol"],
                "timeframe": page["timeframe"],
                "source_begin": begin if isinstance(begin, str) else None,
                "source_end": end if isinstance(end, str) else None,
                "source_values_json": json.dumps(
                    values,
                    default=str,
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
                "body_sha256": page["body"]["sha256"],
                "receipt_sha256": page["receipt"]["sha256"],
                "source_block": "candles",
                "page_start": page["page_start"],
                "row_ordinal": ordinal,
            }
            for name in numbers:
                try:
                    row[name + "_lexeme"] = exact_decimal_lexeme(fields.get(name))
                    if name in numbers[:4] and row[name + "_lexeme"] is None:
                        reason = reason or "missing_ohlc"
                except ValueError:
                    row[name + "_lexeme"] = None
                    reason = reason or "numeric:" + name
            row["rejection_reason"] = reason
            yield row


def producer_code_sha256() -> str:
    from trading_advisor_3000.product_plane.contracts import identity_v3

    from .. import native_observations
    from . import iss_client, observation_acquisition, observation_validation

    paths = [
        Path(module.__file__)
        for module in [
            identity_v3,
            native_observations,
            iss_client,
            observation_acquisition,
            observation_validation,
        ]
    ] + [Path(__file__), identity_v3.SCHEMAS / "market_identity.v3.json"]
    return hashlib.sha256(
        canonical_bytes(
            {str(path.name): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        )
    ).hexdigest()


def build_observation_command(
    root: Path,
    *,
    command_id: str,
    source_pin: dict,
    assignment_pin: dict,
    parent: dict | None = None,
    overlay: dict | None = None,
) -> dict:
    source, assignments = verify_source_bundle(root, source_pin, assignment_pin)
    metadata = observation_metadata(source_pin, assignment_pin, assignments["namespace"])
    metadata_path = root / "source" / "observation-metadata.json"
    atomic_json(metadata_path, metadata, immutable=True)
    mandatory = metadata["policies"]["mandatory_read_columns"]
    columns = [
        f["name"]
        for f in metadata["storage_layout"]["native_bars"]["fields"]
        if f["name"] not in mandatory
    ]
    start = source["window"]["date_from"] + " 00:00:00"
    end = (
        date.fromisoformat(source["window"]["date_till"]) + timedelta(days=1)
    ).isoformat() + " 00:00:00"
    selection = [
        {
            "schema_version": 1,
            "input_mode": "native_observations.v1",
            "namespace": assignments["namespace"]["namespace"],
            "instrument_id": b["instrument_id"],
            "context": b["context"],
            "engine": b["engine"],
            "series_type": "native",
            "timeframe": timeframe,
            "source_begin_from": start,
            "source_begin_to": end,
            "columns": columns,
            "row_limit": 5000,
            "byte_limit": 16777216,
        }
        for b in assignments["bindings"]
        for timeframe in ["1m", "1d"]
    ]
    result = {
        "schema_version": 1,
        "input_mode": "native_observations.v1",
        "command_id": command_id,
        "operation": "baseline" if overlay is None else "simulated_correction",
        "source_manifest": source_pin,
        "assignments": assignment_pin,
        "metadata": pin(root, metadata_path),
        "code_sha256": producer_code_sha256(),
        "selection": selection,
        "parent": parent,
        "overlay": overlay,
    }
    validate_identity_contract(
        "native_observation_intent.v1.json", result, assignments["namespace"]
    )
    return result


@contextmanager
def _command_lock(path: Path):
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _spark_schema(layout):
    from pyspark.sql import types as T

    types = {
        "string": T.StringType(),
        "bigint": T.LongType(),
        "decimal(38,10)": T.DecimalType(38, 10),
    }
    return T.StructType(
        [T.StructField(f["name"], types[f["sql_type"]], f["nullable"]) for f in layout["fields"]]
    )


def _frames(spark, root, source, assignments, metadata, overlay, overlay_sha256):
    from pyspark.sql import functions as F

    layouts = metadata["storage_layout"]
    raw = spark.createDataFrame(
        list(intake_records(root, source, assignments)),
        schema=_spark_schema(layouts["raw_observations"]),
    )
    prices = {
        n: F.col(n + "_lexeme").cast("decimal(38,10)")
        for n in ["open", "high", "low", "close", "volume", "open_interest"]
    }
    bad_bounds = (
        (prices["low"] > F.least(prices["open"], prices["close"]))
        | (prices["high"] < F.greatest(prices["open"], prices["close"]))
        | (prices["low"] > prices["high"])
    )
    bad_quantity = (prices["volume"] < 0) | (prices["open_interest"] < 0)
    raw = raw.withColumn(
        "rejection_reason",
        F.when(F.col("rejection_reason").isNotNull(), F.col("rejection_reason"))
        .when(bad_bounds, F.lit("price_bounds"))
        .when(bad_quantity, F.lit("negative_quantity")),
    )
    kinds = F.create_map(
        *[
            value
            for b in assignments["bindings"]
            for value in [F.lit(b["instrument_id"]), F.lit(b["kind"])]
        ]
    )[F.col("instrument_id")]
    units = F.create_map(
        *[
            value
            for b in assignments["bindings"]
            for value in [F.lit(b["instrument_id"]), F.lit(canonical_bytes(b["units"]).decode())]
        ]
    )[F.col("instrument_id")]
    expressions = {
        f["name"]: F.col(f["name"])
        for f in layouts["native_bars"]["fields"]
        if f["name"] in raw.columns
    }
    expressions.update(
        {name: value for name, value in prices.items() if name in ["open", "high", "low", "close"]}
    )
    for name in ["volume", "open_interest"]:
        not_applicable = kinds == "reference_index"
        if name == "open_interest":
            not_applicable = not_applicable | (kinds == "equity")
        state = (
            F.when(not_applicable, "not_applicable")
            .when(F.col(name + "_lexeme").isNull(), "unavailable")
            .otherwise("known")
        )
        expressions[name] = F.when(not_applicable, F.lit(None).cast("decimal(38,10)")).otherwise(
            prices[name]
        )
        expressions[name + "_state"] = state
        expressions[name + "_null_reason"] = (
            F.when(state == "known", F.lit(None).cast("string"))
            .when(not_applicable, "instrument_kind")
            .otherwise("not_reported_by_source")
        )
    expressions.update(
        source_units_json=units,
        normalization_recipe=F.lit(metadata["normalization_recipe"]),
        value_origin=F.lit("exchange_observation"),
        simulation_id=F.lit(None).cast("string"),
        overlay_sha256=F.lit(None).cast("string"),
    )
    bars = raw.filter(F.col("rejection_reason").isNull()).select(
        *[expressions[f["name"]].alias(f["name"]) for f in layouts["native_bars"]["fields"]]
    )
    if overlay:
        if not overlay.get("simulation_id") or not overlay.get("changes"):
            raise ValueError("named simulation and exact changes required")
        seen = set()
        for change in overlay["changes"]:
            if change["field"] != "volume" or set(change["key"]) != set(
                layouts["native_bars"]["grain"]
            ):
                raise ValueError("simulation must identify one exact native volume key")
            key_bytes = canonical_bytes(change["key"])
            if key_bytes in seen:
                raise ValueError("duplicate simulation key")
            seen.add(key_bytes)
            predicate = F.lit(True)
            for field, value in change["key"].items():
                predicate = predicate & (F.col(field) == F.lit(value))
            if bars.filter(predicate & (F.col("volume_state") == "known")).count() != 1:
                raise ValueError("simulation target absent, ambiguous or inapplicable")
            value = exact_decimal_lexeme(Decimal(change["new_value"]))
            if value is None or Decimal(value) < 0:
                raise ValueError("invalid simulated quantity")
            bars = (
                bars.withColumn(
                    "volume",
                    F.when(predicate, F.lit(value).cast("decimal(38,10)")).otherwise(
                        F.col("volume")
                    ),
                )
                .withColumn(
                    "value_origin",
                    F.when(predicate, "simulated_correction").otherwise(F.col("value_origin")),
                )
                .withColumn(
                    "simulation_id",
                    F.when(predicate, F.lit(overlay["simulation_id"])).otherwise(
                        F.col("simulation_id")
                    ),
                )
                .withColumn(
                    "overlay_sha256",
                    F.when(predicate, F.lit(overlay_sha256)).otherwise(F.col("overlay_sha256")),
                )
            )
    quarantine = raw.filter(F.col("rejection_reason").isNotNull()).select(
        *[f["name"] for f in layouts["quarantine"]["fields"]]
    )
    grouped = bars.withColumn("source_day", F.substring("source_begin", 1, 10))
    keys = [
        f["name"]
        for f in layouts["observed_day_qc"]["fields"]
        if f["name"] in grouped.columns
        and f["name"]
        not in [
            "bar_count",
            "first_source_begin",
            "last_source_begin",
            "volume_sum",
            "simulated_rows",
        ]
    ]
    qc = (
        grouped.groupBy(*keys)
        .agg(
            F.count(F.lit(1)).alias("bar_count"),
            F.min("source_begin").alias("first_source_begin"),
            F.max("source_begin").alias("last_source_begin"),
            F.sum("volume").cast("decimal(38,10)").alias("volume_sum"),
            F.sum(F.when(F.col("value_origin") == "simulated_correction", 1).otherwise(0))
            .cast("bigint")
            .alias("simulated_rows"),
        )
        .select(*[f["name"] for f in layouts["observed_day_qc"]["fields"]])
    )
    frames = {
        "raw_observations": raw,
        "native_bars": bars,
        "observed_day_qc": qc,
        "quarantine": quarantine,
    }
    counts = {name: frame.count() for name, frame in frames.items()}
    if (
        counts["raw_observations"] != source["source_rows"]
        or counts["native_bars"] + counts["quarantine"] != source["source_rows"]
    ):
        raise ValueError("source/quarantine row accounting mismatch")
    for name, frame in frames.items():
        if (
            frame.groupBy(*layouts[name]["grain"])
            .count()
            .filter(F.col("count") != 1)
            .limit(1)
            .count()
        ):
            raise ValueError("duplicate observation grain: " + name)
    return frames, counts


def _write_once(spark, path: Path, frame, layout):
    if path.exists():
        raise ValueError("observation table destination already exists")
    ddl = ", ".join(
        "`" + f["name"] + "` " + f["sql_type"] + ("" if f["nullable"] else " NOT NULL")
        for f in layout["fields"]
    )
    spark.sql(f"CREATE TABLE delta.`{path.as_posix()}` ({ddl}) USING DELTA")
    if frame.limit(1).count():
        frame.write.format("delta").mode("append").save(path.as_posix())
    return capture_table(path)


def materialize_observation_command(
    root: Path, intent: dict, *, spark=None, fault: str | None = None
) -> dict:
    """One run-local command. Immutable receipts fence replay; no head/publisher exists."""
    root = root.resolve()
    if root != Path("/trial") or not (root / "scope.json").is_file():
        raise ValueError("writer requires the explicitly mounted isolated trial root")
    if any((root / name).exists() for name in ["published_current.json", "accepted_current.json"]):
        raise ValueError("publication pointers are forbidden in the observation trial")
    if fault not in {
        None,
        "after_raw_observations",
        "after_native_bars",
        "after_snapshot",
        "after_completion",
    }:
        raise ValueError("unknown controlled fault point")
    source, assignments = verify_source_bundle(
        root, intent["source_manifest"], intent["assignments"]
    )
    metadata = json.loads(pinned_bytes(root, intent["metadata"]))
    validate_identity_contract(
        "native_observation_intent.v1.json", intent, assignments["namespace"]
    )
    validate_identity_contract(
        "native_observation_snapshot.v1.json", metadata, assignments["namespace"]
    )
    expected = build_observation_command(
        root,
        command_id=intent["command_id"],
        source_pin=intent["source_manifest"],
        assignment_pin=intent["assignments"],
        parent=intent["parent"],
        overlay=intent["overlay"],
    )
    # A committed old command keeps its input identity across a reader/writer upgrade.
    expected["code_sha256"] = intent["code_sha256"]
    if expected != intent:
        raise ValueError("command selection/metadata is outside acquired source scope")
    input_hash = hashlib.sha256(canonical_bytes(intent)).hexdigest()
    command_key = hashlib.sha256(intent["command_id"].encode()).hexdigest()
    command_dir = root / "commands" / command_key
    with _command_lock(command_dir / "writer.lock"):
        intent_path, completion_path = command_dir / "intent.json", command_dir / "completion.json"
        if intent_path.exists() and json.loads(intent_path.read_bytes()) != intent:
            raise ValueError("same command identity has conflicting canonical input")
        if not intent_path.exists():
            atomic_json(intent_path, intent, immutable=True)
        intent_pin = pin(root, intent_path)
        if completion_path.exists():
            result_pin = pin(root, completion_path)
            graph = completed_candidate(root, result_pin)
            if graph["completion"]["input_sha256"] != input_hash:
                raise ValueError("command completion belongs to different input")
            return {"completion": result_pin, "replayed": True, "recovered": False}
        parent = completed_candidate(root, intent["parent"]) if intent["parent"] else None
        overlay = json.loads(pinned_bytes(root, intent["overlay"])) if intent["overlay"] else None
        if overlay:
            validate_identity_contract(
                "native_observation_overlay.v1.json", overlay, assignments["namespace"]
            )
        if overlay and (
            overlay.get("parent_completion_sha256") != intent["parent"]["sha256"]
            or parent["metadata"] != metadata
        ):
            raise ValueError("simulation changes its explicitly pinned source/parent")
        # This trial supports one named overlay over an original acquisition.
        if parent and parent["simulations"]:
            raise ValueError("stacked simulation overlays require separate admission")
        attempts = root / "attempts" / command_key
        attempts.mkdir(parents=True, exist_ok=True)
        complete = []
        for path in sorted(attempts.glob("*/manifests/*.json")):
            candidate_pin = pin(root, path)
            graph = validate_candidate_graph(
                root,
                manifest_pin=candidate_pin,
                metadata_pin=intent["metadata"],
                source_pin=intent["source_manifest"],
                assignment_pin=intent["assignments"],
                intent_pin=intent_pin,
                expected_input_sha256=input_hash,
            )
            proof = compare_source_to_physical(
                root,
                source=source,
                assignments=assignments,
                metadata=metadata,
                tables=graph["manifest"]["tables"],
                overlay=overlay,
                overlay_sha256=intent["overlay"]["sha256"] if overlay else None,
            )
            complete.append((path.parent.parent.name, candidate_pin, graph["manifest"], proof))
        if len(complete) > 1:
            raise ValueError("ambiguous saved complete graphs for one command")
        recovered = bool(complete)
        owned_spark = False
        try:
            if recovered:
                attempt_id, snapshot_pin, manifest, proof = complete[0]
            else:
                if intent["code_sha256"] != producer_code_sha256():
                    raise ValueError("trial producer code changed before an uncommitted command")
                if spark is None:
                    from trading_advisor_3000.spark_jobs.canonical_bars_job import (
                        _create_spark_session,
                    )

                    spark = _create_spark_session("unpublished-native-observations", "local[2]")
                    spark.conf.set("spark.sql.ansi.enabled", "true")
                    owned_spark = True
                attempt_id = uuid4().hex
                attempt = attempts / attempt_id
                attempt.mkdir()
                frames, counts = _frames(
                    spark,
                    root,
                    source,
                    assignments,
                    metadata,
                    overlay,
                    intent["overlay"]["sha256"] if overlay else None,
                )
                tables = {}
                for name in ["raw_observations", "native_bars", "observed_day_qc", "quarantine"]:
                    tables[name] = (
                        parent["manifest"]["tables"][name]
                        if parent and name in {"raw_observations", "quarantine"}
                        else _write_once(
                            spark,
                            attempt / (name + ".delta"),
                            frames[name],
                            metadata["storage_layout"][name],
                        )
                    )
                    if fault == "after_" + name:
                        raise RuntimeError("injected partial failure after " + name)
                proof = compare_source_to_physical(
                    root,
                    source=source,
                    assignments=assignments,
                    metadata=metadata,
                    tables=tables,
                    overlay=overlay,
                    overlay_sha256=intent["overlay"]["sha256"] if overlay else None,
                )
                manifest = create_snapshot(
                    tables=tables,
                    required_tables=list(metadata["storage_layout"]),
                    dataset_version="native_observations.v1",
                    run_id=attempt_id,
                    code_version=intent["code_sha256"],
                    quality_checks={
                        name: "PASS"
                        for name in [
                            "source_integrity",
                            "namespace",
                            "grain",
                            "accounting",
                            "decimal_oracle",
                            "scope",
                        ]
                    },
                    parameters={
                        "input_sha256": input_hash,
                        "metadata_sha256": intent["metadata"]["sha256"],
                        "source_manifest_sha256": intent["source_manifest"]["sha256"],
                        "intent_sha256": intent_pin["sha256"],
                        "source_rows": source["source_rows"],
                        "row_counts": counts,
                        "oracle": proof,
                    },
                    window=source["window"],
                    parents=[parent["manifest"]["snapshot_id"]] if parent else [],
                )
                snapshot_path = save_snapshot(attempt, manifest)
                snapshot_pin = pin(root, snapshot_path)
                validate_candidate_graph(
                    root,
                    manifest_pin=snapshot_pin,
                    metadata_pin=intent["metadata"],
                    source_pin=intent["source_manifest"],
                    assignment_pin=intent["assignments"],
                    intent_pin=intent_pin,
                    expected_input_sha256=input_hash,
                )
                if fault == "after_snapshot":
                    raise RuntimeError("injected failure after saved graph before completion")
            if not recovered and producer_code_sha256() != intent["code_sha256"]:
                raise ValueError("producer code changed while candidate was materializing")
            receipt = {
                "schema_version": 1,
                "contract_version": "native_observation_completion.v1",
                "status": "complete_unpublished",
                "command_id": intent["command_id"],
                "input_sha256": input_hash,
                "attempt_id": attempt_id,
                "snapshot": {**snapshot_pin, "snapshot_id": manifest["snapshot_id"]},
                "metadata": intent["metadata"],
                "source_manifest": intent["source_manifest"],
                "assignments": intent["assignments"],
                "intent": intent_pin,
                "parent": intent["parent"],
                "overlay": intent["overlay"],
                "created_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            validate_identity_contract(
                "native_observation_completion.v1.json", receipt, assignments["namespace"]
            )
            atomic_json(completion_path, receipt, immutable=True)
            result_pin = pin(root, completion_path)
            completed_candidate(root, result_pin)
            if fault == "after_completion":
                raise RuntimeError("injected lost acknowledgement after completion")
            return {
                "completion": result_pin,
                "replayed": False,
                "recovered": recovered,
                "oracle": proof,
            }
        finally:
            if owned_spark:
                spark.stop()
