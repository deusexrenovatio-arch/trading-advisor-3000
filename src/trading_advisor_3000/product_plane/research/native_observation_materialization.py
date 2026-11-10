"""Bounded existing-kernel L0 materialization; immutable, unpublished commands."""

from __future__ import annotations

import hashlib
import json
import platform
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from trading_advisor_3000.product_plane.common.data_snapshots import (
    atomic_json,
    create_snapshot,
    save_snapshot,
    write_delta_once,
)
from trading_advisor_3000.product_plane.common.immutable_files import command_lock, pin
from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    SCHEMAS,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    NativeObservationSnapshot,
    contract_constants,
)
from trading_advisor_3000.spark_jobs.research_bar_views_job import _with_l0_metrics

from .native_observations import (
    NativeResearchSnapshot,
    capabilities_from_counts,
    l0_constants,
    l0_layouts,
    require,
    sha,
    tree_rows,
    validate_l0_context,
    validate_l0_graph,
)
from .semantic_fingerprint import SymbolGraph


def _native_l0_frame(
    frame,
    *,
    source_fields,
    additional_fields,
    series_columns,
    analysis_start,
    context_id,
    recipe_version,
):
    from pyspark.sql import functions as F

    working = frame.withColumn("ts", F.col("source_begin"))
    for name in ["open", "high", "low", "close"]:
        working = working.withColumn("_source_" + name, F.col(name)).withColumn(
            name, F.col(name).cast("double")
        )
    derived = _with_l0_metrics(working, series_columns=series_columns, start_ts=None, warmup_bars=0)
    for name in ["open", "high", "low", "close"]:
        derived = derived.withColumn(name, F.col("_source_" + name))
    derived = (
        derived.withColumn("research_context_id", F.lit(context_id))
        .withColumn("research_recipe_version", F.lit(recipe_version))
        .withColumn(
            "slice_role",
            F.when(F.col("source_begin") < analysis_start, "warmup").otherwise("analysis"),
        )
        .withColumn("execution_eligible", F.lit(False))
    )
    for name in ["execution_open", "execution_high", "execution_low", "execution_close"]:
        derived = derived.withColumn(name, F.lit(None).cast("decimal(38,10)"))
    for field in additional_fields:
        name = field["name"]
        if field["sql_type"] == "double":
            value = F.col(name)
            derived = derived.withColumn(
                name,
                F.when(value.isNotNull() & ~F.isnan(value) & (F.abs(value) < float("inf")), value)
                .otherwise(F.lit(None))
                .cast("double"),
            )
    return derived.select(
        *source_fields,
        *[F.col(f["name"]).cast(f["sql_type"]).alias(f["name"]) for f in additional_fields],
    )


def numerical_identity():
    package = Path(__file__).resolve().parents[2]
    roots = {
        "l0": "trading_advisor_3000.spark_jobs.research_bar_views_job._with_l0_metrics",
        "adapter": (
            "trading_advisor_3000.product_plane.research."
            "native_observation_materialization._native_l0_frame"
        ),
    }
    result = {
        "version": "native_research_numerical.v1",
        "basis": "observed_price_sequence.v1",
        "policy_sha256": sha(l0_constants()["policy"]),
        "kernels": {
            key: SymbolGraph(package).fingerprint((symbol,)) for key, symbol in roots.items()
        },
        "engines": {
            "spark": version("pyspark"),
            "delta": version("delta-spark"),
            "arrow": version("pyarrow"),
            "python": platform.python_version(),
        },
    }
    return {**result, "digest": sha(result)}


def producer_code_sha256():
    from trading_advisor_3000.product_plane.common import data_snapshots, immutable_files
    from trading_advisor_3000.product_plane.contracts import identity_v3
    from trading_advisor_3000.product_plane.data_plane import native_observations as md
    from trading_advisor_3000.product_plane.data_plane.moex import (
        observation_acquisition,
        observation_validation,
    )
    from trading_advisor_3000.spark_jobs import research_bar_views_job

    from . import native_observation_job, native_observations, semantic_fingerprint

    paths = [
        Path(module.__file__)
        for module in [
            data_snapshots,
            immutable_files,
            identity_v3,
            md,
            observation_acquisition,
            observation_validation,
            native_observation_job,
            native_observations,
            research_bar_views_job,
            semantic_fingerprint,
        ]
    ] + [Path(__file__), SCHEMAS / "market_identity.v3.json"]
    return sha(
        {
            path.relative_to(Path(__file__).resolve().parents[2]).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in paths
        }
    )


def build_l0_command(source_reader, *, command_id, analysis_start):
    source = source_reader.compute_metadata()
    require(
        source["source_root"] == "/trial", "Research source requires the explicit RO trial mount"
    )
    intent = {
        "schema_version": 1,
        "input_mode": "native_observation_research.v1",
        "layer": "L0",
        "command_id": command_id,
        "source_root": "/trial",
        "source_completion": source["completion"],
        "analysis_start": analysis_start,
        "policy_sha256": sha(l0_constants()["policy"]),
        "numerical_identity": numerical_identity(),
        "code_sha256": producer_code_sha256(),
    }
    validate_identity_contract("native_research_l0_intent.v1.json", intent, source["namespace"])
    return intent


def _spark_counts(frame, source):
    from pyspark.sql import functions as F

    policy = l0_constants()["policy"]
    grouping = [
        name
        for name in contract_constants()["storage_layout"]["native_bars"]["grain"]
        if name != "source_begin"
    ] + ["asset_id", "source_symbol"]
    values = [*policy["metric_requirements"], *policy["source_quantity_capabilities"]]
    aggregations = [
        F.count(F.lit(1)).alias("rows"),
        F.min("source_begin").alias("first_source_begin"),
        F.max("source_begin").alias("last_source_begin"),
        F.max_by("source_end", "source_begin").alias("last_source_end"),
    ]
    aggregations += [F.count(F.col(name)).alias(name + "_finite") for name in values]
    aggregations += [
        F.min(F.when(F.col(name + "_state") == "not_applicable", 1).otherwise(0)).alias(
            name + "_na"
        )
        for name in policy["source_quantity_capabilities"]
    ]
    result = []
    for row in frame.groupBy(*grouping).agg(*aggregations).limit(129).collect():
        sequence = {
            k: row[k]
            for k in [
                "namespace",
                "asset_id",
                "instrument_id",
                "engine",
                "source_symbol",
                "timeframe",
                "rows",
            ]
        }
        sequence["context"] = {key: row[key] for key in ["provider", "venue", "market", "board"]}
        result.append(
            {
                "sequence": sequence,
                "finite_counts": {name: row[name + "_finite"] for name in values},
                "not_applicable": {
                    name: row[name + "_na"] == 1 for name in policy["source_quantity_capabilities"]
                },
                **{
                    k: row[k]
                    for k in ["first_source_begin", "last_source_begin", "last_source_end"]
                },
            }
        )
    return result


def materialize_l0_command(root: Path, intent: dict, *, spark=None, fault=None):
    root = root.resolve()
    require(
        root == Path("/research-trial") and (root / "scope.json").is_file(),
        "Research writer requires the isolated mounted root",
    )
    require(
        not any(
            (root / name).exists() for name in ["published_current.json", "accepted_current.json"]
        ),
        "Research publication pointer forbidden",
    )
    require(
        fault in [None, "after_research_bar_views", "after_snapshot", "after_completion"],
        "unknown Research fault point",
    )
    require(
        intent.get("source_root") == "/trial",
        "Research source requires the explicit RO trial mount",
    )
    source_reader = NativeObservationSnapshot(
        Path(intent["source_root"]), intent["source_completion"]
    )
    source = source_reader.compute_metadata()
    validate_identity_contract("native_research_l0_intent.v1.json", intent, source["namespace"])
    input_hash = sha(intent)
    command_dir = root / "commands" / hashlib.sha256(intent["command_id"].encode()).hexdigest()
    with command_lock(root / "writer.lock"):
        intent_path, completion_path = command_dir / "intent.json", command_dir / "completion.json"
        require(
            not intent_path.exists() or json.loads(intent_path.read_bytes()) == intent,
            "same command identity has conflicting canonical input",
        )
        atomic_json(intent_path, intent, immutable=True)
        intent_pin = pin(root, intent_path)
        if completion_path.exists():
            result_pin = pin(root, completion_path)
            graph = NativeResearchSnapshot(root, result_pin).verify()
            require(
                graph["completion"]["input_sha256"] == input_hash,
                "Research receipt belongs to different input",
            )
            return {"completion": result_pin, "replayed": True, "recovered": False}
        attempts = command_dir / "attempts"
        attempts.mkdir(parents=True, exist_ok=True)
        complete = []
        for path in sorted(attempts.glob("*/manifests/*.json")):
            graph = validate_l0_graph(
                root,
                snapshot_pin=pin(root, path),
                context_pin=pin(root, path.parent.parent / "context.json"),
                intent_pin=intent_pin,
                input_sha256=input_hash,
            )
            complete.append((path, graph))
        require(len(complete) <= 1, "ambiguous saved complete Research graphs")
        recovered = bool(complete)
        owned_spark = False
        cached = None
        try:
            if recovered:
                snapshot_path, graph = complete[0]
                manifest = graph["manifest"]
                context_pin = pin(root, snapshot_path.parent.parent / "context.json")
                attempt_id = manifest["run_id"]
            else:
                expected = build_l0_command(
                    source_reader,
                    command_id=intent["command_id"],
                    analysis_start=intent["analysis_start"],
                )
                require(
                    expected == intent, "Research uncommitted command policy/numerical/code changed"
                )
                if spark is None:
                    from trading_advisor_3000.spark_jobs.canonical_bars_job import (
                        _create_spark_session,
                    )

                    spark = _create_spark_session("unpublished-native-research-l0", "local[2]")
                    spark.conf.set("spark.sql.ansi.enabled", "true")
                    owned_spark = True
                attempt_id = uuid4().hex
                attempt = attempts / attempt_id
                attempt.mkdir()
                frame, source = source_reader.read_spark(spark)
                constants = l0_constants()
                policy = constants["policy"]
                native = contract_constants()["storage_layout"]["native_bars"]
                cached = _native_l0_frame(
                    frame,
                    source_fields=[f["name"] for f in native["fields"]],
                    additional_fields=policy["additional_fields"],
                    series_columns=tuple(k for k in native["grain"] if k != "source_begin"),
                    analysis_start=intent["analysis_start"],
                    context_id="pending",
                    recipe_version=policy["version"],
                ).persist()
                counts = _spark_counts(cached, source)
                context = {
                    "schema_version": 1,
                    "contract_version": "native_research_l0_context.v1",
                    "input_mode": "native_observation_research.v1",
                    "layer": "L0",
                    "source": source,
                    "policy": policy,
                    "policy_sha256": intent["policy_sha256"],
                    "layout_sha256": sha(l0_layouts()),
                    "numerical_identity": intent["numerical_identity"],
                    "runtime_code_sha256": intent["code_sha256"],
                    "quality": {
                        "policy": constants["comparison"],
                        "sha256": sha(constants["comparison"]),
                    },
                    "analysis_start": intent["analysis_start"],
                    "capabilities": capabilities_from_counts(source, counts),
                }
                context["research_context_id"] = "NCTX:" + sha(context)
                validate_l0_context(context, source)
                context_path = attempt / "context.json"
                atomic_json(context_path, context, immutable=True)
                context_pin = pin(root, context_path)
                from pyspark.sql import functions as F

                frames = {
                    "research_bar_views": cached.withColumn(
                        "research_context_id", F.lit(context["research_context_id"])
                    )
                }
                layouts = l0_layouts()
                tree_schema = ", ".join(
                    f["name"] + " " + f["sql_type"]
                    for f in layouts["research_instrument_tree"]["fields"]
                )
                frames["research_instrument_tree"] = spark.createDataFrame(
                    tree_rows(context, counts), schema=tree_schema
                )
                tables = {}
                for name, layout in layouts.items():
                    tables[name] = write_delta_once(
                        spark, attempt / (name + ".delta"), frames[name], layout
                    )
                    if fault == "after_" + name:
                        raise RuntimeError("injected partial Research failure after " + name)
                manifest = create_snapshot(
                    tables=tables,
                    required_tables=list(layouts),
                    dataset_version="native_observation_research_l0.v1",
                    run_id=attempt_id,
                    code_version=intent["code_sha256"],
                    quality_checks={
                        key: "PASS" for key in ["source", "schema", "grain", "capabilities"]
                    },
                    parameters={
                        "input_sha256": input_hash,
                        "context": context_pin,
                        "intent": intent_pin,
                        "source_root": "/trial",
                        "source_completion": source["completion"],
                    },
                    parents=[source["snapshot"]["snapshot_id"]],
                )
                snapshot_path = save_snapshot(attempt, manifest)
                validate_l0_graph(
                    root,
                    snapshot_pin=pin(root, snapshot_path),
                    context_pin=context_pin,
                    intent_pin=intent_pin,
                    input_sha256=input_hash,
                )
                if fault == "after_snapshot":
                    raise RuntimeError("injected Research failure after graph before completion")
            require(
                recovered or producer_code_sha256() == intent["code_sha256"],
                "Research producer changed during compute",
            )
            completion = {
                "schema_version": 1,
                "contract_version": "native_research_l0_completion.v1",
                "status": "complete_unpublished",
                "layer": "L0",
                "command_id": intent["command_id"],
                "input_sha256": input_hash,
                "attempt_id": attempt_id,
                "snapshot": {**pin(root, snapshot_path), "snapshot_id": manifest["snapshot_id"]},
                "context": context_pin,
                "intent": intent_pin,
                "source_root": "/trial",
                "source_completion": source["completion"],
                "created_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            validate_identity_contract(
                "native_research_l0_completion.v1.json", completion, source["namespace"]
            )
            atomic_json(completion_path, completion, immutable=True)
            result_pin = pin(root, completion_path)
            NativeResearchSnapshot(root, result_pin).verify()
            if fault == "after_completion":
                raise RuntimeError("injected Research lost acknowledgement after commit")
            return {"completion": result_pin, "replayed": False, "recovered": recovered}
        finally:
            if cached is not None:
                cached.unpersist()
            if owned_spark:
                spark.stop()
