"""Existing native kernels over explicitly admitted observed-price partitions."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import platform
from dataclasses import asdict
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
import pandas_ta_classic as ta

from trading_advisor_3000.product_plane.common.data_snapshots import (
    atomic_json,
    create_snapshot,
    save_snapshot,
    write_delta_once,
)
from trading_advisor_3000.product_plane.common.immutable_files import (
    canonical_bytes,
    command_lock,
    pin,
)
from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract

from .compute_contract import kernel_contract
from .derived_indicators.materialize import _compute_derived_frame_unmasked
from .derived_indicators.registry import DerivedIndicatorProfile, core_v1_derived_indicator_profile
from .indicators.materialize import _compute_profile_frame_unmasked
from .indicators.registry import IndicatorProfile, core_v1_indicator_profile
from .native_features import (
    NativeFeatureSnapshot,
    feature_additional_outputs,
    feature_capabilities,
    feature_layout,
    feature_policy,
    feature_policy_reference,
    feature_quality,
    feature_requirements,
    feature_schema,
    parent_reader,
    validate_feature_context,
    validate_feature_graph,
)
from .native_observation_materialization import producer_code_sha256
from .native_observations import require, sha
from .semantic_fingerprint import SymbolGraph


def worker_engines():
    return {
        "python": platform.python_version(),
        "numpy": version("numpy"),
        "pandas": version("pandas"),
        "pandas_ta_classic": version("pandas-ta-classic"),
        "spark": version("pyspark"),
        "delta": version("delta-spark"),
        "arrow": version("pyarrow"),
        "talib_absent": importlib.util.find_spec("talib") is None
        and not bool(ta.Imports.get("talib")),
    }


def _ta_source_sha256():
    package = Path(ta.__file__).resolve().parent
    return sha(
        {
            path.relative_to(package).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(package.rglob("*.py"))
        }
    )


def core_worker_engines():
    engines = worker_engines()
    for name in ["numba", "scipy"]:
        module = importlib.import_module(name)
        require(
            module.__version__ == version(name), "optional backend distribution/module mismatch"
        )
        engines[name] = version(name)
        engines["ta_" + name + "_enabled"] = bool(ta.Imports.get(name))
    return engines


def _check_worker_engines(expected):
    core = "numba" in expected
    try:
        actual = core_worker_engines() if core else worker_engines()
        valid = (
            actual == expected
            and actual["pandas_ta_classic"] == "0.4.47"
            and actual["talib_absent"]
        )
        if core:
            runtime = feature_policy("L1", "native_core_l1.v1")["runtime_profile"]
            valid = valid and all(actual[k] == runtime[k] for k in ["numba", "scipy"])
            valid = valid and actual["ta_numba_enabled"] and actual["ta_scipy_enabled"]
            valid = valid and _ta_source_sha256() == runtime["ta_source_sha256"]
    except (ImportError, OSError, KeyError, ValueError) as exc:
        raise ValueError("unadmitted feature worker library/backend/engine profile") from exc
    if not valid:
        raise ValueError("unadmitted feature worker library/backend/engine profile")


def profile_for_policy(policy):
    core = "catalog" in policy
    if policy["parent_layer"] == "L0":
        origin = core_v1_indicator_profile()
        admitted = (
            {row["spec_id"] for row in policy["spec_admission"]} if core else policy["requirements"]
        )
        selected = tuple(spec for spec in origin.indicators if spec.indicator_id in admitted)
        profile = IndicatorProfile(
            policy["profile_version"], "Observed-price native profile", selected
        )
        actual = {
            "version": profile.version,
            "origin_profile": "core_v1",
            "indicators": [asdict(spec) for spec in selected],
        }
        original = {"indicators": [asdict(spec) for spec in origin.indicators]}
    else:
        origin = core_v1_derived_indicator_profile()
        requirements = feature_requirements(policy)
        profile = DerivedIndicatorProfile(
            policy["profile_version"],
            "Observed-price native derived profile",
            tuple(requirements),
            origin.warmup_bars if core else max(requirements.values()),
        )
        actual = {
            "version": profile.version,
            "origin_profile": "core_v1",
            "output_columns": list(profile.output_columns),
            "warmup_bars": profile.warmup_bars,
        }
        original = {
            "output_columns": list(origin.output_columns),
            "warmup_bars": origin.warmup_bars,
        }
    require(
        sha(actual) == policy["resolved_profile_sha256"]
        and sha(original) == policy["origin_profile_sha256"]
        if core
        else json.loads(canonical_bytes(actual)) == policy["profile"],
        "resolved feature factory differs from admitted policy",
    )
    return profile


def _require_core_inputs(columns, policy):
    produced = set(feature_additional_outputs(policy)) if policy["layer"] == "L2" else set()
    required = {
        name
        for row in policy["catalog"]
        if row["materialized"]
        for name in row["required_inputs"]
        if name not in produced
    } | {"open", "high", "low", "close", "volume_state", "open_interest_state"}
    if policy["layer"] == "L2":
        required.add("atr_14")
    require(
        required <= set(columns),
        "missing core input dependencies: " + ", ".join(sorted(required - set(columns))),
    )


def _quantity_available(frame, quantities):
    return all(frame[name + "_state"].eq("known").all() for name in quantities)


def _compute_l1_group(frame, profile, requirements, expected_engines, grain):
    _check_worker_engines(expected_engines)
    ordered = frame.sort_values("source_begin", kind="stable").reset_index(drop=True)
    if ordered.duplicated(grain).any():
        raise ValueError("duplicate feature group grain")
    core = profile.version == "native_core_l1.v1"
    policy = feature_policy("L1", profile.version) if core else None
    if core:
        _require_core_inputs(ordered.columns, policy)
    numeric = ordered.copy()
    for name in ["open", "high", "low", "close"] + (["volume"] if core else []):
        numeric[name] = pd.to_numeric(numeric[name], errors="raise").astype("float64")
    minima = (
        {row["spec_id"]: row["configured_group_minimum"] for row in policy["spec_admission"]}
        if core
        else requirements
    )
    eligible = tuple(
        spec
        for spec in profile.indicators
        if len(ordered) >= minima[spec.indicator_id]
        and (
            not core
            or _quantity_available(
                ordered, set(spec.required_input_columns) & {"volume", "open_interest"}
            )
        )
    )
    computed = (
        _compute_profile_frame_unmasked(
            numeric, IndicatorProfile(profile.version, profile.description, eligible)
        )
        if eligible
        else pd.DataFrame(index=numeric.index)
    )
    produced = {name for spec in eligible for name in spec.output_columns}
    require(produced <= set(computed.columns), "missing admitted feature kernel output")
    result = ordered.loc[:, grain].copy()
    outputs = feature_additional_outputs(policy) if core else profile.expected_output_columns()
    for name in outputs:
        values = computed[name] if name in produced else pd.Series(np.nan, index=numeric.index)
        result[name] = (
            pd.to_numeric(values, errors="raise")
            .replace([np.inf, -np.inf], np.nan)
            .astype("float64")
        )
    return result


def _compute_l2_group(frame, profile, requirements, expected_engines, grain):
    _check_worker_engines(expected_engines)
    ordered = frame.sort_values("source_begin", kind="stable").reset_index(drop=True)
    if ordered.duplicated(grain).any():
        raise ValueError("duplicate feature group grain")
    core = profile.version == "native_core_l2.v1"
    policy = feature_policy("L2", profile.version) if core else None
    if core:
        _require_core_inputs(ordered.columns, policy)
    numeric = ordered.copy()
    for name in ["open", "high", "low", "close"] + (["volume"] if core else []):
        numeric[name] = pd.to_numeric(numeric[name], errors="raise").astype("float64")
    quantities = (
        {row["output"]: row["quantity_requirements"] for row in policy["catalog"]} if core else {}
    )
    outputs = feature_additional_outputs(policy) if core else profile.output_columns
    eligible = tuple(
        name
        for name in outputs
        if len(ordered) >= requirements[name]
        and (not core or _quantity_available(ordered, quantities[name]))
    )
    computed = (
        _compute_derived_frame_unmasked(
            base_frame=numeric,
            profile=DerivedIndicatorProfile(
                profile.version, profile.description, eligible, profile.warmup_bars
            ),
        )
        if eligible
        else pd.DataFrame(index=numeric.index)
    )
    require(set(eligible) <= set(computed.columns), "missing admitted derived kernel output")
    result = ordered.loc[:, grain].copy()
    for name in outputs:
        values = computed[name] if name in eligible else pd.Series(np.nan, index=numeric.index)
        result[name] = (
            pd.to_numeric(values, errors="raise")
            .replace([np.inf, -np.inf], np.nan)
            .astype("float64")
        )
    return result


def feature_numerical_identity(layer, profile_version=None):
    policy = feature_policy(layer, profile_version)
    core = "catalog" in policy
    profile_for_policy(policy)
    engines = core_worker_engines() if core else worker_engines()
    _check_worker_engines(engines)
    prefix = "trading_advisor_3000.product_plane.research."
    leaf = prefix + (
        "indicators.materialize._compute_profile_frame_unmasked"
        if layer == "L1"
        else "derived_indicators.materialize._compute_derived_frame_unmasked"
    )
    guard = prefix + "native_feature_materialization._check_worker_engines"
    adapter = prefix + "native_feature_materialization._compute_" + layer.lower() + "_group"
    result = {
        "version": "native_research_core_numerical.v1"
        if core
        else "native_research_feature_numerical.v1",
        "layer": layer,
        "basis": policy["source_basis"],
        "policy_sha256": sha(policy),
        "kernels": {
            "legacy": kernel_contract()[layer.lower()],
            "adapter": SymbolGraph(
                Path(__file__).resolve().parents[2],
                leaves=(
                    leaf,
                    guard,
                    prefix + "native_features.feature_policy",
                    prefix + "native_observations.require",
                ),
            ).fingerprint((adapter,)),
        },
        "engines": engines,
    }
    if core:
        result.update(
            profile_sha256=policy["resolved_profile_sha256"],
            library_source_sha256=_ta_source_sha256(),
        )
    return {**result, "digest": sha(result)}


def build_feature_command(source_reader, *, layer, command_id, profile_version=None):
    policy = feature_policy(layer, profile_version)
    core = "catalog" in policy
    source = source_reader.compute_metadata()
    require(
        source["root"] == "/research-trial" and source["layer"] == policy["parent_layer"],
        "feature requires the exact admitted parent layer/root",
    )
    if core:
        graph = source_reader.verify()
        if layer == "L2":
            require(
                graph["context"].get("contract_version") == "native_research_core_context.v1"
                and graph["context"].get("policy")
                == feature_policy_reference(feature_policy("L1", "native_core_l1.v1")),
                "core L2 requires completed core L1 profile",
            )
        _require_core_inputs(graph["tables"][policy["parent_table"]].column_names, policy)
        require(
            len(source["sequences"]) <= policy["limits"]["sequences"],
            "core sequence budget exceeded",
        )
    intent = {
        "schema_version": 1,
        "input_mode": "native_observation_research.v1",
        "layer": layer,
        "command_id": command_id,
        "source_root": "/research-trial",
        "parent_completion": source["completion"],
        "policy_sha256": sha(policy),
        "numerical_identity": feature_numerical_identity(layer, profile_version),
        "code_sha256": producer_code_sha256(),
    }
    if core:
        intent.update(
            contract_version="native_research_core_intent.v1", profile_version=profile_version
        )
    validate_identity_contract(feature_schema("intent", core), intent, source["namespace"])
    return intent


def _spark_feature_counts(frame, source, policy):
    from pyspark.sql import functions as F

    grouping = [
        k for k in feature_layout("L1")["grain"] if k not in {"source_begin", "research_context_id"}
    ] + ["asset_id", "source_symbol"]
    analysis = F.col("slice_role") == "analysis"
    aggregates = [
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(analysis, 1).otherwise(0)).alias("analysis_rows"),
    ]
    for name in feature_requirements(policy):
        valid = F.col(name).isNotNull()
        aggregates += [
            F.count(F.col(name)).alias(name + "_finite"),
            F.sum(F.when(analysis & valid, 1).otherwise(0)).alias(name + "_analysis"),
            F.min(F.when(valid, F.struct("bar_index", "source_begin"))).alias(name + "_first"),
        ]
    if "catalog" in policy:
        for name in ["volume", "open_interest"]:
            for state in ["known", "not_applicable"]:
                aggregates.append(
                    F.sum(F.when(F.col(name + "_state") == state, 1).otherwise(0)).alias(
                        name + "_" + state
                    )
                )
    result = []
    for row in frame.groupBy(*grouping).agg(*aggregates).limit(129).collect():
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
        sequence["context"] = {k: row[k] for k in ["provider", "venue", "market", "board"]}
        features = {}
        for name in feature_requirements(policy):
            first = row[name + "_first"]
            features[name] = {
                "finite_count": row[name + "_finite"],
                "analysis_finite_count": row[name + "_analysis"],
                "first_finite_bar_index": first.bar_index if first else None,
                "first_finite_source_begin": first.source_begin if first else None,
            }
        entry = {"sequence": sequence, "analysis_rows": row.analysis_rows, "features": features}
        if "catalog" in policy:
            entry["quantities"] = {
                name: {state: row[name + "_" + state] for state in ["known", "not_applicable"]}
                for name in ["volume", "open_interest"]
            }
        result.append(entry)
    return result


def materialize_feature_command(root: Path, intent: dict, *, spark=None, fault=None):
    root = root.resolve()
    require(
        root == Path("/research-trial") and (root / "scope.json").is_file(),
        "feature writer requires isolated mounted Research root",
    )
    require(
        not any((root / p).exists() for p in ["published_current.json", "accepted_current.json"]),
        "feature publication pointer forbidden",
    )
    require(intent.get("source_root") == "/research-trial", "feature source mount mismatch")
    require(
        fault in [None, "after_table", "after_snapshot", "after_completion"],
        "unknown feature fault point",
    )
    profile_version = intent.get("profile_version")
    policy = feature_policy(intent["layer"], profile_version)
    core = "catalog" in policy
    source_reader = parent_reader(
        root, intent["parent_completion"], intent["layer"], profile_version
    )
    source = source_reader.compute_metadata()
    validate_identity_contract(feature_schema("intent", core), intent, source["namespace"])
    input_hash = sha(intent)
    command_dir = root / "commands" / hashlib.sha256(intent["command_id"].encode()).hexdigest()
    with command_lock(root / "writer.lock"):
        intent_path, completion_path = command_dir / "intent.json", command_dir / "completion.json"
        require(
            not intent_path.exists() or json.loads(intent_path.read_bytes()) == intent,
            "same feature command has conflicting canonical input",
        )
        atomic_json(intent_path, intent, immutable=True)
        intent_pin = pin(root, intent_path)
        if completion_path.exists():
            result_pin = pin(root, completion_path)
            graph = NativeFeatureSnapshot(root, result_pin).verify()
            require(
                graph["completion"]["input_sha256"] == input_hash,
                "feature receipt belongs to different input",
            )
            return {"completion": result_pin, "replayed": True, "recovered": False}
        attempts = command_dir / "attempts"
        attempts.mkdir(parents=True, exist_ok=True)
        saved = []
        for path in sorted(attempts.glob("*/manifests/*.json")):
            graph = validate_feature_graph(
                root,
                snapshot_pin=pin(root, path),
                context_pin=pin(root, path.parent.parent / "context.json"),
                intent_pin=intent_pin,
                input_sha256=input_hash,
            )
            saved.append((path, graph))
        require(len(saved) <= 1, "ambiguous saved complete feature graphs")
        recovered = bool(saved)
        owned_spark = False
        cached = None
        calculated = None
        try:
            if recovered:
                snapshot_path, graph = saved[0]
                manifest = graph["manifest"]
                context_pin = pin(root, snapshot_path.parent.parent / "context.json")
                attempt_id = manifest["run_id"]
            else:
                expected = build_feature_command(
                    source_reader,
                    layer=intent["layer"],
                    command_id=intent["command_id"],
                    profile_version=profile_version,
                )
                require(
                    expected == intent, "uncommitted feature policy/numerical/runtime code changed"
                )
                if spark is None:
                    from trading_advisor_3000.spark_jobs.canonical_bars_job import (
                        _create_spark_session,
                    )

                    spark = _create_spark_session(
                        "unpublished-observed-" + intent["layer"].lower(), "local[2]"
                    )
                    owned_spark = True
                attempt_id = uuid4().hex
                attempt = attempts / attempt_id
                attempt.mkdir()
                frame, source = source_reader.read_spark(spark)
                profile = profile_for_policy(policy)
                layout = feature_layout(intent["layer"], profile_version)
                outputs = feature_additional_outputs(policy)
                grain = layout["grain"]
                grouping = [k for k in grain if k != "source_begin"]
                from pyspark.sql import functions as F
                from pyspark.sql import types as T

                udf_schema = T.StructType(
                    [frame.schema[k] for k in grain]
                    + [T.StructField(k, T.DoubleType(), True) for k in outputs]
                )
                compute = _compute_l1_group if intent["layer"] == "L1" else _compute_l2_group
                requirements, engines = (
                    feature_requirements(policy),
                    intent["numerical_identity"]["engines"],
                )
                calculated = (
                    frame.groupBy(*grouping)
                    .applyInPandas(
                        lambda pdf: compute(pdf, profile, requirements, engines, grain),
                        schema=udf_schema,
                    )
                    .persist()
                )
                require(
                    calculated.count() == source["row_count"],
                    "feature returned cardinality mismatch",
                )
                require(
                    not calculated.groupBy(*grain).count().where("count > 1").limit(1).count(),
                    "feature returned duplicate grain",
                )
                require(
                    not frame.join(calculated, grain, "left_anti").limit(1).count()
                    and not calculated.join(frame, grain, "left_anti").limit(1).count(),
                    "feature returned missing/extra keys",
                )
                cached = (
                    frame.join(calculated, grain, "inner")
                    .select(*frame.columns, *outputs)
                    .persist()
                )
                counts = _spark_feature_counts(cached, source, policy)
                quality = feature_quality(profile_version)
                context = {
                    "schema_version": 1,
                    "contract_version": "native_research_core_context.v1"
                    if core
                    else "native_research_feature_context.v1",
                    "input_mode": "native_observation_research.v1",
                    "layer": intent["layer"],
                    "research_context_id": source["research_context_id"],
                    "source": source,
                    "policy": feature_policy_reference(policy),
                    "policy_sha256": intent["policy_sha256"],
                    "layout_sha256": sha({policy["table"]: layout}),
                    "numerical_identity": intent["numerical_identity"],
                    "runtime_code_sha256": intent["code_sha256"],
                    "quality": {"policy": quality, "sha256": sha(quality)},
                    "capabilities": feature_capabilities(source, policy, counts),
                }
                context["feature_context_id"] = "NFCTX:" + sha(context)
                validate_feature_context(context, source)
                context_path = attempt / "context.json"
                atomic_json(context_path, context, immutable=True)
                context_pin = pin(root, context_path)
                prefix = "indicator" if intent["layer"] == "L1" else "derived"
                output = cached.withColumn(
                    prefix + "_context_id", F.lit(context["feature_context_id"])
                ).withColumn(prefix + "_profile_version", F.lit(policy["profile_version"]))
                reference = write_delta_once(
                    spark, attempt / (policy["table"] + ".delta"), output, layout
                )
                if fault == "after_table":
                    raise RuntimeError("injected feature partial failure after table")
                parent = source_reader.verify()["manifest"]
                tables = {**parent["tables"], policy["table"]: reference}
                manifest = create_snapshot(
                    tables=tables,
                    required_tables=[*parent["required_tables"], policy["table"]],
                    dataset_version=policy["version"],
                    run_id=attempt_id,
                    code_version=intent["code_sha256"],
                    quality_checks={
                        key: "PASS" for key in ["parent", "schema", "grain", "capabilities"]
                    },
                    parameters={
                        "input_sha256": input_hash,
                        "context": context_pin,
                        "intent": intent_pin,
                        "source_root": "/research-trial",
                        "parent_completion": intent["parent_completion"],
                    },
                    parents=[source["snapshot"]["snapshot_id"]],
                )
                snapshot_path = save_snapshot(attempt, manifest)
                validate_feature_graph(
                    root,
                    snapshot_pin=pin(root, snapshot_path),
                    context_pin=context_pin,
                    intent_pin=intent_pin,
                    input_sha256=input_hash,
                )
                if fault == "after_snapshot":
                    raise RuntimeError("injected feature failure after saved graph")
            require(
                recovered or producer_code_sha256() == intent["code_sha256"],
                "feature code changed during compute",
            )
            completion = {
                "schema_version": 1,
                "contract_version": "native_research_core_completion.v1"
                if core
                else "native_research_feature_completion.v1",
                "status": "complete_unpublished",
                "layer": intent["layer"],
                "command_id": intent["command_id"],
                "input_sha256": input_hash,
                "attempt_id": attempt_id,
                "snapshot": {**pin(root, snapshot_path), "snapshot_id": manifest["snapshot_id"]},
                "context": context_pin,
                "intent": intent_pin,
                "source_root": "/research-trial",
                "parent_completion": intent["parent_completion"],
                "created_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            validate_identity_contract(
                feature_schema("completion", core), completion, source["namespace"]
            )
            atomic_json(completion_path, completion, immutable=True)
            result_pin = pin(root, completion_path)
            NativeFeatureSnapshot(root, result_pin).verify()
            if fault == "after_completion":
                raise RuntimeError("injected feature lost acknowledgement")
            return {"completion": result_pin, "replayed": False, "recovered": recovered}
        finally:
            if cached is not None:
                cached.unpersist()
            if calculated is not None:
                calculated.unpersist()
            if owned_spark:
                spark.stop()
