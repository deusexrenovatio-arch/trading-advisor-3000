"""Closed observed-price feature contexts and explicit unpublished readback."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.compute as pc

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    load_snapshot,
    open_pinned_table,
)
from trading_advisor_3000.product_plane.common.immutable_files import (
    canonical_bytes,
    child,
    pinned_bytes,
)
from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    SCHEMAS,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.data_plane.native_observations import physical_schema

from .native_core_policy import core_policy, core_policy_ref
from .native_observations import (
    NativeResearchSnapshot,
    _selected,
    l0_layouts,
    read_research_spark,
    require,
    research_compute_metadata,
    sha,
)


def feature_policy(layer, profile_version=None):
    require(layer in {"L1", "L2"}, "unsupported feature layer")
    if profile_version == "native_core_" + layer.lower() + ".v1":
        return core_policy(layer)
    require(
        profile_version in {None, "native_price_" + layer.lower() + ".v1"},
        "unsupported feature profile",
    )
    definitions = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"]
    return definitions["native_observed_" + layer.lower() + "_policy"]["const"]


def feature_schema(kind, core=False):
    return "native_research_" + ("core_" if core else "feature_") + kind + ".v1.json"


def feature_policy_reference(policy):
    return core_policy_ref(policy["layer"]) if "catalog" in policy else policy


def feature_policy_from_context(context):
    version = context.get("contract_version")
    require(
        version in {"native_research_feature_context.v1", "native_research_core_context.v1"},
        "unsupported feature context family",
    )
    policy = feature_policy(context["layer"], context["policy"]["profile_version"])
    require(
        ("catalog" in policy) == (version == "native_research_core_context.v1"),
        "feature context/profile family mismatch",
    )
    return policy


def feature_requirements(policy):
    if "catalog" not in policy:
        return policy["requirements"]
    return {
        row["output"]: row["admission_minimum"] for row in policy["catalog"] if row["materialized"]
    }


def feature_additional_outputs(policy):
    if "catalog" not in policy:
        return tuple(policy["requirements"])
    return tuple(
        row["output"]
        for row in policy["catalog"]
        if row["materialized"] and row["passthrough"] is None
    )


def feature_quality(profile_version=None):
    definitions = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"]
    name = (
        "native_research_core_context"
        if profile_version and profile_version.startswith("native_core_")
        else "native_research_feature_context"
    )
    return definitions[name]["properties"]["quality"]["properties"]["policy"]["const"]


def feature_layout(layer, profile_version=None):
    policy = feature_policy(layer, profile_version)
    base = l0_layouts()["research_bar_views"]
    core = "catalog" in policy
    fields = list(base["fields"])
    for current in ["L1"] if layer == "L1" else ["L1", "L2"]:
        current_policy = feature_policy(
            current, "native_core_" + current.lower() + ".v1" if core else None
        )
        if core:
            fields += [
                {"name": name, "sql_type": "double", "nullable": True}
                for name in feature_additional_outputs(current_policy)
            ]
            prefix = "indicator" if current == "L1" else "derived"
            fields += [
                {"name": prefix + suffix, "sql_type": "string", "nullable": False}
                for suffix in ["_context_id", "_profile_version"]
            ]
        else:
            fields += current_policy["additional_fields"]
    return {"fields": fields, "grain": base["grain"]}


def parent_reader(root, completion_pin, layer, profile_version=None):
    require(layer in {"L1", "L2"}, "unsupported feature layer")
    policy = feature_policy(layer, profile_version)
    receipt = json.loads(pinned_bytes(root, completion_pin))
    require(
        receipt.get("layer") == policy["parent_layer"],
        "feature parent layer mismatch",
    )
    context = json.loads(pinned_bytes(root, receipt["context"]))
    require(context.get("layer") == receipt["layer"], "feature parent context layer mismatch")
    if "catalog" in policy and layer == "L2":
        require(
            context.get("contract_version") == "native_research_core_context.v1"
            and context.get("policy") == core_policy_ref("L1"),
            "core L2 requires completed core L1 profile",
        )
    return (
        NativeResearchSnapshot(root, completion_pin)
        if layer == "L1"
        else NativeFeatureSnapshot(root, completion_pin)
    )


def feature_capabilities(source, policy, counts):
    if "catalog" in policy:
        return core_capabilities(source, policy, counts)
    result = []
    for sequence in source["sequences"]:
        matches = [row for row in counts if row["sequence"] == sequence]
        require(len(matches) == 1, "missing/duplicate feature sequence")
        row = matches[0]
        require(
            0 < row["analysis_rows"] <= sequence["rows"], "empty/invalid feature analysis sequence"
        )
        for name, needed in sorted(policy["requirements"].items()):
            values = row["features"][name]
            require(
                sequence["rows"] >= needed
                or (
                    values["finite_count"] == values["analysis_finite_count"] == 0
                    and values["first_finite_bar_index"] is None
                    and values["first_finite_source_begin"] is None
                ),
                "feature values emitted with insufficient history",
            )
            state = (
                "ready"
                if values["analysis_finite_count"]
                else "insufficient_history"
                if sequence["rows"] < needed
                else "undefined_arithmetic"
            )
            result.append(
                {
                    "sequence": sequence,
                    "feature": name,
                    "state": state,
                    "required_observations": needed,
                    "analysis_rows": row["analysis_rows"],
                    **values,
                    "reason": policy["version"] + "/" + name + "/" + state,
                }
            )
    require(len(counts) == len(source["sequences"]), "extra feature sequence")
    return result


def core_capabilities(source, policy, counts):
    result = []
    for sequence in source["sequences"]:
        matches = [row for row in counts if row["sequence"] == sequence]
        require(len(matches) == 1, "missing/duplicate core sequence")
        measured = matches[0]
        require(
            0 < measured["analysis_rows"] <= sequence["rows"],
            "empty/invalid core analysis sequence",
        )
        for item in policy["catalog"]:
            quantity_state = None
            for name in item["quantity_requirements"]:
                quantity = measured["quantities"][name]
                if quantity["not_applicable"] == sequence["rows"]:
                    quantity_state = "not_applicable"
                    break
                if quantity["known"] != sequence["rows"]:
                    quantity_state = "missing_source"
            state = quantity_state
            if not item["materialized"]:
                require(
                    state is not None or item["required_source_facts"],
                    "newly applicable output is outside core policy",
                )
                state = state or "unverified_policy"
            else:
                values = measured["features"][item["output"]]
                minimum = item["admission_minimum"]
                state = state or ("insufficient_history" if sequence["rows"] < minimum else "ready")
                if state == "ready":
                    require(
                        values["analysis_finite_count"] > 0,
                        "unexplained all-NULL core output after admission: " + item["output"],
                    )
                else:
                    require(
                        values["finite_count"] == values["analysis_finite_count"] == 0
                        and values["first_finite_bar_index"] is None
                        and values["first_finite_source_begin"] is None,
                        "core values emitted without source/history admission",
                    )
            entry = {
                "sequence": sequence,
                "feature": item["output"],
                "state": state,
                "materialized": item["materialized"],
                "analysis_rows": measured["analysis_rows"],
                "reason": policy["version"] + "/" + item["output"] + "/" + state,
            }
            if item["materialized"]:
                entry.update(required_observations=item["admission_minimum"], **values)
            result.append(entry)
    require(len(counts) == len(source["sequences"]), "extra core sequence")
    return result


def validate_feature_context(context, source):
    policy = feature_policy_from_context(context)
    core = "catalog" in policy
    validate_identity_contract(feature_schema("context", core), context, source["namespace"])
    require(context["source"] == source, "feature source admission mismatch")
    require(context["policy"] == feature_policy_reference(policy), "feature policy mismatch")
    require(context["policy_sha256"] == sha(policy), "feature policy hash mismatch")
    layout = {policy["table"]: feature_layout(context["layer"], policy["profile_version"])}
    require(context["layout_sha256"] == sha(layout), "feature layout hash mismatch")
    require(
        context["quality"]["sha256"] == sha(context["quality"]["policy"]),
        "feature quality hash mismatch",
    )
    numerical = context["numerical_identity"]
    runtime = policy["runtime_profile"]
    require(
        numerical["engines"]["pandas_ta_classic"] == runtime["pandas_ta_classic"]
        and numerical["engines"]["talib_absent"]
        is (runtime["talib_absent"] if core else runtime["talib_must_be_absent_in_worker"]),
        "stored feature engine/backend differs from admitted profile",
    )
    if core:
        require(
            numerical["profile_sha256"] == policy["resolved_profile_sha256"]
            and numerical["library_source_sha256"] == runtime["ta_source_sha256"],
            "stored core profile/library source mismatch",
        )
        require(
            all(numerical["engines"][k] == runtime[k] for k in ["numba", "scipy"])
            and numerical["engines"]["ta_numba_enabled"]
            and numerical["engines"]["ta_scipy_enabled"],
            "stored core optional backend mismatch",
        )
    require(
        numerical["digest"] == sha({k: v for k, v in numerical.items() if k != "digest"}),
        "feature numerical digest mismatch",
    )
    require(
        context["feature_context_id"]
        == "NFCTX:" + sha({k: v for k, v in context.items() if k != "feature_context_id"}),
        "feature context hash mismatch",
    )
    expected = {
        (canonical_bytes(s), name)
        for s in source["sequences"]
        for name in (
            [row["output"] for row in policy["catalog"]] if core else policy["requirements"]
        )
    }
    actual = [(canonical_bytes(c["sequence"]), c["feature"]) for c in context["capabilities"]]
    require(
        len(actual) == len(expected) and set(actual) == expected,
        "incomplete/duplicate/extra feature capability matrix",
    )


def feature_arrow_counts(table, source, policy):
    result = []
    for sequence in source["sequences"]:
        data = _selected(table, sequence)
        require(data.num_rows == sequence["rows"], "feature sequence cardinality mismatch")
        analysis = pc.equal(data["slice_role"], "analysis")
        features = {}
        for name in feature_requirements(policy):
            valid = pc.is_valid(data[name])
            positions = pc.indices_nonzero(valid)
            first = positions[0].as_py() if len(positions) else None
            features[name] = {
                "finite_count": pc.count(data[name]).as_py(),
                "analysis_finite_count": pc.sum(pc.and_(valid, analysis)).as_py(),
                "first_finite_bar_index": data["bar_index"][first].as_py()
                if first is not None
                else None,
                "first_finite_source_begin": data["source_begin"][first].as_py()
                if first is not None
                else None,
            }
        row = {
            "sequence": sequence,
            "analysis_rows": pc.sum(analysis).as_py(),
            "features": features,
        }
        if "catalog" in policy:
            row["quantities"] = {
                name: {
                    "known": pc.sum(
                        pc.and_(pc.equal(data[name + "_state"], "known"), pc.is_valid(data[name]))
                    ).as_py(),
                    "not_applicable": pc.sum(
                        pc.equal(data[name + "_state"], "not_applicable")
                    ).as_py(),
                }
                for name in ["volume", "open_interest"]
            }
        result.append(row)
    return result


def validate_feature_graph(root, *, snapshot_pin, context_pin, intent_pin, input_sha256):
    context = json.loads(pinned_bytes(root, context_pin))
    require(context["source"]["root"] == "/research-trial", "feature source mount mismatch")
    policy = feature_policy_from_context(context)
    core = "catalog" in policy
    source_reader = parent_reader(
        root, context["source"]["completion"], context["layer"], policy["profile_version"]
    )
    parent = source_reader.verify()
    source = research_compute_metadata(root, context["source"]["completion"], parent)
    validate_feature_context(context, source)
    intent = json.loads(pinned_bytes(root, intent_pin))
    validate_identity_contract(feature_schema("intent", core), intent, source["namespace"])
    require(sha(intent) == input_sha256, "feature intent hash mismatch")
    for key, expected in {
        "layer": context["layer"],
        "source_root": source["root"],
        "parent_completion": source["completion"],
        "policy_sha256": context["policy_sha256"],
        "numerical_identity": context["numerical_identity"],
        "code_sha256": context["runtime_code_sha256"],
    }.items():
        require(intent[key] == expected, "feature intent/context mismatch: " + key)
    pinned_bytes(root, snapshot_pin)
    manifest = load_snapshot(child(root, snapshot_pin["path"]))
    if core:
        require(
            intent["profile_version"] == policy["profile_version"], "core intent profile mismatch"
        )
    require(
        manifest["dataset_version"] == policy["version"]
        and manifest["code_version"] == intent["code_sha256"],
        "feature manifest recipe mismatch",
    )
    require(
        manifest["parents"] == [source["snapshot"]["snapshot_id"]],
        "feature parent snapshot mismatch",
    )
    require(
        manifest["parameters"]
        == {
            "input_sha256": input_sha256,
            "context": context_pin,
            "intent": intent_pin,
            "source_root": source["root"],
            "parent_completion": source["completion"],
        },
        "feature manifest crosslinks mismatch",
    )
    require(
        manifest["quality_checks"]
        == {key: "PASS" for key in ["parent", "schema", "grain", "capabilities"]},
        "feature quality not accepted",
    )
    require(manifest["run_id"] == Path(snapshot_pin["path"]).parts[-3], "feature attempt mismatch")
    old_tables = parent["manifest"]["tables"]
    require(
        set(manifest["tables"])
        == set(manifest["required_tables"])
        == set(old_tables) | {policy["table"]},
        "incomplete feature table graph",
    )
    require(
        all(manifest["tables"][name] == reference for name, reference in old_tables.items()),
        "feature changed inherited table refs",
    )
    reference = manifest["tables"][policy["table"]]
    require(
        Path(reference["table_path"]).resolve().is_relative_to(root.resolve()),
        "feature table escapes isolated root",
    )
    require(
        0 < reference["row_count"] == source["row_count"] <= 20000
        and 0 < reference["file_count"] <= 128,
        "feature physical count budget mismatch",
    )
    delta = open_pinned_table(reference)
    require(
        sum(delta.get_add_actions().column("size_bytes").to_pylist()) <= 67108864,
        "feature physical byte budget exceeded",
    )
    layout = feature_layout(context["layer"], policy["profile_version"])
    require(not delta.metadata().partition_columns, "unexpected feature partition layout")
    require(
        delta.to_pyarrow_dataset().schema.equals(physical_schema(layout), check_metadata=True),
        "feature physical schema mismatch",
    )
    data = (
        delta.to_pyarrow_table()
        .combine_chunks()
        .sort_by([(k, "ascending") for k in layout["grain"]])
    )
    require(data.num_rows == reference["row_count"], "feature physical row count mismatch")
    require(
        data.select(layout["grain"]).group_by(layout["grain"]).aggregate([]).num_rows
        == data.num_rows,
        "duplicate feature grain",
    )
    for field in layout["fields"]:
        require(
            field["nullable"] or data[field["name"]].null_count == 0, "NULL required feature field"
        )
    parent_data = parent["tables"][policy["parent_table"]]
    parent_fields = parent_data.column_names
    require(
        data.select(parent_fields).equals(
            parent_data.sort_by([(k, "ascending") for k in layout["grain"]]).combine_chunks()
        ),
        "feature changed parent buffers/lineage",
    )
    prefix = "indicator" if context["layer"] == "L1" else "derived"
    for name, value in [
        (prefix + "_context_id", context["feature_context_id"]),
        (prefix + "_profile_version", policy["profile_version"]),
    ]:
        require(
            pc.all(pc.equal(data[name], value)).as_py() is True,
            "feature context/profile column mismatch",
        )
    for name in feature_requirements(policy):
        require(
            pc.all(
                pc.or_(pc.is_null(data[name]), pc.fill_null(pc.is_finite(data[name]), False))
            ).as_py()
            is True,
            "nonfinite feature value",
        )
    if core:
        for item in policy["catalog"]:
            if item["discrete_values"] is not None:
                require(
                    set(data[item["output"]].drop_null().to_pylist())
                    <= set(item["discrete_values"]),
                    "invalid core discrete domain",
                )
    counts = feature_arrow_counts(data, source, policy)
    require(
        context["capabilities"] == feature_capabilities(source, policy, counts),
        "feature physical capability matrix mismatch",
    )
    return {
        "context": context,
        "intent": intent,
        "manifest": manifest,
        "tables": {**parent["tables"], policy["table"]: data},
    }


class NativeFeatureSnapshot:
    """Explicit completed L1/L2 snapshot; no current or source-path discovery."""

    def __init__(self, root: Path, completion_pin: dict):
        self.root = root.resolve()
        self._completion_json = canonical_bytes(completion_pin).decode()
        self.verify()

    def verify(self):
        try:
            completion = json.loads(pinned_bytes(self.root, json.loads(self._completion_json)))
            graph = validate_feature_graph(
                self.root,
                snapshot_pin=completion["snapshot"],
                context_pin=completion["context"],
                intent_pin=completion["intent"],
                input_sha256=completion["input_sha256"],
            )
            validate_identity_contract(
                feature_schema(
                    "completion",
                    graph["context"]["contract_version"] == "native_research_core_context.v1",
                ),
                completion,
                graph["context"]["source"]["namespace"],
            )
            require(
                completion["layer"] == graph["context"]["layer"]
                and completion["command_id"] == graph["intent"]["command_id"]
                and completion["snapshot"]["snapshot_id"] == graph["manifest"]["snapshot_id"]
                and completion["attempt_id"] == graph["manifest"]["run_id"]
                and completion["source_root"] == graph["intent"]["source_root"]
                and completion["parent_completion"] == graph["intent"]["parent_completion"],
                "feature completion binding mismatch",
            )
            return {**graph, "completion": completion}
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SnapshotUnavailableError(
                "invalid unpublished feature candidate: " + str(exc)
            ) from exc

    def read_frame(self):
        graph = self.verify()
        return graph["tables"][graph["context"]["policy"]["table"]]

    def read_tree(self):
        return self.verify()["tables"]["research_instrument_tree"]

    def compute_metadata(self):
        return research_compute_metadata(
            self.root, json.loads(self._completion_json), self.verify()
        )

    def read_spark(self, spark):
        graph = self.verify()
        require(graph["context"]["layer"] == "L1", "unsupported Research compute parent layer")
        return read_research_spark(
            self.root,
            json.loads(self._completion_json),
            graph,
            feature_layout("L1", graph["context"]["policy"]["profile_version"]),
            spark,
        )
