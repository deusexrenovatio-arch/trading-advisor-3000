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

from .native_observations import (
    NativeResearchSnapshot,
    _selected,
    l0_layouts,
    read_research_spark,
    require,
    research_compute_metadata,
    sha,
)


def feature_policy(layer):
    require(layer in {"L1", "L2"}, "unsupported feature layer")
    definitions = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"]
    return definitions["native_observed_" + layer.lower() + "_policy"]["const"]


def feature_quality():
    definitions = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"]
    return definitions["native_research_feature_context"]["properties"]["quality"]["properties"][
        "policy"
    ]["const"]


def feature_layout(layer):
    require(layer in {"L1", "L2"}, "unsupported feature layer")
    base = l0_layouts()["research_bar_views"]
    l1 = {
        "fields": base["fields"] + feature_policy("L1")["additional_fields"],
        "grain": base["grain"],
    }
    return (
        l1
        if layer == "L1"
        else {
            "fields": l1["fields"] + feature_policy("L2")["additional_fields"],
            "grain": base["grain"],
        }
    )


def parent_reader(root, completion_pin, layer):
    require(layer in {"L1", "L2"}, "unsupported feature layer")
    receipt = json.loads(pinned_bytes(root, completion_pin))
    require(
        receipt.get("layer") == feature_policy(layer)["parent_layer"],
        "feature parent layer mismatch",
    )
    return (
        NativeResearchSnapshot(root, completion_pin)
        if layer == "L1"
        else NativeFeatureSnapshot(root, completion_pin)
    )


def feature_capabilities(source, policy, counts):
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


def validate_feature_context(context, source):
    validate_identity_contract(
        "native_research_feature_context.v1.json", context, source["namespace"]
    )
    require(context["source"] == source, "feature source admission mismatch")
    require(context["policy"] == feature_policy(context["layer"]), "feature policy mismatch")
    require(context["policy_sha256"] == sha(context["policy"]), "feature policy hash mismatch")
    layout = {context["policy"]["table"]: feature_layout(context["layer"])}
    require(context["layout_sha256"] == sha(layout), "feature layout hash mismatch")
    require(
        context["quality"]["sha256"] == sha(context["quality"]["policy"]),
        "feature quality hash mismatch",
    )
    numerical = context["numerical_identity"]
    runtime = context["policy"]["runtime_profile"]
    require(
        numerical["engines"]["pandas_ta_classic"] == runtime["pandas_ta_classic"]
        and numerical["engines"]["talib_absent"] is runtime["talib_must_be_absent_in_worker"],
        "stored feature engine/backend differs from admitted profile",
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
        for name in context["policy"]["requirements"]
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
        for name in policy["requirements"]:
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
        result.append(
            {"sequence": sequence, "analysis_rows": pc.sum(analysis).as_py(), "features": features}
        )
    return result


def validate_feature_graph(root, *, snapshot_pin, context_pin, intent_pin, input_sha256):
    context = json.loads(pinned_bytes(root, context_pin))
    require(context["source"]["root"] == "/research-trial", "feature source mount mismatch")
    source_reader = parent_reader(root, context["source"]["completion"], context["layer"])
    parent = source_reader.verify()
    source = research_compute_metadata(root, context["source"]["completion"], parent)
    validate_feature_context(context, source)
    intent = json.loads(pinned_bytes(root, intent_pin))
    validate_identity_contract(
        "native_research_feature_intent.v1.json", intent, source["namespace"]
    )
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
    policy = context["policy"]
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
    layout = feature_layout(context["layer"])
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
    for name in policy["requirements"]:
        require(
            pc.all(
                pc.or_(pc.is_null(data[name]), pc.fill_null(pc.is_finite(data[name]), False))
            ).as_py()
            is True,
            "nonfinite feature value",
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
                "native_research_feature_completion.v1.json",
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
            self.root, json.loads(self._completion_json), graph, feature_layout("L1"), spark
        )
