"""Explicit unpublished L0 contexts and bounded public readback; no current pointer."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
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
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    NativeObservationSnapshot,
    contract_constants,
    physical_schema,
)


def require(condition, message):
    if not condition:
        raise SchemaValidationError(message)


def sha(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def l0_constants():
    definitions = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"]
    return {
        "policy": definitions["native_research_l0_policy"]["const"],
        "comparison": definitions["native_research_l0_comparison"]["const"],
    }


def l0_layouts():
    policy = l0_constants()["policy"]
    native = contract_constants()["storage_layout"]["native_bars"]
    return {
        "research_bar_views": {
            "fields": native["fields"] + policy["additional_fields"],
            "grain": policy["bars_grain"],
        },
        "research_instrument_tree": {
            "fields": policy["tree_fields"],
            "grain": policy["tree_grain"],
        },
    }


def capabilities_from_counts(source, counts):
    policy = l0_constants()["policy"]
    result = []
    for sequence in source["sequences"]:
        matches = [row for row in counts if row["sequence"] == sequence]
        require(len(matches) == 1, "missing/duplicate/foreign compute sequence")
        row = matches[0]
        for feature in [
            *policy["metric_requirements"],
            *policy["source_quantity_capabilities"],
            *policy["unknown_capabilities"],
        ]:
            required = policy["metric_requirements"].get(feature, 0)
            finite = row["finite_counts"].get(feature, 0)
            if feature in policy["unknown_capabilities"]:
                state = "unverified_policy"
            elif finite:
                state = "ready"
            elif feature in policy["source_quantity_capabilities"]:
                state = "not_applicable" if row["not_applicable"][feature] else "missing_source"
            else:
                state = (
                    "insufficient_history"
                    if sequence["rows"] < required
                    else "undefined_arithmetic"
                )
            result.append(
                {
                    "sequence": sequence,
                    "feature": feature,
                    "state": state,
                    "required_observations": required,
                    "finite_count": finite,
                    "reason": "native_l0_capabilities.v1/" + feature + "/" + state,
                }
            )
    require(len(counts) == len(source["sequences"]), "extra compute sequence")
    return result


def validate_l0_context(context, source):
    validate_identity_contract("native_research_l0_context.v1.json", context, source["namespace"])
    require(context["source"] == source, "context/source admission mismatch")
    require(context["policy_sha256"] == sha(context["policy"]), "policy hash mismatch")
    require(context["layout_sha256"] == sha(l0_layouts()), "layout hash mismatch")
    require(
        context["quality"]["sha256"] == sha(context["quality"]["policy"]), "quality hash mismatch"
    )
    numerical = context["numerical_identity"]
    require(
        numerical["digest"] == sha({k: v for k, v in numerical.items() if k != "digest"}),
        "numerical digest mismatch",
    )
    require(
        context["research_context_id"]
        == "NCTX:" + sha({k: v for k, v in context.items() if k != "research_context_id"}),
        "NCTX hash mismatch",
    )
    policy = context["policy"]
    features = [
        *policy["metric_requirements"],
        *policy["source_quantity_capabilities"],
        *policy["unknown_capabilities"],
    ]
    expected = {(canonical_bytes(s), feature) for s in source["sequences"] for feature in features}
    actual = [(canonical_bytes(c["sequence"]), c["feature"]) for c in context["capabilities"]]
    require(
        len(actual) == len(expected) and set(actual) == expected,
        "incomplete/duplicate/extra capability matrix",
    )


def _selected(table, sequence):
    mask = pa.array([True] * table.num_rows)
    for name, value in {
        **{
            k: sequence[k]
            for k in [
                "namespace",
                "asset_id",
                "instrument_id",
                "engine",
                "source_symbol",
                "timeframe",
            ]
        },
        **sequence["context"],
    }.items():
        mask = pc.and_(mask, pc.equal(table[name], value))
    return table.filter(mask).sort_by([("source_begin", "ascending")])


def arrow_sequence_counts(table, source):
    policy = l0_constants()["policy"]
    counts = []
    for sequence in source["sequences"]:
        selected = _selected(table, sequence)
        require(
            selected.num_rows == sequence["rows"], "research/source sequence cardinality mismatch"
        )
        finite = {}
        for name in [*policy["metric_requirements"], *policy["source_quantity_capabilities"]]:
            values = selected[name]
            finite[name] = (
                pc.sum(pc.fill_null(pc.is_finite(values), False)).as_py()
                if pa.types.is_floating(values.type)
                else pc.count(values).as_py()
            )
        counts.append(
            {
                "sequence": sequence,
                "finite_counts": finite,
                "not_applicable": {
                    name: pc.all(pc.equal(selected[name + "_state"], "not_applicable")).as_py()
                    for name in policy["source_quantity_capabilities"]
                },
                "first_source_begin": selected["source_begin"][0].as_py(),
                "last_source_begin": selected["source_begin"][-1].as_py(),
                "last_source_end": selected["source_end"][-1].as_py(),
            }
        )
    return counts


def tree_rows(context, counts):
    instruments = {i["instrument_id"]: i for i in context["source"]["namespace"]["instruments"]}
    result = []
    for row in counts:
        sequence = row["sequence"]
        instrument = instruments[sequence["instrument_id"]]
        result.append(
            {
                "research_context_id": context["research_context_id"],
                **{
                    k: sequence[k]
                    for k in [
                        "namespace",
                        "asset_id",
                        "instrument_id",
                        "engine",
                        "source_symbol",
                        "timeframe",
                    ]
                },
                **sequence["context"],
                "instrument_kind": instrument["kind"],
                "instrument_role": instrument["instrument_role"],
                "bar_count": sequence["rows"],
                **{
                    k: row[k]
                    for k in ["first_source_begin", "last_source_begin", "last_source_end"]
                },
                "execution_eligible": False,
            }
        )
    return result


def _source_rows(reader):
    graph = reader.verify()
    return pa.concat_tables(
        [reader.read(request).table for request in graph["intent"]["selection"]]
    )


def validate_l0_tables(root, manifest, context, source_reader):
    tables = {}
    layouts = l0_layouts()
    require(
        set(manifest["tables"]) == set(manifest["required_tables"]) == set(layouts),
        "incomplete L0 table graph",
    )
    for name, layout in layouts.items():
        reference = manifest["tables"][name]
        require(
            Path(reference["table_path"]).resolve().is_relative_to(root.resolve()),
            "Research table escapes isolated root",
        )
        require(
            0 < reference["row_count"] <= 20000 and 0 < reference["file_count"] <= 128,
            "Research table budget exceeded",
        )
        table = open_pinned_table(reference)
        require(
            sum(table.get_add_actions().column("size_bytes").to_pylist()) <= 67108864,
            "Research physical byte budget exceeded",
        )
        require(not table.metadata().partition_columns, "unexpected Research partition layout")
        require(
            table.to_pyarrow_dataset().schema.equals(physical_schema(layout), check_metadata=True),
            "Research physical schema mismatch",
        )
        data = table.to_pyarrow_table().combine_chunks()
        require(data.num_rows == reference["row_count"], "Research physical count mismatch")
        for field in layout["fields"]:
            require(
                field["nullable"] or data[field["name"]].null_count == 0,
                "NULL in required Research field",
            )
        require(
            data.select(layout["grain"]).group_by(layout["grain"]).aggregate([]).num_rows
            == data.num_rows,
            "duplicate Research grain",
        )
        tables[name] = data.sort_by([(k, "ascending") for k in layout["grain"]])
    bars = tables["research_bar_views"]
    require(bars.num_rows == context["source"]["row_count"], "Research row conservation failed")
    source = _source_rows(source_reader)
    native = contract_constants()["storage_layout"]["native_bars"]
    fields = [f["name"] for f in native["fields"]]
    require(
        bars.select(fields).equals(
            source.select(fields)
            .sort_by([(k, "ascending") for k in native["grain"]])
            .combine_chunks()
        ),
        "Research changed source buffers or lineage",
    )
    for name, expected in [
        ("research_context_id", context["research_context_id"]),
        ("research_recipe_version", context["policy"]["version"]),
        ("execution_eligible", False),
    ]:
        require(
            pc.all(pc.equal(bars[name], expected)).as_py() is True,
            "Research context/execution mismatch",
        )
    for name in ["execution_open", "execution_high", "execution_low", "execution_close"]:
        require(bars[name].null_count == bars.num_rows, "unadmitted execution price")
    for name in context["quality"]["policy"]["derived_values"]:
        require(
            pc.all(
                pc.or_(pc.is_null(bars[name]), pc.fill_null(pc.is_finite(bars[name]), False))
            ).as_py()
            is True,
            "nonfinite Research feature",
        )
    expected_roles = pc.if_else(
        pc.less(bars["source_begin"], context["analysis_start"]), "warmup", "analysis"
    )
    require(
        pc.all(pc.equal(bars["slice_role"], expected_roles)).as_py() is True,
        "Research history role mismatch",
    )
    counts = arrow_sequence_counts(bars, context["source"])
    require(
        context["capabilities"] == capabilities_from_counts(context["source"], counts),
        "capability counts/states disagree with physical bars",
    )
    for sequence in context["source"]["sequences"]:
        selected = _selected(bars, sequence)
        require(
            selected["bar_index"].equals(
                pa.chunked_array([pa.array(range(sequence["rows"]), type=pa.int64())])
            ),
            "Research bar index/history mismatch",
        )
        require(
            pc.any(pc.equal(selected["slice_role"], "analysis")).as_py() is True,
            "empty analysis sequence",
        )
    tree_layout = layouts["research_instrument_tree"]
    expected_tree = pa.Table.from_pylist(
        tree_rows(context, counts), schema=physical_schema(tree_layout)
    )
    require(
        tables["research_instrument_tree"].equals(
            expected_tree.sort_by([(k, "ascending") for k in tree_layout["grain"]])
        ),
        "Research tree/sequence mismatch",
    )
    return tables


def validate_l0_graph(root, *, snapshot_pin, context_pin, intent_pin, input_sha256):
    context = json.loads(pinned_bytes(root, context_pin))
    require(context["source"]["source_root"] == "/trial", "Research source mount mismatch")
    source_reader = NativeObservationSnapshot(
        Path(context["source"]["source_root"]), context["source"]["completion"]
    )
    source = source_reader.compute_metadata()
    validate_l0_context(context, source)
    intent = json.loads(pinned_bytes(root, intent_pin))
    validate_identity_contract("native_research_l0_intent.v1.json", intent, source["namespace"])
    require(sha(intent) == input_sha256, "Research intent hash mismatch")
    for key, expected in {
        "source_root": source["source_root"],
        "source_completion": source["completion"],
        "analysis_start": context["analysis_start"],
        "policy_sha256": context["policy_sha256"],
        "numerical_identity": context["numerical_identity"],
        "code_sha256": context["runtime_code_sha256"],
    }.items():
        require(intent[key] == expected, "Research intent/context mismatch: " + key)
    pinned_bytes(root, snapshot_pin)
    manifest = load_snapshot(child(root, snapshot_pin["path"]))
    require(manifest["parents"] == [source["snapshot"]["snapshot_id"]], "Research parent mismatch")
    require(
        manifest["dataset_version"] == "native_observation_research_l0.v1"
        and manifest["code_version"] == intent["code_sha256"],
        "Research manifest recipe mismatch",
    )
    require(
        manifest["parameters"]
        == {
            "input_sha256": input_sha256,
            "context": context_pin,
            "intent": intent_pin,
            "source_root": source["source_root"],
            "source_completion": source["completion"],
        },
        "Research manifest crosslinks mismatch",
    )
    require(
        manifest["quality_checks"]
        == {key: "PASS" for key in ["source", "schema", "grain", "capabilities"]},
        "Research quality not accepted",
    )
    require(manifest["run_id"] == Path(snapshot_pin["path"]).parts[-3], "Research attempt mismatch")
    return {
        "context": context,
        "intent": intent,
        "manifest": manifest,
        "tables": validate_l0_tables(root, manifest, context, source_reader),
    }


class NativeResearchSnapshot:
    """Public readback of one explicit, completed, unpublished L0 graph."""

    def __init__(self, root: Path, completion_pin: dict):
        self.root = root.resolve()
        self._completion_json = canonical_bytes(completion_pin).decode()
        self.verify()

    def verify(self):
        try:
            completion = json.loads(pinned_bytes(self.root, json.loads(self._completion_json)))
            graph = validate_l0_graph(
                self.root,
                snapshot_pin=completion["snapshot"],
                context_pin=completion["context"],
                intent_pin=completion["intent"],
                input_sha256=completion["input_sha256"],
            )
            validate_identity_contract(
                "native_research_l0_completion.v1.json",
                completion,
                graph["context"]["source"]["namespace"],
            )
            require(
                completion["command_id"] == graph["intent"]["command_id"]
                and completion["snapshot"]["snapshot_id"] == graph["manifest"]["snapshot_id"]
                and completion["attempt_id"] == graph["manifest"]["run_id"]
                and completion["source_root"] == graph["intent"]["source_root"]
                and completion["source_completion"] == graph["intent"]["source_completion"],
                "Research completion binding mismatch",
            )
            return {**graph, "completion": completion}
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SnapshotUnavailableError(
                "invalid unpublished Research candidate: " + str(exc)
            ) from exc

    def read_bars(self):
        return self.verify()["tables"]["research_bar_views"]

    def read_tree(self):
        return self.verify()["tables"]["research_instrument_tree"]
