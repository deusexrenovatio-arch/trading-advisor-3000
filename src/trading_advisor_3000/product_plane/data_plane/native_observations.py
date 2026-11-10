"""Public, explicitly source-local reads of completed unpublished candidates."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    load_snapshot,
    open_pinned_table,
)
from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    SCHEMAS,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError

from .moex.observation_acquisition import canonical_bytes, child, pinned_bytes, verify_source_bundle

METADATA_KEY = b"ta3000.native_observation_read.v1"


def contract_constants() -> dict:
    definition = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"][
        "native_observation_snapshot"
    ]["properties"]
    return {
        name: definition[name]["const"]
        for name in ["limitations", "normalization_recipe", "storage_layout", "policies"]
    }


def observation_metadata(source_pin: dict, assignment_pin: dict, namespace: dict) -> dict:
    result = {
        "schema_version": 1,
        "contract_version": "native_observation_snapshot.v1",
        "input_mode": "native_observations.v1",
        "namespace": namespace,
        "source_manifest_sha256": source_pin["sha256"],
        "assignments_sha256": assignment_pin["sha256"],
        **contract_constants(),
    }
    validate_identity_contract("native_observation_snapshot.v1.json", result, namespace)
    return result


def physical_schema(layout: dict) -> pa.Schema:
    types = {"string": pa.string(), "bigint": pa.int64(), "decimal(38,10)": pa.decimal128(38, 10)}
    return pa.schema(
        [
            pa.field(f["name"], types[f["sql_type"]], nullable=f["nullable"])
            for f in layout["fields"]
        ]
    )


def _require(condition, message):
    if not condition:
        raise SchemaValidationError(message)


def _ipc(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(
        sink, table.schema, options=pa.ipc.IpcWriteOptions(compression=None)
    ) as writer:
        writer.write_table(table.combine_chunks())
    return sink.getvalue().to_pybytes()


def validate_candidate_graph(
    root: Path,
    *,
    manifest_pin: dict,
    metadata_pin: dict,
    source_pin: dict,
    assignment_pin: dict,
    intent_pin: dict,
    expected_input_sha256: str,
) -> dict:
    """Validate the complete pinned DAG; a saved manifest is not a completion receipt."""
    metadata = json.loads(pinned_bytes(root, metadata_pin))
    namespace = metadata["namespace"]
    validate_identity_contract("native_observation_snapshot.v1.json", metadata, namespace)
    source, assignments = verify_source_bundle(root, source_pin, assignment_pin)
    _require(
        metadata["namespace"] == assignments["namespace"]
        and metadata["source_manifest_sha256"] == source_pin["sha256"]
        and metadata["assignments_sha256"] == assignment_pin["sha256"],
        "source/namespace/metadata pins disagree",
    )
    intent = json.loads(pinned_bytes(root, intent_pin))
    validate_identity_contract("native_observation_intent.v1.json", intent, namespace)
    _require(
        hashlib.sha256(canonical_bytes(intent)).hexdigest() == expected_input_sha256,
        "command input hash mismatch",
    )
    _require(
        intent["metadata"] == metadata_pin
        and intent["source_manifest"] == source_pin
        and intent["assignments"] == assignment_pin,
        "command/source pins disagree",
    )
    pinned_bytes(root, manifest_pin)
    manifest = load_snapshot(child(root, manifest_pin["path"]))
    layouts = metadata["storage_layout"]
    _require(
        set(manifest["tables"]) == set(manifest["required_tables"]) == set(layouts),
        "candidate table graph incomplete",
    )
    _require(
        manifest["quality_checks"]
        and all(value == "PASS" for value in manifest["quality_checks"].values()),
        "candidate quality is not accepted",
    )
    parameters = manifest["parameters"]
    _require(
        parameters["input_sha256"] == expected_input_sha256
        and parameters["metadata_sha256"] == metadata_pin["sha256"]
        and parameters["source_manifest_sha256"] == source_pin["sha256"]
        and parameters["intent_sha256"] == intent_pin["sha256"],
        "candidate graph metadata mismatch",
    )
    for name, layout in layouts.items():
        reference = manifest["tables"][name]
        path = Path(reference["table_path"]).resolve()
        _require(path.is_relative_to(root.resolve()), "candidate table escapes verification root")
        table = open_pinned_table(reference)
        actual = table.to_pyarrow_dataset().schema
        _require(
            actual.equals(physical_schema(layout), check_metadata=True),
            "physical observation layout mismatch: " + name,
        )
        _require(not table.metadata().partition_columns, "unexpected observation partition layout")
    _require(
        parameters["source_rows"] == source["source_rows"], "candidate source accounting mismatch"
    )
    return {
        "manifest": manifest,
        "metadata": metadata,
        "source": source,
        "assignments": assignments,
        "intent": intent,
    }


def completed_candidate(root: Path, completion_pin: dict, *, remaining_depth: int = 16) -> dict:
    if remaining_depth <= 0:
        raise SnapshotUnavailableError("candidate parent chain exceeds the explicit bound")
    try:
        completion = json.loads(pinned_bytes(root, completion_pin))
        graph = validate_candidate_graph(
            root,
            manifest_pin=completion["snapshot"],
            metadata_pin=completion["metadata"],
            source_pin=completion["source_manifest"],
            assignment_pin=completion["assignments"],
            intent_pin=completion["intent"],
            expected_input_sha256=completion["input_sha256"],
        )
        validate_identity_contract(
            "native_observation_completion.v1.json", completion, graph["metadata"]["namespace"]
        )
        intent = graph["intent"]
        _require(
            completion["command_id"] == intent["command_id"]
            and completion["parent"] == intent["parent"]
            and completion["overlay"] == intent["overlay"]
            and completion["snapshot"]["snapshot_id"] == graph["manifest"]["snapshot_id"],
            "completion/command binding mismatch",
        )
        _require(
            Path(completion["snapshot"]["path"]).parts[-3] == completion["attempt_id"],
            "completion attempt mismatch",
        )
        simulations = {}
        if completion["parent"] is None:
            _require(not graph["manifest"]["parents"], "baseline has an unexpected parent")
        else:
            parent = completed_candidate(
                root, completion["parent"], remaining_depth=remaining_depth - 1
            )
            _require(
                graph["manifest"]["parents"] == [parent["manifest"]["snapshot_id"]],
                "candidate parent manifest mismatch",
            )
            _require(parent["metadata"] == graph["metadata"], "correction changes source metadata")
            simulations.update(parent["simulations"])
            overlay = json.loads(pinned_bytes(root, completion["overlay"]))
            validate_identity_contract(
                "native_observation_overlay.v1.json", overlay, graph["metadata"]["namespace"]
            )
            _require(
                overlay["parent_completion_sha256"] == completion["parent"]["sha256"]
                and bool(overlay["simulation_id"]),
                "simulation/parent mismatch",
            )
            simulations[overlay["simulation_id"]] = completion["overlay"]["sha256"]
        return {
            **graph,
            "completion": completion,
            "completion_pin": completion_pin,
            "simulations": simulations,
        }
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SnapshotUnavailableError(
            "incomplete or invalid unpublished candidate: " + str(exc)
        ) from exc


def _all_true(mask) -> bool:
    return pc.all(pc.fill_null(mask, False)).as_py() is True


def validate_observation_rows(table: pa.Table, graph: dict, binding: dict) -> None:
    """Vectorized checks on the bounded selected Arrow buffers, never a row loader."""
    if table.num_rows == 0:
        return
    for name, value in {
        "namespace": graph["metadata"]["namespace"]["namespace"],
        "instrument_id": binding["instrument_id"],
        "asset_id": binding["asset_id"],
        "source_symbol": binding["source_symbol"],
        "provider": binding["context"]["provider"],
        "venue": binding["context"]["venue"],
        "engine": binding["engine"],
        "market": binding["context"]["market"],
        "board": binding["context"]["board"],
    }.items():
        if name in table.column_names:
            _require(_all_true(pc.equal(table[name], value)), "observation FK/context mismatch")
    for name in ["volume", "open_interest"]:
        if name not in table.column_names or name + "_state" not in table.column_names:
            continue
        values, states = table[name], table[name + "_state"]
        reasons = table[name + "_null_reason"]
        known = pc.equal(states, "known")
        valid = pc.and_(
            pc.is_valid(values), pc.greater_equal(values, pa.scalar(0, pa.decimal128(38, 10)))
        )
        _require(
            _all_true(pc.if_else(known, valid, pc.is_null(values))), "quantity value/state mismatch"
        )
        _require(
            _all_true(
                pc.if_else(
                    known,
                    pc.is_null(reasons),
                    pc.or_(
                        pc.and_(
                            pc.equal(states, "unavailable"),
                            pc.equal(reasons, "not_reported_by_source"),
                        ),
                        pc.and_(
                            pc.equal(states, "not_applicable"), pc.equal(reasons, "instrument_kind")
                        ),
                    ),
                )
            ),
            "quantity NULL reason mismatch",
        )
    if binding["kind"] == "reference_index":
        for name in ["volume", "open_interest"]:
            if name in table.column_names:
                _require(table[name].null_count == table.num_rows, "index quantity must be NULL")
    if "source_units_json" in table.column_names:
        _require(
            _all_true(
                pc.equal(table["source_units_json"], canonical_bytes(binding["units"]).decode())
            ),
            "units differ from pinned source facts",
        )
    if {"value_origin", "simulation_id", "overlay_sha256"} <= set(table.column_names):
        normal = pc.and_(
            pc.equal(table["value_origin"], "exchange_observation"),
            pc.and_(pc.is_null(table["simulation_id"]), pc.is_null(table["overlay_sha256"])),
        )
        simulated = pa.array([False] * table.num_rows)
        for name, digest in graph["simulations"].items():
            simulated = pc.or_(
                simulated,
                pc.and_(
                    pc.equal(table["value_origin"], "simulated_correction"),
                    pc.and_(
                        pc.equal(table["simulation_id"], name),
                        pc.equal(table["overlay_sha256"], digest),
                    ),
                ),
            )
        _require(
            _all_true(pc.or_(pc.fill_null(normal, False), pc.fill_null(simulated, False))),
            "observation/simulation lineage mismatch",
        )


@dataclass(frozen=True)
class NativeObservationRead:
    table: pa.Table
    metadata_json: str

    @property
    def metadata(self):
        return json.loads(self.metadata_json)

    def to_ipc(self, namespace: dict) -> bytes:
        metadata = self.metadata
        validate_identity_contract("native_observation_read.v1.json", metadata, namespace)
        _verify_payload(self.table, metadata, namespace)
        schema_metadata = {
            **(self.table.schema.metadata or {}),
            METADATA_KEY: canonical_bytes(metadata),
        }
        wire = _ipc(self.table.replace_schema_metadata(schema_metadata))
        _require(
            len(wire) <= metadata["request"]["byte_limit"], "observation wire byte budget exceeded"
        )
        return wire

    @classmethod
    def from_ipc(cls, wire: bytes, namespace: dict):
        _require(len(wire) <= 16777216, "observation wire exceeds the maximum budget")
        source = pa.BufferReader(wire)
        table = pa.ipc.open_stream(source).read_all()
        _require(source.tell() == len(wire), "unexpected trailing observation wire bytes")
        raw = (table.schema.metadata or {}).get(METADATA_KEY)
        _require(raw is not None, "observation wire has no metadata envelope")
        metadata = json.loads(raw)
        validate_identity_contract("native_observation_read.v1.json", metadata, namespace)
        _require(
            len(wire) <= metadata["request"]["byte_limit"], "observation wire byte budget exceeded"
        )
        original_metadata = dict(table.schema.metadata or {})
        original_metadata.pop(METADATA_KEY)
        table = table.replace_schema_metadata(original_metadata or None)
        _verify_payload(table, metadata, namespace)
        return cls(table, canonical_bytes(metadata).decode())


def _verify_payload(table: pa.Table, metadata: dict, namespace: dict) -> None:
    request, payload = metadata["request"], metadata["payload"]
    constants = contract_constants()
    columns = constants["policies"]["mandatory_read_columns"] + request["columns"]
    _require(
        table.column_names == columns == payload["columns"]
        and table.num_rows == payload["row_count"]
        and table.num_rows <= request["row_limit"],
        "observation count/projection mismatch",
    )
    schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(payload["schema_ipc_base64"], validate=True))
    )
    expected = physical_schema(constants["storage_layout"]["native_bars"])
    expected = pa.schema([expected.field(name) for name in columns])
    _require(
        table.schema.equals(schema, check_metadata=True)
        and schema.equals(expected, check_metadata=True),
        "observation Arrow schema mismatch",
    )
    for field in schema:
        _require(
            field.nullable or table[field.name].null_count == 0,
            "NULL in a required observation field",
        )
    grain = constants["policies"]["read_sort_and_grain"]
    _require(
        table.equals(table.sort_by([(name, "ascending") for name in grain]).combine_chunks()),
        "observation payload is not sorted",
    )
    _require(
        table.select(grain).group_by(grain).aggregate([]).num_rows == table.num_rows,
        "duplicate observation grain",
    )
    if table.num_rows:
        instrument = next(
            item
            for item in namespace["instruments"]
            if item["instrument_id"] == request["instrument_id"]
        )
        constants = {
            "namespace": request["namespace"],
            "instrument_id": request["instrument_id"],
            "asset_id": instrument["asset_id"],
            "provider": request["context"]["provider"],
            "venue": request["context"]["venue"],
            "market": request["context"]["market"],
            "board": request["context"]["board"],
            "engine": request["engine"],
            "timeframe": request["timeframe"],
        }
        for field, value in constants.items():
            _require(
                _all_true(pc.equal(table[field], value)),
                "observation payload/request constant mismatch",
            )
        _require(
            _all_true(
                pc.and_(
                    pc.greater_equal(table["source_begin"], request["source_begin_from"]),
                    pc.less(table["source_begin"], request["source_begin_to"]),
                )
            ),
            "observation outside source-key selection",
        )
    _require(
        hashlib.sha256(_ipc(table)).hexdigest() == payload["ipc_sha256"],
        "observation IPC content hash mismatch",
    )


class NativeObservationSnapshot:
    """Explicit opt-in source-time boundary; the legacy UTC/r7 route is untouched."""

    def __init__(self, root: Path, completion_pin: dict):
        self.root = root.resolve()
        self._completion_json = canonical_bytes(completion_pin).decode()
        self._namespace_json = canonical_bytes(self.verify()["metadata"]["namespace"]).decode()

    @property
    def namespace(self) -> dict:
        return json.loads(self._namespace_json)

    def verify(self) -> dict:
        return completed_candidate(self.root, json.loads(self._completion_json))

    def read(self, request: dict) -> NativeObservationRead:
        request = json.loads(canonical_bytes(request))
        validate_identity_contract("native_observation_request.v1.json", request, self.namespace)
        graph = self.verify()
        matches = [
            b
            for b in graph["assignments"]["bindings"]
            if b["instrument_id"] == request["instrument_id"]
            and b["engine"] == request["engine"]
            and b["context"] == request["context"]
        ]
        _require(len(matches) == 1, "absent/ambiguous observation binding")
        binding = matches[0]
        table = open_pinned_table(graph["manifest"]["tables"]["native_bars"])
        predicate = ds.field("instrument_id") == request["instrument_id"]
        for name, value in {
            "namespace": request["namespace"],
            "provider": request["context"]["provider"],
            "venue": request["context"]["venue"],
            "engine": request["engine"],
            "market": request["context"]["market"],
            "board": request["context"]["board"],
            "timeframe": request["timeframe"],
        }.items():
            predicate &= ds.field(name) == value
        predicate &= (ds.field("source_begin") >= request["source_begin_from"]) & (
            ds.field("source_begin") < request["source_begin_to"]
        )
        columns = graph["metadata"]["policies"]["mandatory_read_columns"] + request["columns"]
        data = (
            table.to_pyarrow_dataset()
            .scanner(
                columns=columns, filter=predicate, batch_size=min(request["row_limit"] + 1, 8192)
            )
            .head(request["row_limit"] + 1)
        )
        _require(data.num_rows <= request["row_limit"], "observation row budget exceeded")
        data = data.sort_by(
            [(name, "ascending") for name in graph["metadata"]["policies"]["read_sort_and_grain"]]
        ).combine_chunks()
        validate_observation_rows(data, graph, binding)
        completion = graph["completion"]
        metadata = {
            "schema_version": 1,
            "contract_version": "native_observation_read.v1",
            "request": request,
            "completion_sha256": json.loads(self._completion_json)["sha256"],
            "snapshot_manifest_sha256": completion["snapshot"]["sha256"],
            "source_manifest_sha256": completion["source_manifest"]["sha256"],
            "observation_metadata_sha256": completion["metadata"]["sha256"],
            "limitations": graph["metadata"]["limitations"],
            "payload": {
                "row_count": data.num_rows,
                "columns": columns,
                "schema_ipc_base64": base64.b64encode(
                    data.schema.serialize().to_pybytes()
                ).decode(),
                "ipc_sha256": hashlib.sha256(_ipc(data)).hexdigest(),
            },
        }
        result = NativeObservationRead(data, canonical_bytes(metadata).decode())
        result.to_ipc(self.namespace)
        return result
