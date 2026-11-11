"""Bounded public reads of completed source-local Research snapshots."""

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
    atomic_json,
    open_pinned_table,
)
from trading_advisor_3000.product_plane.common.immutable_files import (
    canonical_bytes,
    child,
    pin,
    pinned_bytes,
)
from trading_advisor_3000.product_plane.common.snapshot_read_cache import SnapshotReadCache
from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    SCHEMAS,
    _Evaluator,
    _source_key_time,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    NativeObservationSnapshot,
    contract_constants,
    observation_read_code_sha256,
    physical_schema,
    validate_observation_rows,
)

from .native_features import NativeFeatureSnapshot, feature_layout, validate_feature_context
from .native_observations import (
    NativeResearchSnapshot,
    l0_layouts,
    require,
    sha,
    validate_l0_context,
)

TABLES = {
    "L0": "research_bar_views",
    "L1": "research_indicator_frames",
    "L2": "research_derived_indicator_frames",
}


def native_read_rules():
    return json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"][
        "native_research_read_rules"
    ]["const"]


class _RequestShape(_Evaluator):
    def _constraints(self, schema, value):
        # Resolve namespace/FKs after graph verification, with the same complete validator.
        super()._constraints(
            {k: v for k, v in schema.items() if k not in {"x-namespace", "x-relations"}}, value
        )


def _validate_request_shape(request, definition="native_research_read_request"):
    require(
        definition in {"native_research_read_request", "native_research_source_key_request"},
        "unsupported native read request family",
    )
    evaluator = _RequestShape({"namespace": "", "assets": [], "instruments": []})
    evaluator.check(evaluator._resolve("market_identity.v3.json#/$defs/" + definition), request)


def native_read_columns(request):
    rules = native_read_rules()
    return list(
        dict.fromkeys(
            [
                *contract_constants()["policies"]["mandatory_read_columns"],
                *rules["mandatory_research_columns"],
                *request["projection"],
            ]
        )
    )


def _namespace(envelope):
    source = (
        envelope["source"]
        if envelope["contract_version"] == "native_research_read.v1"
        else envelope["lower"]["source"]
    )
    return source["context_payload"]["source"]["namespace"]


def _limits(envelope):
    return (
        envelope["request"]
        if envelope["contract_version"] == "native_research_read.v1"
        else envelope["request"]["lower"]
    )


def _metadata_key(envelope):
    return native_read_rules()[
        "metadata_key"
        if envelope["contract_version"] == "native_research_read.v1"
        else "source_key_metadata_key"
    ].encode()


def _ipc(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _validate_payload(table, envelope):
    payload = envelope["payload"]
    require(
        table.num_rows == payload["row_count"] <= _limits(envelope)["row_limit"],
        "native Research row budget/count mismatch",
    )
    require(
        table.column_names == payload["columns"]
        and len(set(table.column_names)) == len(table.column_names),
        "native Research projection mismatch",
    )
    schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(payload["schema_ipc_base64"], validate=True))
    )
    require(
        table.schema.equals(schema, check_metadata=True), "native Research Arrow schema mismatch"
    )
    require(
        hashlib.sha256(_ipc(table)).hexdigest() == payload["ipc_sha256"],
        "native Research IPC content hash mismatch",
    )


def _validate_read(table, envelope, namespace):
    version = envelope["contract_version"]
    require(
        version in {"native_research_read.v1", "native_research_source_key_read.v1"},
        "unsupported native Research read family",
    )
    require(_namespace(envelope) == namespace, "native Research namespace mismatch")
    validate_identity_contract(version + ".json", envelope, namespace)
    _validate_payload(table, envelope)
    if version == "native_research_read.v1":
        validate_native_research_payload(table, envelope, namespace)
    else:
        from .native_source_key import validate_source_key_payload

        validate_source_key_payload(table, envelope, namespace)


@dataclass(frozen=True)
class NativeResearchRead:
    table: pa.Table
    envelope_json: str

    @property
    def envelope(self):
        return json.loads(self.envelope_json)

    @property
    def namespace(self):
        return _namespace(self.envelope)

    def to_ipc(self):
        envelope = self.envelope
        _validate_read(self.table, envelope, self.namespace)
        metadata = dict(self.table.schema.metadata or {})
        key = _metadata_key(envelope)
        require(key not in metadata, "source schema contains reserved native Research metadata")
        wire = _ipc(
            self.table.replace_schema_metadata({**metadata, key: canonical_bytes(envelope)})
        )
        require(
            len(wire) <= min(16777216, _limits(envelope)["byte_limit"]),
            "native Research full wire byte budget exceeded",
        )
        return wire

    @classmethod
    def from_ipc(cls, wire: bytes, namespace=None):
        try:
            require(
                isinstance(wire, bytes) and len(wire) <= 16777216,
                "native Research wire exceeds the maximum budget",
            )
            source = pa.BufferReader(wire)
            table = pa.ipc.open_stream(source).read_all()
            require(source.tell() == len(wire), "unexpected trailing native Research wire bytes")
            metadata = dict(table.schema.metadata or {})
            rules = native_read_rules()
            keys = [
                rules[name].encode()
                for name in ["metadata_key", "source_key_metadata_key"]
                if rules[name].encode() in metadata
            ]
            require(len(keys) == 1, "missing/ambiguous native Research wire envelope")
            envelope = json.loads(metadata.pop(keys[0]))
            require(keys[0] == _metadata_key(envelope), "native Research envelope family mismatch")
            require(
                len(wire) <= _limits(envelope)["byte_limit"],
                "native Research full wire byte budget exceeded",
            )
            table = table.replace_schema_metadata(metadata or None)
            expected_namespace = _namespace(envelope)
            if namespace is not None:
                require(
                    namespace == expected_namespace, "native Research supplied namespace mismatch"
                )
            _validate_read(table, envelope, expected_namespace)
            return cls(table, canonical_bytes(envelope).decode())
        except SchemaValidationError:
            raise
        except (pa.ArrowException, ValueError, TypeError, KeyError) as exc:
            raise SchemaValidationError("invalid native Research IPC: " + str(exc)) from exc


def make_native_research_read(table, envelope, namespace=None):
    table = table.combine_chunks()
    envelope = json.loads(canonical_bytes(envelope))
    envelope["payload"] = {
        "row_count": table.num_rows,
        "columns": table.column_names,
        "schema_ipc_base64": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
        "ipc_sha256": hashlib.sha256(_ipc(table)).hexdigest(),
    }
    if namespace is not None:
        require(namespace == _namespace(envelope), "native Research supplied namespace mismatch")
    result = NativeResearchRead(table, canonical_bytes(envelope).decode())
    result.to_ipc()
    return result


def _layout(source):
    context = source["context_payload"]
    return (
        l0_layouts()[TABLES["L0"]]
        if source["layer"] == "L0"
        else feature_layout(source["layer"], context["policy"]["profile_version"])
    )


def _sequence(context, request):
    matches = [
        row
        for row in context["source"]["sequences"]
        if all(
            row[name] == request[name]
            for name in ["namespace", "instrument_id", "engine", "timeframe", "context"]
        )
    ]
    require(len(matches) == 1, "absent/ambiguous native Research source sequence")
    return matches[0]


def validate_native_research_payload(table, envelope, namespace):
    """Validate bounded frame buffers; shared IPC hashes remain part of this contract."""
    validate_identity_contract("native_research_read.v1.json", envelope, namespace)
    _validate_payload(table, envelope)
    request, source, identity = envelope["request"], envelope["source"], envelope["identity"]
    context = source["context_payload"]
    require(
        source["layer"] == context["layer"] == request["layer"]
        and source["table_name"] == TABLES[request["layer"]],
        "native Research source layer/table mismatch",
    )
    require(source["completion"] == request["completion"], "native Research completion mismatch")
    (validate_l0_context if request["layer"] == "L0" else validate_feature_context)(
        context, context["source"]
    )
    sequence = _sequence(context, request)
    columns = native_read_columns(request)
    expected = physical_schema(_layout(source))
    require(set(columns) <= set(expected.names), "unknown native Research projection column")
    expected = pa.schema([expected.field(name) for name in columns], metadata=expected.metadata)
    require(
        table.column_names == columns and table.schema.equals(expected, check_metadata=True),
        "native Research schema/projection differs from selected layer",
    )
    for field in expected:
        require(
            field.nullable or table[field.name].null_count == 0,
            "NULL required native Research field",
        )
    grain = _layout(source)["grain"]
    require(
        table.equals(table.sort_by([(name, "ascending") for name in grain]).combine_chunks()),
        "native Research payload is not sorted",
    )
    require(
        table.select(grain).group_by(grain).aggregate([]).num_rows == table.num_rows,
        "duplicate native Research grain",
    )
    instruments = [
        row for row in namespace["instruments"] if row["instrument_id"] == request["instrument_id"]
    ]
    require(len(instruments) == 1, "ambiguous native Research instrument")
    instrument = instruments[0]
    require(
        {
            k: identity[k]
            for k in ["asset_id", "instrument_id", "kind", "tradable", "execution_eligible"]
        }
        == {
            "asset_id": instrument["asset_id"],
            "instrument_id": instrument["instrument_id"],
            "kind": instrument["kind"],
            "tradable": instrument["tradable"],
            "execution_eligible": False,
        },
        "native Research identity flags mismatch",
    )
    require(
        envelope["limitations"] == context["source"]["limitations"],
        "native Research limitations changed",
    )
    if table.num_rows:
        constants = {
            "namespace": request["namespace"],
            "instrument_id": request["instrument_id"],
            "asset_id": identity["asset_id"],
            "source_symbol": sequence["source_symbol"],
            "engine": request["engine"],
            "timeframe": request["timeframe"],
            "research_context_id": context["research_context_id"],
            "execution_eligible": False,
            **request["context"],
        }
        for name, value in constants.items():
            require(
                pc.all(pc.fill_null(pc.equal(table[name], value), False)).as_py() is True,
                "native Research row/context mismatch: " + name,
            )
        require(
            pc.all(
                pc.and_(
                    pc.greater_equal(table["source_begin"], request["source_begin_from"]),
                    pc.less(table["source_begin"], request["source_begin_to"]),
                )
            ).as_py()
            is True,
            "native Research row outside source-key interval",
        )
        for name in ["source_begin", "source_end"]:
            for value in pc.unique(table[name]).to_pylist():
                _source_key_time(value)
        require(
            pc.all(pc.greater_equal(table["source_end"], table["source_begin"])).as_py() is True,
            "native Research source end precedes begin",
        )
        binding = {**sequence, "kind": identity["kind"], "units": identity["source_units"]}
        validate_observation_rows(
            table, {"metadata": {"namespace": namespace}, "simulations": {}}, binding
        )


class NativeResearchReader:
    """Configured root and explicit completed pins only; no current discovery."""

    def __init__(self, root: Path):
        self.root = root.resolve()

    def _verified_source(self, layer, completion_pin):
        require(layer in TABLES, "unsupported native Research layer")
        try:
            receipt = json.loads(pinned_bytes(self.root, completion_pin))
            require(isinstance(receipt, dict), "native Research completion must be an object")
            require(
                receipt.get("layer") == layer, "native Research requested/receipt layer mismatch"
            )
            owner = NativeResearchSnapshot if layer == "L0" else NativeFeatureSnapshot
            _, graph = owner._verified(self.root, completion_pin)
            completion = graph["completion"]
            require(graph["context"]["layer"] == layer, "native Research context layer mismatch")
            source = {
                "layer": layer,
                "completion": completion_pin,
                "context": completion["context"],
                "intent": completion["intent"],
                "snapshot": completion["snapshot"],
                "table_name": TABLES[layer],
                "table": graph["manifest"]["tables"][TABLES[layer]],
                "context_payload": graph["context"],
            }
            return source, graph
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SnapshotUnavailableError(
                "invalid completed native Research source: " + str(exc)
            ) from exc

    def read(self, payload, *, cache: SnapshotReadCache | None = None):
        try:
            request = json.loads(canonical_bytes(payload))
        except (ValueError, TypeError) as exc:
            raise SchemaValidationError("native Research request must be canonical JSON") from exc
        _validate_request_shape(request)
        source, graph = self._verified_source(request["layer"], request["completion"])
        namespace = source["context_payload"]["source"]["namespace"]
        validate_identity_contract("native_research_read_request.v1.json", request, namespace)
        sequence = _sequence(source["context_payload"], request)
        instrument = next(
            row
            for row in namespace["instruments"]
            if row["instrument_id"] == request["instrument_id"]
        )
        full = graph["tables"][TABLES[request["layer"]]]
        selected = full
        for name, value in {
            "namespace": request["namespace"],
            "instrument_id": request["instrument_id"],
            "engine": request["engine"],
            "timeframe": request["timeframe"],
            **request["context"],
        }.items():
            selected = selected.filter(pc.equal(selected[name], value))
        units = pc.unique(selected["source_units_json"]).to_pylist()
        require(
            len(units) == 1 and selected.num_rows == sequence["rows"],
            "native Research sequence/units mismatch",
        )
        identity = {
            "asset_id": instrument["asset_id"],
            "instrument_id": instrument["instrument_id"],
            "kind": instrument["kind"],
            "tradable": instrument["tradable"],
            "execution_eligible": False,
            "source_units": json.loads(units[0]),
        }
        envelope = {
            "schema_version": 1,
            "contract_version": "native_research_read.v1",
            "request": request,
            "source": source,
            "identity": identity,
            "limitations": source["context_payload"]["source"]["limitations"],
        }
        table = open_pinned_table(source["table"])
        columns = native_read_columns(request)
        require(
            set(columns) <= set(table.to_pyarrow_dataset().schema.names),
            "unknown native Research projection column",
        )
        rules = native_read_rules()
        key = sha(
            {
                "contract": rules["metadata_key"],
                "read_code_sha256": observation_read_code_sha256(),
                **envelope,
            }
        )
        if cache is not None:
            wire = cache.get(rules["metadata_key"], key)
            if wire is not None:
                result = NativeResearchRead.from_ipc(wire, namespace)
                require(
                    {k: v for k, v in result.envelope.items() if k != "payload"} == envelope,
                    "native Research cache provenance mismatch",
                )
                return result
        predicate = ds.field("instrument_id") == request["instrument_id"]
        for name, value in {
            "namespace": request["namespace"],
            "engine": request["engine"],
            "timeframe": request["timeframe"],
            **request["context"],
        }.items():
            predicate &= ds.field(name) == value
        predicate &= (ds.field("source_begin") >= request["source_begin_from"]) & (
            ds.field("source_begin") < request["source_begin_to"]
        )
        data = (
            table.to_pyarrow_dataset()
            .scanner(
                columns=columns, filter=predicate, batch_size=min(request["row_limit"] + 1, 8192)
            )
            .head(request["row_limit"] + 1)
        )
        require(
            data.num_rows <= request["row_limit"],
            "native Research row budget exceeded; narrow the source-key interval",
        )
        data = data.sort_by(
            [(name, "ascending") for name in _layout(source)["grain"]]
        ).combine_chunks()
        result = make_native_research_read(data, envelope, namespace)
        if cache is not None:
            cache.put(rules["metadata_key"], key, result.to_ipc())
        return result

    def _shadow_descriptor(self, completions):
        require(set(completions) == set(TABLES), "shadow requires exact L0/L1/L2 completions")
        sources = {layer: self._verified_source(layer, completions[layer])[0] for layer in TABLES}
        for layer, parent in [("L1", "L0"), ("L2", "L1")]:
            require(
                sources[layer]["context_payload"]["source"]["completion"] == completions[parent],
                "mixed-generation shadow parent",
            )
        source = sources["L0"]["context_payload"]["source"]
        _, data = NativeObservationSnapshot._verified(
            Path(source["source_root"]), source["completion"]
        )
        namespace = data["metadata"]["namespace"]
        require(
            all(s["context_payload"]["source"]["namespace"] == namespace for s in sources.values()),
            "shadow namespace mismatch",
        )
        completion = data["completion"]
        descriptor = {
            "schema_version": 1,
            "contract_version": "native_research_shadow.v1",
            "input_mode": "native_observation_research.v1",
            "namespace": namespace,
            "data": {
                "completion": source["completion"],
                **{
                    name: completion[name]
                    for name in ["snapshot", "metadata", "assignments", "source_manifest", "intent"]
                },
                "table": data["manifest"]["tables"]["native_bars"],
            },
            "research": sources,
            "limitations": source["limitations"],
        }
        validate_identity_contract("native_research_shadow.v1.json", descriptor, namespace)
        return descriptor

    def save_shadow(self, completions, directory: Path | None = None):
        require(
            (self.root / "scope.json").is_file()
            and not any(
                (self.root / name).exists()
                for name in ["published_current.json", "accepted_current.json"]
            ),
            "shadow save requires isolated unpublished Research scope",
        )
        directory = (directory or self.root / "read-shadows").resolve()
        require(directory.is_relative_to(self.root), "shadow destination escapes configured root")
        descriptor = self._shadow_descriptor(json.loads(canonical_bytes(completions)))
        path = directory / (sha(descriptor) + ".json")
        atomic_json(path, descriptor, immutable=True)
        reference = pin(self.root, path)
        self.open_shadow(reference)
        return reference

    def open_shadow(self, reference):
        descriptor = json.loads(pinned_bytes(self.root, reference))
        require(
            child(self.root, reference["path"]).name == sha(descriptor) + ".json",
            "shadow content-address mismatch",
        )
        validate_identity_contract(
            "native_research_shadow.v1.json", descriptor, descriptor["namespace"]
        )
        expected = self._shadow_descriptor(
            {layer: source["completion"] for layer, source in descriptor["research"].items()}
        )
        require(descriptor == expected, "shadow graph no longer matches completed parents")
        return descriptor


from .native_source_key import align_source_key  # noqa: E402,F401
