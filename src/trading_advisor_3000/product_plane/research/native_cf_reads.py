"""Bounded Arrow/IPC reads of one explicitly pinned observed-CF family."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from trading_advisor_3000.product_plane.common.data_snapshots import SnapshotUnavailableError
from trading_advisor_3000.product_plane.common.immutable_files import canonical_bytes
from trading_advisor_3000.product_plane.common.snapshot_read_cache import SnapshotReadCache
from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    SCHEMAS,
    _source_date,
    _source_key_time,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    physical_schema,
    validate_observation_rows,
)

from .native_cf_policy import TABLES, cf_policy
from .native_features import validate_capability_policy
from .native_observations import require, sha
from .native_reads import _ipc, _RequestShape, _validate_cached_read


def cf_read_rules():
    return json.loads((SCHEMAS / "market_identity.v3.json").read_text(encoding="utf-8"))["$defs"][
        "native_cf_read_rules"
    ]["const"]


def cf_read_code_sha256():
    paths = [
        Path(__file__),
        Path(__file__).with_name("native_reads.py"),
        Path(__file__).with_name("native_cf_policy.py"),
        Path(__file__).with_name("native_features.py"),
        SCHEMAS.parent / "identity_v3.py",
        SCHEMAS.parent / "schema_validation.py",
        SCHEMAS.parents[1] / "common/immutable_files.py",
        SCHEMAS.parents[1] / "data_plane/native_observations.py",
        SCHEMAS / "market_identity.v3.json",
        SCHEMAS / "native_cf_read.v1.json",
        SCHEMAS / "native_cf_read_request.v1.json",
    ]
    return sha({path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})


def _shape(request):
    evaluator = _RequestShape({"namespace": "", "assets": [], "instruments": []})
    evaluator.check(
        evaluator._resolve("market_identity.v3.json#/$defs/native_cf_read_request"), request
    )


def _namespace(envelope):
    require(
        isinstance(envelope, dict) and isinstance(envelope.get("source"), dict),
        "CF envelope/source must be objects",
    )
    namespace = envelope["source"].get("namespace")
    require(isinstance(namespace, dict), "CF namespace must be an object")
    return namespace


def _selected_role(source, request):
    matches = [item for item in source["tables"] if item["role"] == request["role"]]
    require(len(matches) == 1, "missing/ambiguous CF table role")
    item = matches[0]
    require(item["table_name"] == TABLES[request["role"]], "CF role/table mismatch")
    require(
        item["layout"] == source["context_payload"]["layouts"][request["role"]],
        "CF role layout differs from context",
    )
    return item


def _selected_sequence(context, request):
    definition = request["definition"]
    require(
        sum(value == definition for value in context["definitions"]) == 1,
        "foreign/ambiguous CF definition selector",
    )
    matches = [
        row
        for row in context["sequences"]
        if row["definition"] == definition and row["timeframe"] == request["timeframe"]
    ]
    require(len(matches) == 1, "absent/ambiguous CF sequence")
    return matches[0]


def cf_read_columns(request, layout):
    # These sealed roles use numeric payloads; their other fields carry keys/provenance.
    mandatory = [
        field["name"]
        for field in layout["fields"]
        if field["sql_type"] not in {"double", "decimal(38,10)"}
    ]
    columns = list(dict.fromkeys([*layout["grain"], *mandatory, *request["projection"]]))
    require(
        set(columns) <= {field["name"] for field in layout["fields"]},
        "unknown CF projection column",
    )
    return columns


def _all(values):
    return pc.all(pc.fill_null(values, False)).as_py() is True


def _constant(table, name, value):
    if name in table.column_names:
        require(
            _all(pc.is_null(table[name]) if value is None else pc.equal(table[name], value)),
            "CF row binding mismatch: " + name,
        )


def _validate_rows(table, envelope, namespace):
    request, source = envelope["request"], envelope["source"]
    definition = request["definition"]
    canonical_definition = canonical_bytes(definition).decode()
    context = source["context_payload"]
    _selected_sequence(context, request)
    if not table.num_rows:
        return
    _constant(table, "cf_definition_json", canonical_definition)
    _constant(table, "timeframe", request["timeframe"])
    context_field = {"indicators": "indicators_context_id", "derived": "derived_context_id"}.get(
        request["role"], "cf_context_id"
    )
    _constant(table, context_field, context["cf_context_id"])
    _constant(table, "instrument_id", None)
    if request["selection"]["kind"] == "source_key_interval":
        require(
            _all(
                pc.and_(
                    pc.greater_equal(
                        table["source_begin"], request["selection"]["source_begin_from"]
                    ),
                    pc.less(table["source_begin"], request["selection"]["source_begin_to"]),
                )
            ),
            "CF row outside source-key interval",
        )
    for name in table.column_names:
        if name == "switch_source_date":
            for value in pc.unique(table[name]).to_pylist():
                _source_date(value)
        if name in {
            "source_begin",
            "source_end",
            "old_source_begin",
            "old_source_end",
            "new_source_begin",
            "new_source_end",
            "observed_switch_source_begin",
            "reference_calculation_embargo_source_label",
        }:
            for value in pc.unique(table[name]).to_pylist():
                _source_key_time(value)
        if pa.types.is_floating(table[name].type):
            require(
                _all(pc.fill_null(pc.is_finite(table[name]), True)),
                "non-finite CF metric",
            )
        if name.endswith(("_ordinal", "_rows", "_count")) or name in {
            "roll_epoch",
            "page_start",
            "observed_events",
            "ladder_rows",
        }:
            require(_all(pc.greater_equal(table[name], 0)), "negative CF count/ordinal")
    components = {row["instrument"]["instrument_id"]: row for row in definition["components"]}
    for name in [
        "active_instrument_id",
        "source_instrument_id",
        "old_instrument_id",
        "new_instrument_id",
    ]:
        if name in table.column_names:
            require(
                _all(pc.is_in(table[name], value_set=pa.array(list(components)))),
                "foreign CF component FK",
            )
    if "active_instrument_id" in table.column_names:
        require(
            _all(pc.equal(table["active_instrument_id"], table["source_instrument_id"])),
            "CF selected source/active FK mismatch",
        )
        for name, value in {**definition["source"], "asset_id": definition["asset_id"]}.items():
            _constant(table, name, value)
        for instrument_id, component in components.items():
            selected = table.filter(pc.equal(table["active_instrument_id"], instrument_id))
            if not selected.num_rows:
                continue
            _constant(selected, "source_symbol", component["source_symbol"])
            _constant(selected, "source_units_json", canonical_bytes(component["units"]).decode())
            names = [name for name in selected.column_names if name != "instrument_id"]
            original = selected.select(names).rename_columns(
                ["instrument_id" if name == "source_instrument_id" else name for name in names]
            )
            binding = {
                "instrument_id": instrument_id,
                "asset_id": definition["asset_id"],
                "source_symbol": component["source_symbol"],
                "context": {
                    k: definition["source"][k] for k in ["provider", "venue", "market", "board"]
                },
                "engine": definition["source"]["engine"],
                "kind": "future",
                "units": component["units"],
            }
            validate_observation_rows(
                original, {"metadata": {"namespace": namespace}, "simulations": {}}, binding
            )
    for prefix in ["", "old_", "new_"]:
        begin, end = prefix + "source_begin", prefix + "source_end"
        if begin in table.column_names and end in table.column_names:
            require(
                _all(pc.greater_equal(table[end], table[begin])), "CF source end precedes begin"
            )
            require(
                _all(
                    pc.greater_equal(
                        table["reference_calculation_embargo_source_label"], table[end]
                    )
                ),
                "CF calculation embargo precedes source end",
            )
    if "old_instrument_id" in table.column_names:
        require(
            _all(pc.not_equal(table["old_instrument_id"], table["new_instrument_id"])),
            "CF transition does not change component",
        )
    if "roll_epoch" in table.column_names:
        for reference in context["used_references"]:
            if (
                reference["definition_json"] != canonical_definition
                or reference["timeframe"] != request["timeframe"]
            ):
                continue
            used = pc.greater_equal(table["roll_epoch"], reference["roll_ordinal"])
            require(
                _all(
                    pc.or_(
                        pc.invert(used),
                        pc.greater_equal(
                            table["reference_calculation_embargo_source_label"],
                            reference["source_end"],
                        ),
                    )
                ),
                "CF calculation embargo precedes USED reference",
            )
    if request["role"] == "qc":
        _constant(table, "status", "PASS")
        require(
            _all(pc.equal(table["observed_events"], table["ladder_rows"])),
            "CF QC event/ladder mismatch",
        )
        require(
            _all(pc.less_equal(table["selected_rows"], table["eligible_component_rows"])),
            "CF QC selected cardinality mismatch",
        )


def validate_cf_payload(table, envelope, namespace=None):
    require(isinstance(table, pa.Table), "CF payload must be an Arrow table")
    actual_namespace = _namespace(envelope)
    require(namespace is None or namespace == actual_namespace, "CF supplied namespace mismatch")
    validate_identity_contract("native_cf_read.v1.json", envelope, actual_namespace)
    source, request = envelope["source"], envelope["request"]
    context = source["context_payload"]
    require(
        context["policy_sha256"] == sha(context["policy_payload"]),
        "CF policy content hash mismatch",
    )
    require(
        context["cf_context_id"]
        == "CFCTX:" + sha({k: v for k, v in context.items() if k != "cf_context_id"}),
        "CF context content hash mismatch",
    )
    numerical = context["numerical_identity"]
    require(
        numerical["digest"] == sha({k: v for k, v in numerical.items() if k != "digest"}),
        "CF numerical identity hash mismatch",
    )
    # Immutable C6 v1 structure/catalog; recorded numerical/runtime IDs stay historical.
    fixed = cf_policy(context["layer"])
    require(
        context["layouts"] == fixed["layouts"],
        "CF fixed family role layout mismatch",
    )
    require(
        context["policy_payload"]["catalog"] == fixed["catalog"]
        and context["policy_payload"]["catalog_sha256"] == fixed["catalog_sha256"],
        "CF fixed family catalog mismatch",
    )
    if context["layer"] != "CF":
        validate_capability_policy(context["capabilities"], fixed["catalog"])
    item = _selected_role(source, request)
    columns = cf_read_columns(request, item["layout"])
    expected = physical_schema(item["layout"])
    expected = pa.schema([expected.field(name) for name in columns], metadata=expected.metadata)
    require(
        table.column_names == columns and table.schema.equals(expected, check_metadata=True),
        "CF schema/projection differs from sealed role",
    )
    payload = envelope["payload"]
    require(
        table.num_rows == payload["row_count"] <= request["row_limit"],
        "CF row count/budget mismatch",
    )
    require(table.column_names == payload["columns"], "CF payload columns mismatch")
    encoded_schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(payload["schema_ipc_base64"], validate=True))
    )
    require(table.schema.equals(encoded_schema, check_metadata=True), "CF IPC schema mismatch")
    require(
        hashlib.sha256(_ipc(table)).hexdigest() == payload["ipc_sha256"],
        "CF IPC content hash mismatch",
    )
    for field in expected:
        require(field.nullable or table[field.name].null_count == 0, "NULL required CF field")
    grain = item["layout"]["grain"]
    require(
        table.equals(table.sort_by([(name, "ascending") for name in grain]).combine_chunks()),
        "CF payload is not sorted",
    )
    require(
        table.select(grain).group_by(grain).aggregate([]).num_rows == table.num_rows,
        "duplicate CF grain",
    )
    _validate_rows(table, envelope, actual_namespace)


@dataclass(frozen=True)
class NativeCfRead:
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
        validate_cf_payload(self.table, envelope)
        metadata = dict(self.table.schema.metadata or {})
        key = cf_read_rules()["metadata_key"].encode()
        require(key not in metadata, "source schema contains reserved CF metadata")
        wire = _ipc(
            self.table.replace_schema_metadata({**metadata, key: canonical_bytes(envelope)})
        )
        require(
            len(wire) <= min(16777216, envelope["request"]["byte_limit"]),
            "CF full wire byte budget exceeded",
        )
        return wire

    @classmethod
    def from_ipc(cls, wire, namespace=None):
        try:
            require(isinstance(wire, bytes) and len(wire) <= 16777216, "invalid/excessive CF wire")
            source = pa.BufferReader(wire)
            table = pa.ipc.open_stream(source).read_all()
            require(source.tell() == len(wire), "unexpected trailing CF wire bytes")
            metadata = dict(table.schema.metadata or {})
            key = cf_read_rules()["metadata_key"].encode()
            require(key in metadata, "missing CF wire envelope")
            envelope = json.loads(metadata.pop(key))
            require(isinstance(envelope, dict), "CF wire envelope must be an object")
            _shape(envelope.get("request"))
            require(
                len(wire) <= envelope["request"]["byte_limit"], "CF full wire byte budget exceeded"
            )
            table = table.replace_schema_metadata(metadata or None)
            validate_cf_payload(table, envelope, namespace)
            return cls(table, canonical_bytes(envelope).decode())
        except SchemaValidationError:
            raise
        except (
            pa.ArrowException,
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            IndexError,
        ) as exc:
            raise SchemaValidationError("invalid CF IPC: " + str(exc)) from exc


def make_cf_read(table, envelope):
    table = table.combine_chunks()
    envelope = json.loads(canonical_bytes(envelope))
    envelope["payload"] = {
        "row_count": table.num_rows,
        "columns": table.column_names,
        "schema_ipc_base64": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
        "ipc_sha256": hashlib.sha256(_ipc(table)).hexdigest(),
    }
    result = NativeCfRead(table, canonical_bytes(envelope).decode())
    result.to_ipc()
    return result


class NativeCfReader:
    def __init__(self, root):
        self.root = Path(root).resolve()

    def _verified_source(self, completion):
        from .native_cf import NativeCfSnapshot

        try:
            reader, graph = NativeCfSnapshot._verified(self.root, completion)
            return reader, graph, reader._compute_metadata(graph)
        except SnapshotUnavailableError:
            raise
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            raise SnapshotUnavailableError("invalid completed CF source: " + str(exc)) from exc

    def read(self, payload, *, cache: SnapshotReadCache | None = None):
        try:
            request = json.loads(canonical_bytes(payload))
        except (ValueError, TypeError) as exc:
            raise SchemaValidationError("CF request must be canonical JSON") from exc
        _shape(request)
        reader, graph, source = self._verified_source(request["completion"])
        namespace = source["namespace"]
        validate_identity_contract("native_cf_read_request.v1.json", request, namespace)
        validate_identity_contract("native_cf_compute.v1.json", source, namespace)
        require(
            request["layer"] == source["layer"] and request["completion"] == source["completion"],
            "CF requested/source layer or completion mismatch",
        )
        context = source["context_payload"]
        _selected_sequence(context, request)
        item = _selected_role(source, request)
        columns = cf_read_columns(request, item["layout"])
        envelope = {
            "schema_version": 1,
            "contract_version": "native_cf_read.v1",
            "request": request,
            "source": source,
            "identity": {
                "definition": request["definition"],
                "instrument_id": None,
                "tradable": False,
                "execution_eligible": False,
            },
            "limitations": context["limitations"],
        }
        rules = cf_read_rules()
        key = sha(
            {
                "contract": rules["metadata_key"],
                "read_code_sha256": cf_read_code_sha256(),
                **envelope,
            }
        )
        full = reader._read_role_verified(graph, request["role"])
        require(
            isinstance(full, pa.Table)
            and full.schema.equals(physical_schema(item["layout"]), check_metadata=True),
            "CF owner role schema mismatch",
        )
        predicate = (
            ds.field("cf_definition_json") == canonical_bytes(request["definition"]).decode()
        ) & (ds.field("timeframe") == request["timeframe"])
        if request["selection"]["kind"] == "source_key_interval":
            predicate = (
                predicate
                & (ds.field("source_begin") >= request["selection"]["source_begin_from"])
                & (ds.field("source_begin") < request["selection"]["source_begin_to"])
            )
        data = (
            ds.dataset(full)
            .scanner(columns=columns, filter=predicate)
            .head(request["row_limit"] + 1)
        )
        require(
            data.num_rows <= request["row_limit"], "CF row budget exceeded; narrow the selection"
        )
        data = data.sort_by(
            [(name, "ascending") for name in item["layout"]["grain"]]
        ).combine_chunks()
        if cache is not None:
            wire = cache.get(rules["metadata_key"], key)
            if wire is not None:
                result = NativeCfRead.from_ipc(wire, namespace)
                _validate_cached_read(result, envelope, data)
                return result
        result = make_cf_read(data, envelope)
        if cache is not None:
            cache.put(rules["metadata_key"], key, result.to_ipc())
        return result
