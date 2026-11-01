"""Executable Arrow representation of the shared snapshot read contract.

Identity/FK and carrier shapes live in market_identity.v3.json. Numeric buffers
never enter JSON; this module validates and transports them without conversion.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from .identity_v3 import (
    _get,
    identity_input_fingerprint,
    snapshot_arrow_rules,
    validate_identity_contract,
)
from .schema_validation import SchemaValidationError


def require(value: bool, message: str) -> None:
    if not value:
        raise SchemaValidationError(message)


@dataclass(frozen=True)
class NativeReadRequest:
    """Immutable envelope, with schema-owned fields and native-only semantics."""

    _json: str

    @classmethod
    def from_payload(cls, value: dict[str, Any], namespace: dict[str, Any]) -> NativeReadRequest:
        validate_identity_contract("snapshot_read_request.v1.json", value, namespace)
        require(
            not (set(value["projection"]) & set(snapshot_arrow_rules()["forbidden_columns"])),
            "forbidden legacy projection column",
        )
        return cls(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False))

    def to_payload(self) -> dict[str, Any]:
        return json.loads(self._json)


def utc_array(array: pa.ChunkedArray) -> pa.ChunkedArray:
    """Normalize only comparison keys; never replace the returned source buffers."""
    if pa.types.is_timestamp(array.type):
        require(array.type.tz == "UTC", "source timestamp must have explicit UTC timezone")
    else:
        require(pa.types.is_string(array.type), "unsupported source timestamp type")
        require(
            len(array) == 0
            or bool(pc.all(pc.fill_null(pc.match_substring_regex(array, r"Z$"), False)).as_py()),
            "source timestamp strings must be explicit UTC",
        )
    return pc.cast(array, pa.timestamp("ns", tz="UTC"), safe=True)


def expected_columns(request: dict[str, Any]) -> list[str]:
    rules = snapshot_arrow_rules()
    timestamp = "bar_start_ts" if request["layer"] == "bars" else "ts"
    return list(dict.fromkeys([*rules["required_columns"], timestamp, *request["projection"]]))


def projection_constants(request: dict[str, Any], binding: dict[str, Any]) -> dict[str, Any]:
    """Evaluate the schema-owned metadata bindings, without row/ID inference."""
    context = {"request": request, "identity": binding}
    return {column: _get(context, path) for column, path in snapshot_arrow_rules()["constants"]}


def ordered_projection(data: pa.Table, timestamp: str) -> pa.Table:
    """Canonical comparison order without casting source timestamp/value columns."""
    keys = pa.table({"timestamp": utc_array(data[timestamp])})
    return data.take(pc.sort_indices(keys, sort_keys=[("timestamp", "ascending")])).combine_chunks()


def data_ipc(data: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, data.schema) as writer:
        writer.write_table(data.combine_chunks())
    return sink.getvalue().to_pybytes()


def _fingerprint(envelope: dict[str, Any]) -> str:
    return identity_input_fingerprint({k: v for k, v in envelope.items() if k != "fingerprint"})


def _wire(envelope: dict[str, Any], data: pa.Table) -> bytes:
    metadata = dict(data.schema.metadata or {})
    key = snapshot_arrow_rules()["metadata_key"].encode("utf-8")
    require(key not in metadata, "source schema already uses reserved carrier metadata")
    metadata[key] = json.dumps(
        envelope, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return data_ipc(data.replace_schema_metadata(metadata))


def validate_projection(
    envelope: dict[str, Any], data: pa.Table | None, namespace: dict[str, Any]
) -> None:
    validate_identity_contract("snapshot_read_projection.v1.json", envelope, namespace)
    require(
        envelope["identity"]["selector"]["pins"]["namespace_sha256"]
        == identity_input_fingerprint(namespace),
        "native namespace fingerprint mismatch",
    )
    require(envelope["fingerprint"] == _fingerprint(envelope), "projection fingerprint mismatch")
    if envelope["status"] == "unavailable":
        require(data is None, "unavailable projection cannot carry data")
        return
    require(data is not None, "projection requires Arrow payload")
    assert data is not None
    configuration = envelope["source"]["configuration"]
    config_hash = hashlib.sha256(configuration["original_json"].encode("utf-8")).hexdigest()
    require(
        configuration["sha256"] == config_hash
        and configuration["dataset_context_id"] == "CTX:" + config_hash,
        "configuration hash/context mismatch",
    )
    request, projection, payload = envelope["request"], envelope["projection"], envelope["payload"]
    rules = snapshot_arrow_rules()
    columns = expected_columns(request)
    timestamp = projection["timestamp_column"]
    grain = [*rules["required_columns"], timestamp]
    require(projection["columns"] == data.column_names == columns, "projection order mismatch")
    require(projection["grain"] == grain, "projection grain differs from contract")
    require(
        not (set(data.column_names) & set(rules["forbidden_columns"])), "legacy identity leaked"
    )
    require(
        payload["row_count"] == data.num_rows <= request["row_limit"], "row count/budget mismatch"
    )
    schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(payload["schema_ipc_base64"], validate=True))
    )
    require(schema.equals(data.schema, check_metadata=True), "Arrow schema/metadata mismatch")
    source_pin = envelope["source"]["read_pin"]
    source_schema_bytes = base64.b64decode(source_pin["source_schema_ipc_base64"], validate=True)
    require(
        hashlib.sha256(source_schema_bytes).hexdigest() == source_pin["arrow_schema_sha256"],
        "original source schema hash mismatch",
    )
    original_schema = pa.ipc.read_schema(pa.BufferReader(source_schema_bytes))
    require(data.schema.metadata == original_schema.metadata, "source schema metadata changed")
    for name in columns:
        if name not in rules["required_columns"]:
            require(
                name in original_schema.names
                and data.schema.field(name).equals(
                    original_schema.field(name), check_metadata=True
                ),
                "raw source field type/metadata changed",
            )
    require(
        hashlib.sha256(data_ipc(data)).hexdigest() == payload["ipc_sha256"], "IPC hash mismatch"
    )
    if data.num_rows:
        for column, path in rules["constants"]:
            values = data[column]
            require(values.null_count == 0, "null native identity/grain")
            require(
                bool(
                    pc.all(
                        pc.equal(values, pa.scalar(_get(envelope, path), type=values.type))
                    ).as_py()
                ),
                "Arrow identity/timeframe differs from admitted selector",
            )
        times = utc_array(data[timestamp])
        require(times.null_count == 0, "null source timestamp")
        start, end = [
            pa.scalar(datetime.fromisoformat(request[k].replace("Z", "+00:00"))).cast(times.type)
            for k in ("start", "end")
        ]
        require(
            bool(pc.all(pc.and_(pc.greater_equal(times, start), pc.less(times, end))).as_py()),
            "timestamp outside half-open request period",
        )
        keys = data.select(rules["required_columns"]).append_column(timestamp, times)
        require(
            keys.group_by(grain).aggregate([]).num_rows == data.num_rows, "duplicate native grain"
        )
        require(times.equals(pc.take(times, pc.sort_indices(times))), "non-deterministic row order")
    require(len(_wire(envelope, data)) <= request["byte_limit"], "wire byte budget exceeded")


@dataclass(frozen=True)
class ArrowReadResult:
    _envelope_json: str
    data: pa.Table | None

    @property
    def envelope(self) -> dict[str, Any]:
        return json.loads(self._envelope_json)

    def to_ipc(self, namespace: dict[str, Any]) -> bytes:
        envelope = self.envelope
        validate_projection(envelope, self.data, namespace)
        require(self.data is not None, "unavailable output has no Arrow wire payload")
        assert self.data is not None
        return _wire(envelope, self.data)


def projection_result(
    *,
    request: dict[str, Any],
    binding: dict[str, Any],
    source: dict[str, Any] | None,
    data: pa.Table | None,
    namespace: dict[str, Any],
    unavailable_reason: str | None = None,
) -> ArrowReadResult:
    timestamp = "bar_start_ts" if request["layer"] == "bars" else "ts"
    if data is not None:
        data = ordered_projection(data, timestamp)
    envelope = {
        "schema_version": 1,
        "contract_version": "snapshot_read_projection.v1",
        "status": "unavailable" if data is None else "ok" if data.num_rows else "empty",
        "request": copy.deepcopy(request),
        "identity": copy.deepcopy(binding),
        "source": copy.deepcopy(source),
        "projection": {
            "timestamp_column": timestamp,
            "timestamp_semantics": "source_key_timestamp",
            "grain": [*snapshot_arrow_rules()["required_columns"], timestamp],
            "columns": expected_columns(request),
            "canonical_Q_verified": False,
            "historical_PIT_verified": False,
            "unverified_Q": [
                "official_session_calendar",
                "publication_time",
                "availability_lag_seconds",
            ],
        },
        "payload": None
        if data is None
        else {
            "row_count": data.num_rows,
            "schema_ipc_base64": base64.b64encode(data.schema.serialize().to_pybytes()).decode(
                "ascii"
            ),
            "ipc_sha256": hashlib.sha256(data_ipc(data)).hexdigest(),
        },
        "fingerprint": "0" * 64,
        "unavailable_reason": unavailable_reason,
    }
    envelope["fingerprint"] = _fingerprint(envelope)
    validate_projection(envelope, data, namespace)
    return ArrowReadResult(
        json.dumps(envelope, sort_keys=True, ensure_ascii=False, allow_nan=False), data
    )


def decode_projection(wire: bytes, namespace: dict[str, Any]) -> ArrowReadResult:
    try:
        return _decode_projection(wire, namespace)
    except SchemaValidationError:
        raise
    except (pa.ArrowException, ValueError, KeyError, TypeError, UnicodeError) as exc:
        raise SchemaValidationError("invalid snapshot Arrow carrier") from exc


def _decode_projection(wire: bytes, namespace: dict[str, Any]) -> ArrowReadResult:
    table = pa.ipc.open_stream(wire).read_all()
    key = snapshot_arrow_rules()["metadata_key"].encode("utf-8")
    require(key in (table.schema.metadata or {}), "missing Arrow carrier envelope")
    envelope = json.loads(table.schema.metadata[key])
    validate_identity_contract("snapshot_read_projection.v1.json", envelope, namespace)
    require(envelope["status"] != "unavailable", "unavailable output cannot contain Arrow IPC")
    schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(envelope["payload"]["schema_ipc_base64"], validate=True))
    )
    expected_metadata = dict(schema.metadata or {})
    expected_metadata[key] = json.dumps(
        envelope, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    require(table.schema.metadata == expected_metadata, "unexpected wire schema metadata")
    data = table.replace_schema_metadata(schema.metadata)
    validate_projection(envelope, data, namespace)
    require(len(wire) <= envelope["request"]["byte_limit"], "wire byte budget exceeded")
    return ArrowReadResult(
        json.dumps(envelope, sort_keys=True, ensure_ascii=False, allow_nan=False), data
    )
