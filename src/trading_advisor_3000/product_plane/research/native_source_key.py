"""Configured retrospective label alignment; no market-availability claim."""

import base64
import hashlib
import json
from datetime import timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from trading_advisor_3000.product_plane.common.immutable_files import canonical_bytes
from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    _source_key_time,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError

from .native_observations import require


def _align_tables(lower: pa.Table, higher: pa.Table, columns: list[str]) -> pa.Table:
    lower_keys = pd.DatetimeIndex(
        pd.array(
            [_source_key_time(s) for s in lower["source_begin"].to_pylist()], dtype="datetime64[us]"
        )
    )
    try:
        higher_keys = pd.DatetimeIndex(
            pd.array(
                [
                    _source_key_time(s) + timedelta(minutes=20)
                    for s in higher["source_end"].to_pylist()
                ],
                dtype="datetime64[us]",
            )
        )
    except OverflowError as exc:
        raise SchemaValidationError("configured higher key exceeds calendar range") from exc
    if higher_keys.has_duplicates:
        raise SchemaValidationError("duplicate higher matching key")
    left = pd.DataFrame({"key": lower_keys, "order": range(lower.num_rows)})
    right = pd.DataFrame(
        {"key": higher_keys, "row": pd.Series(range(higher.num_rows), dtype="Int64")}
    )
    matched = pd.merge_asof(
        left.sort_values("key"),
        right.sort_values("key"),
        on="key",
        direction="backward",
        allow_exact_matches=False,
    ).sort_values("order")
    indices = pa.array(matched["row"], type=pa.int64(), from_pandas=True)
    result = lower
    for name in dict.fromkeys([*columns, "source_begin", "source_end"]):
        field = higher.schema.field(name).with_name("higher__" + name).with_nullable(True)
        if field.name in result.column_names:
            raise SchemaValidationError("higher projection collides with lower field")
        result = result.append_column(field, pc.take(higher[name], indices))
    key_labels = pa.array(
        [key.isoformat(sep=" ", timespec="seconds") for key in higher_keys], type=pa.string()
    )
    return result.append_column("higher__join_key", pc.take(key_labels, indices))


def validate_source_key_payload(table, envelope, namespace):
    from .native_reads import validate_native_research_payload

    validate_identity_contract("native_research_source_key_read.v1.json", envelope, namespace)
    lower = envelope["lower"]
    higher = envelope["higher"]
    lower_columns = lower["payload"]["columns"]
    projection = envelope["request"]["higher"]["projection"]
    names = list(dict.fromkeys([*projection, "source_begin", "source_end"]))
    require(
        table.column_names
        == [*lower_columns, *("higher__" + n for n in names), "higher__join_key"],
        "SOURCE_KEY payload columns mismatch",
    )
    validate_native_research_payload(table.select(lower_columns), lower, namespace)
    higher_schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(higher["payload"]["schema_ipc_base64"], validate=True))
    )
    for name in names:
        expected = higher_schema.field(name).with_name("higher__" + name).with_nullable(True)
        require(
            table.schema.field(expected.name).equals(expected, check_metadata=True),
            "SOURCE_KEY higher field schema mismatch",
        )
    require(
        table.schema.field("higher__join_key").equals(
            pa.field("higher__join_key", pa.string()), check_metadata=True
        ),
        "SOURCE_KEY join-key schema mismatch",
    )
    begin = table["higher__source_begin"]
    end = table["higher__source_end"]
    join = table["higher__join_key"]
    require(
        pc.is_null(begin).equals(pc.is_null(end)) and pc.is_null(end).equals(pc.is_null(join)),
        "SOURCE_KEY matched lineage mask mismatch",
    )
    try:
        expected_keys = pa.array(
            [
                None
                if s is None
                else (_source_key_time(s) + timedelta(minutes=20)).isoformat(
                    sep=" ", timespec="seconds"
                )
                for s in end.to_pylist()
            ],
            type=pa.string(),
        )
    except OverflowError as exc:
        raise SchemaValidationError("configured higher key exceeds calendar range") from exc
    require(join.combine_chunks().equals(expected_keys), "SOURCE_KEY configured label key mismatch")
    for value in begin.drop_null().to_pylist():
        _source_key_time(value)
    if table.num_rows:
        require(
            pc.all(pc.fill_null(pc.less(join, table["source_begin"]), True)).as_py() is True,
            "SOURCE_KEY key is not strictly before target",
        )
        request = envelope["request"]["higher"]
        in_window = pc.and_(
            pc.greater_equal(begin, request["source_begin_from"]),
            pc.less(begin, request["source_begin_to"]),
        )
        require(
            pc.all(pc.fill_null(pc.and_(in_window, pc.greater_equal(end, begin)), True)).as_py()
            is True,
            "SOURCE_KEY higher lineage outside explicit window",
        )
        for name in names:
            require(
                not pc.any(
                    pc.and_(pc.is_null(join), pc.is_valid(table["higher__" + name]))
                ).as_py(),
                "SOURCE_KEY unmatched higher value is not NULL",
            )


def align_source_key(reader, payload, *, cache=None):
    from .native_reads import NativeResearchRead, _validate_request_shape, make_native_research_read

    if not isinstance(payload, dict):
        raise SchemaValidationError("SOURCE_KEY request must be an object")
    if payload.get("mode") == "TEMPORAL":
        raise SchemaValidationError("temporal_mode_unverified")
    try:
        request = json.loads(canonical_bytes(payload))
    except (TypeError, ValueError) as exc:
        raise SchemaValidationError("invalid SOURCE_KEY request") from exc
    _validate_request_shape(request, "native_research_source_key_request")
    lower = reader.read(request["lower"], cache=cache)
    namespace = lower.namespace
    validate_identity_contract("native_research_source_key_request.v1.json", request, namespace)
    higher = reader.read(request["higher"], cache=cache)
    require(lower.namespace == higher.namespace, "SOURCE_KEY namespace graph mismatch")
    low = lower.envelope
    high = higher.envelope
    for key in ["asset_id", "instrument_id", "kind", "tradable", "execution_eligible"]:
        require(
            low["identity"][key] == high["identity"][key], "SOURCE_KEY instrument identity mismatch"
        )
    envelope = {
        "schema_version": 1,
        "contract_version": "native_research_source_key_read.v1",
        "request": request,
        "lower": low,
        "higher": high,
        "policy": request["policy"],
        "limitations": low["limitations"],
    }
    from trading_advisor_3000.product_plane.data_plane.native_observations import (
        observation_read_code_sha256,
    )

    key = hashlib.sha256(
        canonical_bytes({"envelope": envelope, "read_code_sha256": observation_read_code_sha256()})
    ).hexdigest()
    cache_namespace = "ta3000.native_research_source_key_read.v1"
    cached = cache.get(cache_namespace, key) if cache is not None else None
    aligned = _align_tables(lower.table, higher.table, request["higher"]["projection"])
    if cached is not None:
        result = NativeResearchRead.from_ipc(cached, namespace)
        require(
            {k: v for k, v in result.envelope.items() if k != "payload"} == envelope,
            "cached SOURCE_KEY provenance mismatch",
        )
        require(
            result.table.equals(aligned, check_metadata=True),
            "cached SOURCE_KEY parent alignment mismatch",
        )
        return result
    result = make_native_research_read(aligned, envelope, namespace)
    wire = result.to_ipc()
    if cache is not None:
        cache.put(cache_namespace, key, wire)
    return result
