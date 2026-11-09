"""Independent in-memory value/type/mask and carrier rejection oracles."""

import base64
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.contracts import identity_v3 as identity
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.contracts.snapshot_reads import (
    NativeReadRequest,
    decode_projection,
    projection_result,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/contracts"


def load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "name", ["snapshot_read_request.v1.json", "snapshot_read_projection.v1.json"]
)
def test_versioned_schema_fixture_pairs(name):
    identity.validate_identity_contract(name, load(name), load("instrument_namespace.v1.json"))


@pytest.mark.parametrize("wire", [b"", b"not-an-arrow-carrier"])
def test_malformed_carrier_is_a_controlled_contract_rejection(wire):
    with pytest.raises(SchemaValidationError, match="invalid snapshot Arrow carrier"):
        decode_projection(wire, load("instrument_namespace.v1.json"))


def example():
    namespace = load("instrument_namespace.v1.json")
    binding = load("snapshot_identity_binding.v1.json")
    request = {
        "schema_version": 1,
        "identity": binding["selector"],
        "layer": "bars",
        "series_type": "native",
        "timeframe": "1d",
        "start": "2026-10-19T00:00:00Z",
        "end": "2026-10-23T00:00:00Z",
        "projection": ["close", "quantity", "amount"],
        "row_limit": 500,
        "byte_limit": 2_000_000,
        "research": None,
    }
    schema = pa.schema(
        [
            pa.field("bar_start_ts", pa.string(), metadata={b"axis": b"source"}),
            pa.field("close", pa.float64(), metadata={b"unit": b"synthetic-price"}),
            pa.field("quantity", pa.int64()),
            pa.field("amount", pa.decimal128(12, 3)),
        ],
        metadata={b"fixture": b"source-schema"},
    )
    raw = pa.Table.from_arrays(
        [
            pa.array(["2026-10-20T00:00:00Z", "2026-10-19T00:00:00Z", "2026-10-21T00:00:00Z"]),
            pa.array([-0.0, float("nan"), None], type=pa.float64()),
            pa.array([2**60 + 3, 0, None], type=pa.int64()),
            pa.array([Decimal("1.001"), Decimal("-0.500"), None], type=pa.decimal128(12, 3)),
        ],
        schema=schema,
    )
    data = raw
    for name, value in {
        "asset_id": binding["asset_id"],
        "instrument_id": "SBER@MOEX",
        "series_type": "native",
        "timeframe": "1d",
    }.items():
        data = data.append_column(name, pa.repeat(pa.scalar(value), raw.num_rows))
    data = data.select(["asset_id", "instrument_id", "series_type", "timeframe", *raw.column_names])
    config = '{"recipe":"synthetic-observation"}'
    config_hash = hashlib.sha256(config.encode()).hexdigest()
    pins = binding["selector"]["pins"]
    pins.update(configuration_sha256=config_hash, dataset_context_id="CTX:" + config_hash)
    pins["table"] = {
        "source_path": "fixture://source",
        "table_id": "fixture-table",
        "delta_version": 7,
    }
    source = {
        "original_reference": {"path": "fixture://source", "version": 7},
        "read_pin": {
            "table_id": "fixture-table",
            "delta_version": 7,
            "arrow_schema_sha256": hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
            "source_schema_ipc_base64": base64.b64encode(schema.serialize().to_pybytes()).decode(),
            "reference_sha256": "f" * 64,
        },
        "configuration": {
            "dataset_context_id": "CTX:" + config_hash,
            "sha256": config_hash,
            "original_json": config,
        },
        "recipes": [{"name": "normalization", "version": "synthetic-observation"}],
        "report_sha256": pins["report_sha256"],
        "manifest_sha256": pins["source_manifest_sha256"],
        "mapping_version": pins["assignment_version"],
        "namespace_sha256": pins["namespace_sha256"],
        "pin_bundle_sha256": "e" * 64,
    }
    return namespace, binding, request, source, raw, data


def build(namespace, binding, request, source, data):
    NativeReadRequest.from_payload(request, namespace)
    return projection_result(
        request=request, binding=binding, source=source, data=data, namespace=namespace
    )


def test_arrow_buffers_masks_large_integer_decimal_metadata_and_replay():
    namespace, binding, request, source, raw, data = example()
    result = build(namespace, binding, request, source, data)
    replay = decode_projection(result.to_ipc(namespace), namespace)
    expected = raw.take(pa.array([1, 0, 2]))  # independently specified chronological order
    assert replay.data.select(raw.column_names).schema.equals(expected.schema, check_metadata=True)
    for column in raw.column_names:
        actual, wanted = replay.data[column].combine_chunks(), expected[column].combine_chunks()
        assert actual.is_valid().equals(wanted.is_valid())
        if column == "close":
            assert actual.is_nan().equals(wanted.is_nan())
            assert actual.buffers()[1].to_pybytes() == wanted.buffers()[1].to_pybytes()
        else:
            assert actual.equals(wanted)
    assert replay.data["quantity"][1].as_py() == 2**60 + 3
    assert replay.envelope == result.envelope
    assert replay.data.schema.metadata == raw.schema.metadata
    assert replay.envelope["projection"]["canonical_Q_verified"] is False
    assert replay.envelope["projection"]["historical_PIT_verified"] is False
    changed = replay.envelope
    changed["source"]["original_reference"]["path"] = "not-the-source"
    assert replay.envelope == result.envelope
    assert (
        build(namespace, binding, request, source, data.take(pa.array([2, 1, 0]))).envelope
        == result.envelope
    )


@pytest.mark.parametrize(
    "change", ["type", "field_metadata", "global_metadata", "source_schema_hash"]
)
def test_source_schema_not_payload_self_description_is_type_oracle(change):
    namespace, binding, request, source, raw, data = example()
    if change == "type":
        index = data.column_names.index("quantity")
        data = data.set_column(index, "quantity", data["quantity"].cast(pa.float64(), safe=False))
    elif change == "field_metadata":
        index = data.column_names.index("close")
        data = data.set_column(
            index, pa.field("close", pa.float64(), metadata={b"wrong": b"unit"}), data["close"]
        )
    elif change == "global_metadata":
        data = data.replace_schema_metadata({b"wrong": b"schema"})
    else:
        source["read_pin"]["arrow_schema_sha256"] = "0" * 64
    with pytest.raises(SchemaValidationError):
        build(namespace, binding, request, source, data)


@pytest.mark.parametrize(
    "change", ["duplicate", "equivalent_utc_duplicate", "wrong_native", "byte_budget", "row_budget"]
)
def test_invalid_grain_identity_and_overflow_never_truncate_or_deduplicate(change):
    namespace, binding, request, source, raw, data = example()
    if change == "duplicate":
        data = pa.concat_tables([data, data.slice(0, 1)])
    elif change == "equivalent_utc_duplicate":
        extra = data.slice(0, 1).set_column(
            data.column_names.index("bar_start_ts"),
            raw.schema.field("bar_start_ts"),
            pa.array(["2026-10-20T00:00:00.000Z"]),
        )
        data = pa.concat_tables([data, extra])
    elif change == "wrong_native":
        data = data.set_column(1, "instrument_id", pa.array(["WRONG"] * data.num_rows))
    elif change == "byte_budget":
        request["byte_limit"] = 1
    else:
        request["row_limit"] = 2
    with pytest.raises(SchemaValidationError):
        build(namespace, binding, request, source, data)


def test_empty_and_missing_output_are_different_and_typed():
    namespace, binding, request, source, raw, data = example()
    empty = build(namespace, binding, request, source, data.slice(0, 0))
    assert empty.envelope["status"] == "empty"
    decoded = decode_projection(empty.to_ipc(namespace), namespace)
    assert decoded.data.num_rows == 0
    assert decoded.data.schema.equals(data.schema, check_metadata=True)
    missing = projection_result(
        request=request,
        binding=binding,
        source=None,
        data=None,
        namespace=namespace,
        unavailable_reason="missing_accepted_output",
    )
    assert missing.envelope["status"] == "unavailable" and missing.data is None
    with pytest.raises(SchemaValidationError):
        missing.to_ipc(namespace)


@pytest.mark.parametrize(
    "change",
    [
        "numeric_buffer",
        "extra_wire_metadata",
        "schema",
        "count",
        "sha256",
        "dataset_context_id",
        "delta_version",
        "coherent_config",
        "coherent_table",
        "coherent_path",
        "coherent_version",
    ],
)
def test_carrier_cross_validation_rejects_wire_tampering(change):
    namespace, binding, request, source, raw, data = example()
    result = build(namespace, binding, request, source, data)
    wire_table = pa.ipc.open_stream(result.to_ipc(namespace)).read_all()
    key = b"ta3000.snapshot_read_projection.v1"
    metadata = dict(wire_table.schema.metadata)
    envelope = json.loads(metadata[key])
    if change == "numeric_buffer":
        index = wire_table.column_names.index("quantity")
        wire_table = wire_table.set_column(
            index, wire_table.schema.field(index), pa.array([9, 8, 7], type=pa.int64())
        )
    elif change == "extra_wire_metadata":
        metadata[b"unbound"] = b"wrong"
    else:
        if change.startswith("coherent_"):
            source = envelope["source"]
            if change == "coherent_config":
                config = source["configuration"]
                config["original_json"] = '{"recipe":"different"}'
                config["sha256"] = hashlib.sha256(config["original_json"].encode()).hexdigest()
                config["dataset_context_id"] = "CTX:" + config["sha256"]
            elif change == "coherent_version":
                source["read_pin"]["delta_version"] = source["original_reference"]["version"] = 99
            elif change == "coherent_table":
                source["read_pin"]["table_id"] = "different-table"
            else:
                source["original_reference"]["path"] = "fixture://different"
        elif change in {"sha256", "dataset_context_id", "delta_version"}:
            source = envelope["source"]
            section = "read_pin" if change == "delta_version" else "configuration"
            source[section][change] = 99 if change == "delta_version" else "0" * 64
        else:
            envelope["payload"]["row_count"] = 90
        if change == "schema":
            envelope["payload"]["schema_ipc_base64"] = base64.b64encode(
                pa.schema([("wrong", pa.int64())]).serialize().to_pybytes()
            ).decode()
        envelope["fingerprint"] = identity.identity_input_fingerprint(
            {k: v for k, v in envelope.items() if k != "fingerprint"}
        )
        metadata[key] = json.dumps(
            envelope, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode()
    wire_table = wire_table.replace_schema_metadata(metadata)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, wire_table.schema) as writer:
        writer.write_table(wire_table)
    with pytest.raises(SchemaValidationError):
        decode_projection(sink.getvalue().to_pybytes(), namespace)


@pytest.mark.parametrize(
    "field,value",
    [
        ("series_type", "continuous_front"),
        ("known_at", "2026-10-20T00:00:00Z"),
        ("valid_at", "2026-10-20T00:00:00Z"),
        ("layer", "CF"),
        ("projection", ["series_id"]),
        ("projection", ["contract_id"]),
        ("projection", ["security_id"]),
        ("projection", ["source_instrument_id"]),
        ("projection", ["source_series_id"]),
        ("projection", ["internal_id"]),
    ],
)
def test_request_rejects_cf_pit_and_legacy_identity_projection(field, value):
    namespace, binding, request, source, raw, data = example()
    request[field] = value
    with pytest.raises(SchemaValidationError):
        NativeReadRequest.from_payload(request, namespace)


@pytest.mark.parametrize("field", ["sha256", "dataset_context_id", "delta_version"])
def test_encoder_rejects_contradictory_source_metadata(field):
    namespace, binding, request, source, raw, data = example()
    section = "read_pin" if field == "delta_version" else "configuration"
    source[section][field] = 99 if field == "delta_version" else "0" * 64
    with pytest.raises(SchemaValidationError):
        build(namespace, binding, request, source, data)
