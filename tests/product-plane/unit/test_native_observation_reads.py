import base64
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.moex.observation_acquisition import (
    canonical_bytes,
)
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    NativeObservationRead,
    NativeObservationSnapshot,
    physical_schema,
)

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"


def sample_read(*, wrong_instrument=False):
    metadata = json.loads((FIXTURES / "native_observation_snapshot.v1.json").read_bytes())
    envelope = json.loads((FIXTURES / "native_observation_read.v1.json").read_bytes())
    columns = metadata["policies"]["mandatory_read_columns"] + envelope["request"]["columns"]
    full = physical_schema(metadata["storage_layout"]["native_bars"])
    schema = pa.schema([full.field(name) for name in columns])
    row = {
        "namespace": "synthetic-only",
        "asset_id": "ASSET:fixture",
        "instrument_id": "INSTR:other" if wrong_instrument else "INSTR:fixture",
        "provider": "moex_iss",
        "venue": "MOEX",
        "engine": "stock",
        "market": "shares",
        "board": "TQBR",
        "source_symbol": "SYNTHETIC",
        "timeframe": "1m",
        "source_begin": "2020-01-01 12:00:00",
        "source_end": "2020-01-01 12:00:59",
        "close": Decimal("1234567890123456789012345678.1234567890"),
        "volume": None,
    }
    table = pa.Table.from_pylist([row], schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    envelope["payload"] = {
        "row_count": 1,
        "columns": columns,
        "schema_ipc_base64": base64.b64encode(schema.serialize()).decode(),
        "ipc_sha256": hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest(),
    }
    return NativeObservationRead(table, canonical_bytes(envelope).decode()), metadata["namespace"]


def test_arrow_roundtrip_preserves_exact_large_decimal_and_null():
    result, namespace = sample_read()
    wire = result.to_ipc(namespace)
    decoded = NativeObservationRead.from_ipc(wire, namespace)
    assert decoded.table["close"][0].as_py() == Decimal("1234567890123456789012345678.1234567890")
    assert decoded.table["volume"].null_count == 1
    assert decoded.table.schema.field("close").type == pa.decimal128(38, 10)


def test_wire_cannot_bind_another_native_instrument_than_its_request():
    result, namespace = sample_read(wrong_instrument=True)
    with pytest.raises(SchemaValidationError):
        result.to_ipc(namespace)


def test_actual_wire_budget_includes_metadata_envelope():
    result, namespace = sample_read()
    metadata = result.metadata
    metadata["request"]["byte_limit"] = 1
    with pytest.raises(SchemaValidationError):
        NativeObservationRead(result.table, canonical_bytes(metadata).decode()).to_ipc(namespace)


def test_invalid_source_time_request_rejects_before_source_io(monkeypatch):
    metadata = json.loads((FIXTURES / "native_observation_snapshot.v1.json").read_bytes())
    request = json.loads((FIXTURES / "native_observation_request.v1.json").read_bytes())
    reader = object.__new__(NativeObservationSnapshot)
    reader._namespace_json = canonical_bytes(metadata["namespace"]).decode()
    monkeypatch.setattr(reader, "verify", lambda: pytest.fail("invalid request reached source I/O"))
    request["source_begin_from"] = "2020-02-30 00:00:00"
    with pytest.raises(SchemaValidationError):
        reader.read(request)


def test_inspector_observation_route_is_opt_in_and_returns_arrow(tmp_path):
    from fastapi.testclient import TestClient

    from trading_advisor_3000.product_plane.interfaces.data_inspector import create_app

    result, namespace = sample_read()

    class Reader:
        def __init__(self):
            self.namespace = namespace

        def read(self, request):
            assert request == result.metadata["request"]
            return result

    with TestClient(create_app(data_root=tmp_path)) as client:
        assert (
            client.post(
                "/api/data-inspector/native-observation-read", json=result.metadata["request"]
            ).status_code
            == 503
        )
    with TestClient(create_app(data_root=tmp_path, observation_reader=Reader())) as client:
        response = client.post(
            "/api/data-inspector/native-observation-read", json=result.metadata["request"]
        )
        assert response.status_code == 200
        assert NativeObservationRead.from_ipc(response.content, namespace).table.equals(
            result.table
        )
