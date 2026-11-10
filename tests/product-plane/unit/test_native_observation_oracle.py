"""Synthetic negative controls; real source/physical parity is proved natively."""

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from trading_advisor_3000.product_plane.data_plane.moex import observation_validation as oracle
from trading_advisor_3000.product_plane.data_plane.moex.observation_acquisition import (
    canonical_bytes,
    pin,
)
from trading_advisor_3000.product_plane.data_plane.moex.observation_materialization import (
    intake_records,
)
from trading_advisor_3000.product_plane.data_plane.native_observations import physical_schema


@pytest.mark.parametrize(
    "column,value",
    [
        ("open_lexeme", "0"),
        ("asset_id", "ASSET:wrong"),
        ("source_begin", "2020-01-01 12:00:00"),
        ("body_sha256", "1" * 64),
        ("page_start", 10),
    ],
)
def test_oracle_checks_persisted_raw_columns_not_only_source_values(
    tmp_path, monkeypatch, column, value
):
    fixtures = Path(__file__).parents[1] / "fixtures/contracts"
    metadata = json.loads((fixtures / "native_observation_snapshot.v1.json").read_bytes())
    binding = {
        "source_symbol": "SYNTHETIC",
        "asset_id": "ASSET:fixture",
        "instrument_id": "INSTR:fixture",
        "engine": "stock",
        "kind": "equity",
        "units": {},
        "context": {"provider": "moex_iss", "venue": "MOEX", "market": "shares", "board": "TQBR"},
    }
    assignments = {"namespace": metadata["namespace"], "bindings": [binding]}
    body = tmp_path / "body.json"
    body.write_bytes(
        canonical_bytes({"candles": {"columns": ["open", "begin", "end"], "data": [["malformed"]]}})
    )
    source = {
        "limits": {"rows": 20},
        "source_rows": 1,
        "window": {"date_from": "2020-01-01", "date_till": "2020-01-01"},
        "pages": [
            {
                "body": pin(tmp_path, body),
                "receipt": {"sha256": "0" * 64},
                "source_symbol": "SYNTHETIC",
                "timeframe": "1m",
                "page_start": 0,
            }
        ],
    }
    tables = {
        name: pa.Table.from_pylist([], schema=physical_schema(layout))
        for name, layout in metadata["storage_layout"].items()
    }
    tables["raw_observations"] = pa.Table.from_pylist(
        list(intake_records(tmp_path, source, assignments)),
        schema=tables["raw_observations"].schema,
    )
    tables["quarantine"] = (
        tables["raw_observations"]
        .select(tables["quarantine"].column_names)
        .cast(tables["quarantine"].schema)
    )
    monkeypatch.setattr(
        oracle,
        "open_pinned_table",
        lambda name: SimpleNamespace(to_pyarrow_dataset=lambda: ds.dataset(tables[name])),
    )
    arguments = dict(
        source=source,
        assignments=assignments,
        metadata=metadata,
        tables={name: name for name in tables},
    )
    assert oracle.compare_source_to_physical(tmp_path, **arguments)["quarantined_rows"] == 1
    raw = tables["raw_observations"]
    tables["raw_observations"] = raw.set_column(
        raw.schema.get_field_index(column),
        raw.schema.field(column),
        pa.array([value], type=raw[column].type),
    )
    with pytest.raises(ValueError, match="original RAW row/lexeme accounting mismatch"):
        oracle.compare_source_to_physical(tmp_path, **arguments)
