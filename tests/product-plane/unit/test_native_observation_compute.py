"""Compute admission must fail before any lazy Spark read is constructed."""

import json
from pathlib import Path

import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    NativeObservationSnapshot,
)


class ForbiddenSpark:
    @property
    def read(self):
        pytest.fail("rejected compute scope reached Spark IO")


@pytest.mark.parametrize("field,value", [("row_count", 20001), ("file_count", 129)])
def test_compute_rejects_oversized_snapshot_before_spark(tmp_path, monkeypatch, field, value):
    path = Path(__file__).parents[1] / "fixtures/contracts/native_observation_snapshot.v1.json"
    metadata = json.loads(path.read_bytes())
    reference = {"row_count": 2, "file_count": 1}
    reference[field] = value
    graph = {
        "metadata": metadata,
        "source": {"source_rows": reference["row_count"]},
        "manifest": {"tables": {"native_bars": reference, "quarantine": {"row_count": 0}}},
    }
    reader = object.__new__(NativeObservationSnapshot)
    reader.root = tmp_path
    monkeypatch.setattr(reader, "verify", lambda: graph)
    with pytest.raises(SchemaValidationError, match="compute.*budget"):
        reader.read_spark(ForbiddenSpark())


def test_compute_rejects_physical_bytes_before_spark(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.data_plane import native_observations as owner

    graph = {"manifest": {"tables": {"native_bars": {"row_count": 2, "file_count": 1}}}}
    reader = object.__new__(NativeObservationSnapshot)
    reader.root = tmp_path
    monkeypatch.setattr(reader, "verify", lambda: graph)
    table = SimpleNamespace(get_add_actions=lambda: pa.table({"size_bytes": [67108865]}))
    monkeypatch.setattr(owner, "open_pinned_table", lambda _: table)
    with pytest.raises(SchemaValidationError, match="compute physical byte budget"):
        reader.read_spark(ForbiddenSpark())


@pytest.mark.parametrize("reason", ["quarantine", "source_count", "simulation"])
def test_compute_rejects_incomplete_or_simulated_parent_before_spark(tmp_path, monkeypatch, reason):
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.data_plane import native_observations as owner

    graph = {
        "manifest": {
            "tables": {
                "native_bars": {"row_count": 2, "file_count": 1},
                "quarantine": {"row_count": int(reason == "quarantine")},
            }
        },
        "source": {"source_rows": 3 if reason == "source_count" else 2},
        "simulations": {"named-overlay": "a" * 64} if reason == "simulation" else {},
    }
    reader = object.__new__(NativeObservationSnapshot)
    reader.root = tmp_path
    monkeypatch.setattr(reader, "verify", lambda: graph)
    table = SimpleNamespace(get_add_actions=lambda: pa.table({"size_bytes": [1024]}))
    monkeypatch.setattr(owner, "open_pinned_table", lambda _: table)
    with pytest.raises(SchemaValidationError, match="compute"):
        reader.read_spark(ForbiddenSpark())
