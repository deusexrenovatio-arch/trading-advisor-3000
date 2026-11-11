"""Compute admission must fail before any lazy Spark read is constructed."""

import json
from pathlib import Path

import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    NativeObservationSnapshot,
)


def reader_with_graph(root, graph, monkeypatch):
    reader = object.__new__(NativeObservationSnapshot)
    reader.root = root
    completion = {"path": "synthetic/completion.json", "sha256": "a" * 64}
    reader._completion_json = json.dumps(completion)
    graph["completion_pin"] = completion
    for name, reference in graph["manifest"]["tables"].items():
        reference["table_path"] = (root / (name + ".delta")).as_posix()
    monkeypatch.setattr(reader, "verify", lambda: graph)
    return reader


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
    reader = reader_with_graph(tmp_path, graph, monkeypatch)
    with pytest.raises(SchemaValidationError, match="compute.*budget"):
        reader.read_spark(ForbiddenSpark())


def test_compute_rejects_physical_bytes_before_spark(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.data_plane import native_observations as owner

    graph = {"manifest": {"tables": {"native_bars": {"row_count": 2, "file_count": 1}}}}
    reader = reader_with_graph(tmp_path, graph, monkeypatch)
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
    reader = reader_with_graph(tmp_path, graph, monkeypatch)
    table = SimpleNamespace(get_add_actions=lambda: pa.table({"size_bytes": [1024]}))
    monkeypatch.setattr(owner, "open_pinned_table", lambda _: table)
    with pytest.raises(SchemaValidationError, match="compute"):
        reader.read_spark(ForbiddenSpark())


@pytest.mark.parametrize(
    "dependency", ["schema_validation.py", "native_observation_compute.v1.json"]
)
def test_runtime_provenance_covers_validator_and_thin_schema_bytes(monkeypatch, dependency):
    from trading_advisor_3000.product_plane.data_plane.native_observations import (
        observation_read_code_sha256,
    )

    before = observation_read_code_sha256()
    read_bytes = Path.read_bytes

    def changed(path):
        raw = read_bytes(path)
        return raw + b"\n" if path.name == dependency else raw

    monkeypatch.setattr(Path, "read_bytes", changed)
    assert observation_read_code_sha256() != before
