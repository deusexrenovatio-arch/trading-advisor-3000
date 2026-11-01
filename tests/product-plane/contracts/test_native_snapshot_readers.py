"""Unit metadata fixtures + in-memory Arrow; no Delta table or materialization.

Expected rows are chosen by literal descriptor/native/context/time predicates,
including deliberately colliding board/CF rows. Native pin enforcement itself
also receives a separate real existing-Delta read-only proof.
"""

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pytest
from fastapi.testclient import TestClient
from test_snapshot_identity_admission import make_source

from trading_advisor_3000.product_plane.common.data_snapshots import SnapshotUnavailableError
from trading_advisor_3000.product_plane.common.snapshot_read_cache import SnapshotReadCache
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.contracts.snapshot_reads import (
    NativeReadRequest,
    decode_projection,
)
from trading_advisor_3000.product_plane.data_plane import snapshot_reads as market
from trading_advisor_3000.product_plane.interfaces import data_inspector
from trading_advisor_3000.product_plane.research.snapshot_reads import read_native_frame


class DatasetSpy:
    def __init__(self, table, state):
        self.dataset, self.schema, self.state = ds.dataset(table), table.schema, state

    def scanner(self, **kwargs):
        self.state["scans"].append(kwargs)
        scanner = self.dataset.scanner(**kwargs)

        def head(limit):
            self.state["limits"].append(limit)
            return scanner.head(limit)

        return SimpleNamespace(head=head)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    inputs, _ = make_source(tmp_path, monkeypatch)
    state = {"scans": [], "limits": [], "pin_broken": False}
    manifest_path = inputs["source_root"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["target_series_ids"] = ["legacy-SBER", "legacy-IMOEX", "legacy-SRZ6"]
    manifest_raw = json.dumps(manifest, sort_keys=True).encode()
    manifest_path.write_bytes(manifest_raw)
    inputs["expected"]["source_manifest_sha256"] = hashlib.sha256(manifest_raw).hexdigest()
    report = json.loads(inputs["report_path"].read_text())
    contexts, selections = [], {}
    for label, targets in {
        "source": manifest["target_series_ids"],
        "equity": ["legacy-SBER"],
        "index": ["legacy-IMOEX"],
        "futures": ["legacy-SRZ6"],
    }.items():
        profiles = {
            "indicator_profile_version": "profile-" + label,
            "derived_profile_version": "profile-" + label,
            "capability_policy_version": "source-observation",
        }
        config = {
            "label": label,
            "mapping_version": "reviewed-version",
            "registry_hash": "reviewed-registry",
            "source_manifest_sha256": inputs["expected"]["source_manifest_sha256"],
            "calculation_parameters": {
                "timeframes": ["1d"],
                "targets": targets,
                "normalization": "source-observation",
                "quality": "quality-v1",
                "profiles": profiles,
            },
        }
        text = json.dumps(config)
        contexts.append(
            {
                "label": label,
                "context_json": text,
                "dataset_context_id": "CTX:" + hashlib.sha256(text.encode()).hexdigest(),
            }
        )
        selections[label] = {}
        for layer in ["L0", "L1", "L2"]:
            selections[label][layer] = {
                "dataset_version": "dataset-" + label,
                "contour_id": "native",
                "indicator_set_version": None if layer == "L0" else "indicators-test",
                "derived_indicator_set_version": "derived-test" if layer == "L2" else None,
                "profile_version": None if layer == "L0" else "profile-" + label,
                "source_indicator_profile_version": "profile-" + label if layer == "L2" else None,
            }
    report["contexts"] = contexts
    time = pa.array(
        [
            "2026-10-20T00:00:00Z",
            "2026-10-19T00:00:00Z",
            "2026-10-21T00:00:00Z",
            "2026-10-20T00:00:00Z",
            "2026-10-20T00:00:00Z",
        ]
    )
    frames, pins = {}, {}
    for layer, name in {
        "bars": "analytical_bars",
        "L0": "research_bar_views",
        "L1": "research_indicator_frames",
        "L2": "research_derived_indicator_frames",
    }.items():
        context = next(
            c for c in contexts if c["label"] == ("source" if layer == "bars" else "equity")
        )
        data = {
            "bar_start_ts" if layer == "bars" else "ts": time
            if layer == "bars"
            else pc.cast(time, pa.timestamp("us", tz="UTC")),
            "close": pa.array([-0.0, float("nan"), None, 9999.0, 9999.0], type=pa.float64()),
            "quantity": pa.array([2**60 + 3, 0, None, 9999, 9999], type=pa.int64()),
            "asset_id": pa.array(["old-asset"] * 5),
            "instrument_id": pa.array(["legacy-family"] * 5),
            "timeframe": pa.array(["1d"] * 5),
            "contract_id": pa.array([None] * 5, type=pa.string()),
            "legacy_series_id": pa.array(["legacy-SBER"] * 3 + ["OTHER-BOARD", "legacy-SBER"]),
            "series_type": pa.array(["native"] * 4 + ["continuous_front"]),
            "cf_scope_key": pa.array([None] * 4 + ["other-policy"], type=pa.string()),
            "dataset_context_id": pa.array([context["dataset_context_id"]] * 5),
            "active_contract_id": pa.array(["WRONG-ACTIVE@OTHER-VENUE"] * 5),
            "security_id": pa.array(["security-SBER"] * 5),
            "wide_not_requested": pa.array(["must not be read"] * 5),
        }
        if layer != "bars":
            data.update(
                {
                    field: pa.array([value] * 5, type=pa.string())
                    for field, value in selections["equity"][layer].items()
                }
            )
            data["execution_close"] = pa.array([None] * 5, type=pa.float64())
            data["price_space"] = pa.array(["source_native"] * 5)
            data["sma_20"] = pa.array([4.0, 5.0, None, 9999.0, 9999.0])
            data["close_change_1"] = pa.array([1.0, None, 2.0, 9999.0, 9999.0])
        table = pa.table(data).replace_schema_metadata({b"fixture": b"native source"})
        file_name = "bars.delta" if layer == "bars" else name + ".delta"
        section = "data" if layer == "bars" else "research"
        logical = f"/logical/run/{section}/{file_name}"
        physical = inputs["report_path"].parent.parent / section / file_name
        report["tables"][name] = {
            "path": logical,
            "section": section,
            "version": 5,
            "columns": [
                {"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in table.schema
            ],
        }
        reference = {
            "table_path": str(physical),
            "table_id": "physical-table-A" if layer == "bars" else "table-" + layer,
            "delta_version": 5,
            "schema_hash": "a" * 64,
        }
        pins[name] = {
            "reference": reference,
            "arrow_schema_sha256": hashlib.sha256(
                table.schema.serialize().to_pybytes()
            ).hexdigest(),
        }
        frames[file_name] = DatasetSpy(table, state)
    report_raw = json.dumps(report).encode()
    inputs["report_path"].write_bytes(report_raw)
    inputs["expected"]["report_sha256"] = hashlib.sha256(report_raw).hexdigest()
    bundle = {
        "schema_version": 1,
        "admission_inputs": {k: str(v) if isinstance(v, Path) else v for k, v in inputs.items()},
        "tables": pins,
        "selections": selections,
    }

    def open_pin(reference):
        if state["pin_broken"]:
            raise SnapshotUnavailableError("physical UUID differs")
        return SimpleNamespace(
            to_pyarrow_dataset=lambda: frames[Path(reference["table_path"]).name]
        )

    monkeypatch.setattr(market, "open_pinned_table", open_pin)
    monkeypatch.setattr(
        market, "delta_arrow_dataset", lambda table, **_: table.to_pyarrow_dataset()
    )

    def context(value=bundle):
        raw = json.dumps(value).encode()
        return market.PinnedNativeSnapshot(raw, expected_sha256=hashlib.sha256(raw).hexdigest())

    snapshot = context()
    binding = next(b for b in snapshot.bindings if b["source_symbol"] == "SBER")

    def query(layer="bars", **changes):
        value = {
            "schema_version": 1,
            "identity": copy.deepcopy(binding["selector"]),
            "layer": layer,
            "series_type": "native",
            "timeframe": "1d",
            "start": "2026-10-19T00:00:00Z",
            "end": "2026-10-22T00:00:00Z",
            "projection": ["close", "quantity"],
            "row_limit": 500,
            "byte_limit": 2_000_000,
            "research": None if layer == "bars" else copy.deepcopy(selections["equity"][layer]),
        }
        value.update(changes)
        return value

    return SimpleNamespace(
        snapshot=snapshot,
        query=query,
        state=state,
        frames=frames,
        inputs=inputs,
        bundle=bundle,
        context=context,
    )


def read(fixture, query=None, cache=None):
    query = query or fixture.query()
    request = NativeReadRequest.from_payload(query, fixture.snapshot.namespace)
    owner = market.read_native_bars if query["layer"] == "bars" else read_native_frame
    return owner(fixture.snapshot, request, cache=cache)


@pytest.mark.parametrize("layer", ["bars", "L0", "L1", "L2"])
def test_pushdown_projection_and_admitted_identity_preserve_values_masks_and_grain(setup, layer):
    result = read(setup, setup.query(layer))
    assert result.data.num_rows == 3
    assert result.data["instrument_id"].to_pylist() == ["share-A"] * 3
    assert result.data["asset_id"].to_pylist() == ["ordinary-share"] * 3
    assert result.data["quantity"].to_pylist() == [0, 2**60 + 3, None]
    assert result.data["close"].is_valid().to_pylist() == [True, True, False]
    assert pc.is_nan(result.data["close"]).to_pylist() == [True, False, None]
    assert result.data["quantity"].type == pa.int64()
    assert "wide_not_requested" not in setup.state["scans"][-1]["columns"]
    assert "active_contract_id" not in result.data.column_names
    assert setup.state["limits"][-1] == 501


def test_cache_hit_keeps_bytes_immutable_and_revalidates_pins_before_lookup(setup):
    cache = SnapshotReadCache(max_bytes=2_000_000)
    first = read(setup, cache=cache)
    scans = len(setup.state["scans"])
    first.envelope["source"]["original_reference"]["path"] = "mutated caller copy"
    again = read(setup, cache=cache)
    assert again.envelope == first.envelope
    assert len(setup.state["scans"]) == scans
    setup.state["pin_broken"] = True
    with pytest.raises(SnapshotUnavailableError):
        read(setup, cache=cache)
    assert len(setup.state["scans"]) == scans


def test_source_bytes_change_cannot_be_hidden_by_warm_cache(setup):
    cache = SnapshotReadCache(max_bytes=2_000_000)
    read(setup, cache=cache)
    with setup.inputs["report_path"].open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(SchemaValidationError, match="snapshot input changed"):
        read(setup, cache=cache)


def test_unavailable_output_and_empty_period_never_trigger_materialization(setup):
    query = setup.query(start="2027-01-01T00:00:00Z", end="2027-01-02T00:00:00Z")
    assert read(setup, query).envelope["status"] == "empty"
    report = json.loads(setup.inputs["report_path"].read_text())
    del report["tables"]["research_indicator_frames"]
    raw = json.dumps(report).encode()
    setup.inputs["report_path"].write_bytes(raw)
    bundle = copy.deepcopy(setup.bundle)
    bundle["admission_inputs"]["expected"]["report_sha256"] = hashlib.sha256(raw).hexdigest()
    replacement = setup.context(bundle)
    new_query = setup.query("L1")
    new_query["identity"] = next(b for b in replacement.bindings if b["source_symbol"] == "SBER")[
        "selector"
    ]
    scans = len(setup.state["scans"])
    result = read_native_frame(
        replacement, NativeReadRequest.from_payload(new_query, replacement.namespace)
    )
    assert result.envelope["status"] == "unavailable" and result.data is None
    assert len(setup.state["scans"]) == scans


def test_unknown_projection_and_unpinned_selection_fail_before_scanner(setup):
    with pytest.raises(SchemaValidationError, match="unknown projection"):
        read(setup, setup.query(projection=["not_a_column"]))
    query = setup.query("L1")
    query["research"]["profile_version"] = "another-profile"
    with pytest.raises(SchemaValidationError, match="not pinned"):
        read(setup, query)
    assert not setup.state["scans"]


@pytest.mark.parametrize(
    "field",
    [
        "dataset_context_id",
        "dataset_version",
        "contour_id",
        "indicator_set_version",
        "derived_indicator_set_version",
        "profile_version",
        "source_indicator_profile_version",
        "contract_id",
        "price_space",
    ],
)
def test_single_wrong_context_version_or_contract_row_is_excluded(setup, field):
    spy = setup.frames["research_derived_indicator_frames.delta"]
    original = spy.dataset.to_table()
    ghost = original.slice(0, 1)
    index = ghost.column_names.index(field)
    ghost = ghost.set_column(
        index, ghost.schema.field(index), pa.array(["another-scope"], type=pa.string())
    )
    spy.dataset = ds.dataset(pa.concat_tables([original, ghost]))
    result = read(setup, setup.query("L2"))
    assert result.data["quantity"].to_pylist() == [0, 2**60 + 3, None]


def test_native_future_never_falls_back_to_active_or_underlying_rows(setup):
    query = setup.query()
    query["identity"] = next(b for b in setup.snapshot.bindings if b["source_symbol"] == "SRZ6")[
        "selector"
    ]
    result = read(setup, query)
    assert result.envelope["status"] == "empty"
    assert result.data.num_rows == 0


def test_existing_output_without_read_pins_is_rejected(setup):
    bundle = copy.deepcopy(setup.bundle)
    del bundle["tables"]["research_indicator_frames"]
    snapshot = setup.context(bundle)
    with pytest.raises(SchemaValidationError, match="no explicit read pins"):
        read_native_frame(
            snapshot, NativeReadRequest.from_payload(setup.query("L1"), snapshot.namespace)
        )
    assert not setup.state["scans"]


def test_row_and_wire_budget_overflow_are_rejections_not_slices(setup):
    for query in [setup.query(row_limit=2), setup.query(byte_limit=1)]:
        with pytest.raises(SchemaValidationError, match="budget"):
            read(setup, query)


def test_duplicate_native_grain_is_rejected_instead_of_deduplicated(setup):
    spy = setup.frames["bars.delta"]
    original = spy.dataset.to_table()
    spy.dataset = ds.dataset(pa.concat_tables([original, original.slice(0, 1)]))
    with pytest.raises(SchemaValidationError, match="duplicate native grain"):
        read(setup)


def test_v1_cache_namespace_period_and_projection_order_never_alias(setup):
    class TraceCache(SnapshotReadCache):
        def get(self, namespace, key):
            self.last = namespace, key
            return super().get(namespace, key)

    trace = TraceCache(max_bytes=2_000_000)
    read(setup, cache=trace)
    namespace, key = trace.last
    cache = TraceCache(max_bytes=2_000_000)
    cache.put("legacy-v1", key, b"not a vNext Arrow carrier")
    assert read(setup, cache=cache).data.num_rows == 3
    assert cache.last[0] == namespace != "legacy-v1"
    keys = {cache.last[1]}
    for query in [
        setup.query(projection=["quantity", "close"]),
        setup.query(start="2026-10-20T00:00:00Z"),
    ]:
        read(setup, query, cache=cache)
        keys.add(cache.last[1])
    assert len(keys) == 3


def test_byte_lru_evicts_without_mutable_buffers_or_disk():
    cache = SnapshotReadCache(max_bytes=14)
    cache.put("n", "a", b"12345")
    cache.put("n", "b", b"67890")
    cache.get("n", "a")
    cache.put("n", "c", b"abcde")
    assert cache.get("n", "b") is None
    assert cache.get("n", "a") == b"12345" and cache.size_bytes <= 14
    with pytest.raises(TypeError):
        cache.put("n", "z", bytearray(b"mutable"))


def test_opt_in_http_consumer_uses_public_owners_and_preserves_arrow(setup, monkeypatch):
    monkeypatch.setattr(
        data_inspector,
        "_read_sorted_rows",
        lambda *a, **k: pytest.fail("legacy row-list path used"),
    )
    app = data_inspector.create_app(
        snapshot_reader=setup.snapshot, snapshot_cache=SnapshotReadCache(max_bytes=2_000_000)
    )
    with TestClient(app) as client:
        for layer in ["bars", "L0", "L1", "L2"]:
            response = client.post("/api/data-inspector/snapshot-read", json=setup.query(layer))
            assert response.status_code == 200
            assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
            result = decode_projection(response.content, setup.snapshot.namespace)
            assert result.data["quantity"].to_pylist() == [0, 2**60 + 3, None]
        invalid = setup.query(series_type="continuous_front")
        assert client.post("/api/data-inspector/snapshot-read", json=invalid).status_code == 422
        invalid = setup.query()
        invalid["known_at"] = "2020-01-01T00:00:00Z"
        assert client.post("/api/data-inspector/snapshot-read", json=invalid).status_code == 422
    with TestClient(data_inspector.create_app()) as client:
        assert (
            client.post("/api/data-inspector/snapshot-read", json=setup.query()).status_code == 503
        )
