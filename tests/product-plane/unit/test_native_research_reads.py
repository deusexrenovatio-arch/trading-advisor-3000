"""Bounded wire guarantees; native graph/storage proof remains a separate real run."""

import copy
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    atomic_json,
)
from trading_advisor_3000.product_plane.common.immutable_files import (
    canonical_bytes,
    pin,
    pinned_bytes,
)
from trading_advisor_3000.product_plane.common.snapshot_read_cache import SnapshotReadCache
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research import native_reads as owner

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"
LITERAL_CLOSE = Decimal("1234567890123456789012345678.1234567890")


def load(name):
    return json.loads((FIXTURES / name).read_bytes())


class RecordingCache(SnapshotReadCache):
    def __init__(self):
        super().__init__(max_bytes=16777216)
        self.gets = self.hits = 0
        self.last_slot = None

    def get(self, namespace, key):
        self.gets += 1
        result = super().get(namespace, key)
        self.hits += result is not None
        return result

    def put(self, namespace, key, value):
        self.last_slot = (namespace, key)
        super().put(namespace, key, value)


@pytest.fixture
def native_reader(tmp_path, monkeypatch):
    context = load("native_research_l0_context.v1.json")
    context["policy_sha256"] = owner.sha(context["policy"])
    context["layout_sha256"] = owner.sha(owner.l0_layouts())
    context["quality"]["sha256"] = owner.sha(context["quality"]["policy"])
    numerical = context["numerical_identity"]
    numerical["policy_sha256"] = context["policy_sha256"]
    numerical["digest"] = owner.sha({k: v for k, v in numerical.items() if k != "digest"})
    context["research_context_id"] = "NCTX:" + owner.sha(
        {k: v for k, v in context.items() if k != "research_context_id"}
    )
    schema = owner.physical_schema(owner.l0_layouts()["research_bar_views"])
    units = load("native_observation_units.v1.json")
    rows = []
    for ordinal in range(2):
        row = {}
        for field in schema:
            row[field.name] = (
                None
                if field.nullable
                else "fixture"
                if pa.types.is_string(field.type)
                else False
                if pa.types.is_boolean(field.type)
                else 0.0
                if pa.types.is_floating(field.type)
                else 0
            )
        row.update(
            namespace="synthetic-only",
            asset_id="ASSET:fixture",
            instrument_id="INSTR:fixture",
            provider="moex_iss",
            venue="MOEX",
            engine="stock",
            market="shares",
            board="TQBR",
            source_symbol="SYNTHETIC",
            timeframe="1m",
            source_begin=f"2020-01-02 12:0{ordinal}:00",
            source_end=f"2020-01-02 12:0{ordinal}:59",
            open=LITERAL_CLOSE,
            high=LITERAL_CLOSE,
            low=LITERAL_CLOSE,
            close=LITERAL_CLOSE,
            volume=None,
            open_interest=None,
            volume_state="unavailable",
            volume_null_reason="not_reported_by_source",
            open_interest_state="not_applicable",
            open_interest_null_reason="instrument_kind",
            source_units_json=canonical_bytes(units).decode(),
            value_origin="exchange_observation",
            simulation_id=None,
            overlay_sha256=None,
            body_sha256="b" * 64,
            receipt_sha256="c" * 64,
            row_ordinal=ordinal,
            research_context_id=context["research_context_id"],
            research_recipe_version=context["policy"]["version"],
            bar_index=ordinal,
            slice_role="analysis",
            execution_eligible=False,
        )
        rows.append(row)
    data = pa.Table.from_pylist(rows, schema=schema)
    context_path, intent_path, completion_path = (
        tmp_path / name for name in ["context.json", "intent.json", "completion.json"]
    )
    atomic_json(context_path, context, immutable=True)
    atomic_json(intent_path, {"role": "unit graph dependency"}, immutable=True)
    completion = load("native_research_l0_completion.v1.json")
    completion.update(context=pin(tmp_path, context_path), intent=pin(tmp_path, intent_path))
    atomic_json(completion_path, completion, immutable=True)
    request = load("native_research_read_inputs.v1.json")["request"]
    request["completion"] = pin(tmp_path, completion_path)
    reference = load("native_research_read_inputs.v1.json")["table"]
    graph = {
        "context": context,
        "completion": completion,
        "manifest": {"tables": {"research_bar_views": reference}},
        "tables": {"research_bar_views": data},
    }
    state = {"verifications": 0, "context_path": context_path, "graph": graph}

    class VerifiedGraph:
        def __init__(self, root, completion_pin):
            self.root, self.completion_pin = root, completion_pin

        @classmethod
        def _verified(cls, root, completion):
            reader = cls(root, completion)
            return reader, reader.verify()

        def verify(self):
            state["verifications"] += 1
            pinned_bytes(self.root, self.completion_pin)
            pinned_bytes(self.root, completion["context"])
            pinned_bytes(self.root, completion["intent"])
            return graph

    class PinnedTable:
        def to_pyarrow_dataset(self):
            return ds.dataset(data)

    monkeypatch.setattr(owner, "NativeResearchSnapshot", VerifiedGraph)
    monkeypatch.setattr(owner, "open_pinned_table", lambda reference: PinnedTable())
    monkeypatch.setattr(owner, "observation_read_code_sha256", lambda: "a" * 64)
    return owner.NativeResearchReader(tmp_path), request, state


def test_roundtrip_preserves_literal_decimal_null_lineage_and_flags(native_reader):
    reader, request, _ = native_reader
    cache = RecordingCache()
    result = reader.read(request, cache=cache)
    decoded = owner.NativeResearchRead.from_ipc(result.to_ipc())
    assert decoded.table["close"][0].as_py() == LITERAL_CLOSE
    assert decoded.table.schema.field("close").type == pa.decimal128(38, 10)
    assert decoded.table["volume"].null_count == 2
    assert decoded.table["source_end"].to_pylist() == ["2020-01-02 12:00:59", "2020-01-02 12:01:59"]
    assert decoded.table["row_ordinal"].to_pylist() == [0, 1]
    assert decoded.table["execution_eligible"].to_pylist() == [False, False]
    assert decoded.envelope["identity"]["source_units"] == load("native_observation_units.v1.json")
    assert decoded.envelope["limitations"]["historical_PIT_verified"] is False
    hot = reader.read(request, cache=cache)
    assert cache.hits == 1
    assert hot.table.equals(result.table, check_metadata=True)
    hot.envelope["identity"]["tradable"] = False
    assert hot.envelope["identity"]["tradable"] is True


def test_empty_is_typed_and_overflow_rejects_without_truncation(native_reader):
    reader, request, _ = native_reader
    empty = reader.read(
        {
            **request,
            "source_begin_from": "2030-01-01 00:00:00",
            "source_begin_to": "2030-01-02 00:00:00",
        }
    )
    assert empty.table.num_rows == 0
    assert empty.table.schema.field("close").type == pa.decimal128(38, 10)
    assert owner.NativeResearchRead.from_ipc(empty.to_ipc()).table.schema.equals(
        empty.table.schema, check_metadata=True
    )
    with pytest.raises(SchemaValidationError, match="row budget"):
        reader.read({**request, "row_limit": 1})


def test_full_wire_budget_counts_the_metadata(native_reader):
    reader, request, _ = native_reader
    result = reader.read(request)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, result.table.schema) as writer:
        writer.write_table(result.table)
    payload_bytes = len(sink.getvalue())
    assert len(result.to_ipc()) > payload_bytes + 1
    envelope = result.envelope
    envelope["request"]["byte_limit"] = payload_bytes + 1
    with pytest.raises(SchemaValidationError, match="full wire byte budget"):
        owner.make_native_research_read(result.table, envelope)


def test_invalid_projection_context_and_duplicate_grain_reject(native_reader):
    reader, request, _ = native_reader
    with pytest.raises(SchemaValidationError, match="unknown.*projection"):
        reader.read({**request, "projection": ["not_a_metric"]})
    with pytest.raises(SchemaValidationError):
        reader.read({**request, "context": {**request["context"], "board": "OTHER"}})
    result = reader.read(request)
    duplicate = pa.concat_tables([result.table.slice(0, 1), result.table.slice(0, 1)])
    with pytest.raises(SchemaValidationError, match="duplicate.*grain"):
        owner.make_native_research_read(duplicate, result.envelope)
    foreign = result.table.set_column(
        result.table.column_names.index("board"),
        result.table.schema.field("board"),
        pa.array(["OTHER", "OTHER"]),
    )
    with pytest.raises(SchemaValidationError, match="row/context mismatch"):
        owner.make_native_research_read(foreign, result.envelope)


def test_hash_pinned_source_is_checked_before_each_cache_lookup(native_reader):
    reader, request, state = native_reader
    cache = RecordingCache()
    reader.read(request, cache=cache)
    reader.read(request, cache=cache)
    assert cache.hits == 1 and state["verifications"] == 2
    lookups = cache.gets
    state["context_path"].write_text("changed source context", encoding="utf-8")
    with pytest.raises(SnapshotUnavailableError, match="hash mismatch"):
        reader.read(request, cache=cache)
    assert cache.gets == lookups


def test_cache_contract_runtime_and_projection_are_distinct(native_reader, monkeypatch):
    reader, request, _ = native_reader
    cache = RecordingCache()
    reader.read(request, cache=cache)
    assert cache.hits == 0
    monkeypatch.setattr(owner, "observation_read_code_sha256", lambda: "b" * 64)
    reader.read(request, cache=cache)
    assert cache.hits == 0
    projected = reader.read({**request, "projection": ["close"]}, cache=cache)
    assert "volume" not in projected.table.column_names and cache.hits == 0
    cached_slot = cache.last_slot
    other = reader.read(request)
    cache.put(*cached_slot, other.to_ipc())
    with pytest.raises(SchemaValidationError, match="cache provenance"):
        reader.read({**request, "projection": ["close"]}, cache=cache)


@pytest.mark.parametrize(
    "payload", [{}, [], {"layer": "L0"}, {"layer": "L0", "completion": "not a pin"}]
)
def test_malformed_request_rejects_before_graph_lookup(tmp_path, monkeypatch, payload):
    reader = owner.NativeResearchReader(tmp_path)
    monkeypatch.setattr(
        reader,
        "_verified_source",
        lambda *args: pytest.fail("malformed request reached graph lookup"),
    )
    with pytest.raises(SchemaValidationError):
        reader.read(payload)


@pytest.mark.parametrize("path", ["/outside.json", "../outside.json", "commands\\completion.json"])
def test_unsafe_relative_pin_rejects_before_graph_lookup(native_reader, monkeypatch, path):
    reader, request, _ = native_reader
    monkeypatch.setattr(
        reader, "_verified_source", lambda *args: pytest.fail("invalid pin reached graph lookup")
    )
    with pytest.raises(SchemaValidationError):
        reader.read({**request, "completion": {**request["completion"], "path": path}})


def test_valid_but_unavailable_pin_is_unavailable_source(native_reader):
    reader, request, _ = native_reader
    with pytest.raises(SnapshotUnavailableError):
        reader.read(
            {**request, "completion": {"path": "absent/completion.json", "sha256": "a" * 64}}
        )


@pytest.mark.parametrize("wire", [b"not IPC", b"", b"ARROW1"])
def test_invalid_ipc_is_schema_error(wire):
    with pytest.raises(SchemaValidationError):
        owner.NativeResearchRead.from_ipc(wire)


def test_wire_envelope_and_trailing_bytes_reject(native_reader):
    reader, request, _ = native_reader
    result = reader.read(request)
    with pytest.raises(SchemaValidationError, match="trailing"):
        owner.NativeResearchRead.from_ipc(result.to_ipc() + b"tail")
    metadata_key = owner.native_read_rules()["metadata_key"].encode()
    table = result.table.replace_schema_metadata({metadata_key: b"not JSON"})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    with pytest.raises(SchemaValidationError):
        owner.NativeResearchRead.from_ipc(sink.getvalue().to_pybytes())


def test_shadow_is_content_addressed_retryable_and_rechecks_parents(tmp_path, monkeypatch):
    reader = owner.NativeResearchReader(tmp_path)
    atomic_json(tmp_path / "scope.json", {"role": "unit verification"})
    seed = load("native_research_read_inputs.v1.json")
    sources = {}
    for layer in ["L0", "L1", "L2"]:
        context = load(
            "native_research_l0_context.v1.json"
            if layer == "L0"
            else f"native_research_feature_context.{layer.lower()}.v1.json"
        )
        sources[layer] = {
            "layer": layer,
            "completion": {"path": f"commands/{layer}/completion.json", "sha256": layer[-1] * 64},
            "context": {"path": f"commands/{layer}/context.json", "sha256": "b" * 64},
            "intent": {"path": f"commands/{layer}/intent.json", "sha256": "c" * 64},
            "snapshot": {
                "path": f"commands/{layer}/snapshot.json",
                "sha256": "d" * 64,
                "snapshot_id": "snapshot-" + layer,
            },
            "table_name": owner.TABLES[layer],
            "table": {**seed["table"], "table_path": "/research-trial/" + layer + ".delta"},
            "context_payload": context,
        }
    for layer, parent in [("L1", "L0"), ("L2", "L1")]:
        for field in ["completion", "context", "snapshot"]:
            sources[layer]["context_payload"]["source"][field] = copy.deepcopy(
                sources[parent][field]
            )
    original = sources["L0"]["context_payload"]["source"]
    data_completion = {
        name: original[name] for name in ["snapshot", "metadata", "assignments", "source_manifest"]
    }
    data_completion["intent"] = {"path": "data/intent.json", "sha256": "f" * 64}
    data_graph = {
        "metadata": {"namespace": original["namespace"]},
        "completion": data_completion,
        "manifest": {
            "tables": {"native_bars": {**seed["table"], "table_path": "/trial/native_bars.delta"}}
        },
    }

    class DataOwner:
        def __init__(self, *args):
            pass

        @classmethod
        def _verified(cls, root, completion):
            reader = cls(root, completion)
            return reader, reader.verify()

        def verify(self):
            return data_graph

    monkeypatch.setattr(owner, "NativeObservationSnapshot", DataOwner)
    monkeypatch.setattr(reader, "_verified_source", lambda layer, completion: (sources[layer], {}))
    completions = {layer: source["completion"] for layer, source in sources.items()}
    first = reader.save_shadow(completions)
    assert reader.save_shadow(completions) == first
    descriptor = reader.open_shadow(first)
    assert Path(first["path"]).stem == owner.sha(descriptor)
    assert not (tmp_path / "published_current.json").exists()
    sources["L1"]["context_payload"]["source"]["completion"] = {
        "path": "other/completion.json",
        "sha256": "e" * 64,
    }
    with pytest.raises(SchemaValidationError, match="mixed-generation"):
        reader.open_shadow(first)
    sources["L1"]["context_payload"]["source"]["completion"] = completions["L0"]
    path = tmp_path / first["path"]
    path.write_text("conflicting descriptor", encoding="utf-8")
    with pytest.raises(ValueError, match="immutable snapshot"):
        reader.save_shadow(completions)
    assert path.read_text(encoding="utf-8") == "conflicting descriptor"


def test_l2_read_metadata_does_not_use_compute_parent_admission(native_reader, monkeypatch):
    reader, request, state = native_reader
    context = load("native_research_feature_context.l2.v1.json")
    context["research_context_id"] = state["graph"]["context"]["research_context_id"]
    context["source"]["research_context_id"] = context["research_context_id"]
    policy = owner.feature_layout("L2", context["policy"]["profile_version"])
    context["policy_sha256"] = owner.sha(context["policy"])
    context["layout_sha256"] = owner.sha({"research_derived_indicator_frames": policy})
    context["quality"]["sha256"] = owner.sha(context["quality"]["policy"])
    numerical = context["numerical_identity"]
    numerical["engines"].update(pandas_ta_classic="0.4.47", talib_absent=True)
    numerical["policy_sha256"] = context["policy_sha256"]
    numerical["digest"] = owner.sha({k: v for k, v in numerical.items() if k != "digest"})
    context["feature_context_id"] = "NFCTX:" + owner.sha(
        {k: v for k, v in context.items() if k != "feature_context_id"}
    )
    schema = owner.physical_schema(policy)
    rows = state["graph"]["tables"]["research_bar_views"].to_pylist()
    for row in rows:
        for field in schema:
            if field.name not in row:
                row[field.name] = None if field.nullable else "synthetic-context"
        row["derived_context_id"] = context["feature_context_id"]
        row["derived_profile_version"] = context["policy"]["profile_version"]
    table = pa.Table.from_pylist(rows, schema=schema)
    completion = load("native_research_feature_completion.l2.v1.json")
    path = reader.root / "l2-completion.json"
    atomic_json(path, completion, immutable=True)
    graph = {
        "context": context,
        "completion": completion,
        "manifest": {
            "tables": {
                "research_derived_indicator_frames": load("native_research_read_inputs.v1.json")[
                    "table"
                ]
            }
        },
        "tables": {"research_derived_indicator_frames": table},
    }

    class L2Owner:
        def __init__(self, *args):
            pass

        @classmethod
        def _verified(cls, root, completion):
            reader = cls(root, completion)
            return reader, reader.verify()

        def verify(self):
            return graph

        def compute_metadata(self):
            pytest.fail("read-side L2 crossed the compute-parent boundary")

    class PinnedL2:
        def to_pyarrow_dataset(self):
            return ds.dataset(table)

    monkeypatch.setattr(owner, "NativeFeatureSnapshot", L2Owner)
    monkeypatch.setattr(owner, "open_pinned_table", lambda reference: PinnedL2())
    result = reader.read(
        {
            **request,
            "layer": "L2",
            "completion": pin(reader.root, path),
            "projection": ["close", "rolling_high_20"],
        }
    )
    assert result.envelope["source"]["layer"] == "L2"
    assert result.envelope["source"]["table_name"] == "research_derived_indicator_frames"
    assert result.table["close"][0].as_py() == LITERAL_CLOSE
    assert result.table["rolling_high_20"].null_count == 2


@pytest.mark.parametrize("mutation", ["missing_key", "bad_base64"])
def test_malformed_ipc_envelope_is_schema_error(native_reader, mutation):
    reader, request, _ = native_reader
    result = reader.read(request)
    envelope = result.envelope
    if mutation == "missing_key":
        envelope.pop("request")
    else:
        envelope["payload"]["schema_ipc_base64"] = "invalid base64!"
    key = owner.native_read_rules()["metadata_key"].encode()
    table = result.table.replace_schema_metadata({key: canonical_bytes(envelope)})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    with pytest.raises(SchemaValidationError):
        owner.NativeResearchRead.from_ipc(sink.getvalue().to_pybytes())


def test_foreign_root_and_changed_completion_pin_reject_before_cache(native_reader, tmp_path):
    reader, request, _ = native_reader
    cache = RecordingCache()
    reader.read(request, cache=cache)
    lookups = cache.gets
    with pytest.raises(SnapshotUnavailableError):
        owner.NativeResearchReader(tmp_path / "foreign-root").read(request, cache=cache)
    with pytest.raises(SnapshotUnavailableError, match="hash mismatch"):
        reader.read(
            {**request, "completion": {**request["completion"], "sha256": "e" * 64}}, cache=cache
        )
    assert cache.gets == lookups


def test_changed_physical_table_pin_rejects_before_cache(native_reader, monkeypatch):
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common.data_snapshots import (
        capture_table,
        open_pinned_table,
    )

    reader, request, state = native_reader
    path = reader.root / "physical.delta"
    write_deltalake(path, state["graph"]["tables"]["research_bar_views"])
    state["graph"]["manifest"]["tables"]["research_bar_views"] = capture_table(path)
    monkeypatch.setattr(owner, "open_pinned_table", open_pinned_table)
    cache = RecordingCache()
    reader.read(request, cache=cache)
    lookups = cache.gets
    parquet = next(path.glob("*.parquet"))
    with parquet.open("ab") as stream:
        stream.write(b"changed-size")
    with pytest.raises(SnapshotUnavailableError, match="size changed"):
        reader.read(request, cache=cache)
    assert cache.gets == lookups


@pytest.mark.parametrize("kind", ["Data", "L0", "Feature"])
def test_public_methods_never_reuse_a_previous_constructor_graph(tmp_path, monkeypatch, kind):
    from trading_advisor_3000.product_plane.data_plane import native_observations as data_owner
    from trading_advisor_3000.product_plane.research import native_features as feature_owner
    from trading_advisor_3000.product_plane.research import native_observations as l0_owner

    cls = {
        "Data": data_owner.NativeObservationSnapshot,
        "L0": l0_owner.NativeResearchSnapshot,
        "Feature": feature_owner.NativeFeatureSnapshot,
    }[kind]
    path = tmp_path / "source.json"
    atomic_json(path, {"role": "fresh source pin"}, immutable=True)
    completion = pin(tmp_path, path)
    namespace = load("native_observation_snapshot.v1.json")["namespace"]

    def fresh(self):
        try:
            pinned_bytes(self.root, json.loads(self._completion_json))
        except (OSError, ValueError) as exc:
            raise SnapshotUnavailableError("changed public source") from exc
        return {"metadata": {"namespace": namespace}}

    monkeypatch.setattr(cls, "verify", fresh)
    instance = cls(tmp_path, completion)
    path.write_text("changed after constructor", encoding="utf-8")
    with pytest.raises(SnapshotUnavailableError):
        instance.verify()
    with pytest.raises(SnapshotUnavailableError):
        instance.compute_metadata()
    with pytest.raises(SnapshotUnavailableError):
        instance.read_spark(None)
    if kind == "Data":
        request = load("native_observation_request.v1.json")
        with pytest.raises(SnapshotUnavailableError):
            instance.read(request)
    else:
        with pytest.raises(SnapshotUnavailableError):
            instance.read_tree()
        with pytest.raises(SnapshotUnavailableError):
            (instance.read_bars if kind == "L0" else instance.read_frame)()
    with pytest.raises(SnapshotUnavailableError):
        cls._verified(tmp_path / "other-root", completion)


@pytest.mark.parametrize("mismatch", ["root", "completion"])
def test_private_data_batch_binds_exact_root_and_completion(tmp_path, monkeypatch, mismatch):
    from trading_advisor_3000.product_plane.data_plane import native_observations as data_owner

    reader = object.__new__(data_owner.NativeObservationSnapshot)
    reader.root = (tmp_path / "configured").resolve()
    completion = {"path": "commands/completion.json", "sha256": "a" * 64}
    reader._completion_json = json.dumps(completion)
    reader._namespace_json = canonical_bytes(
        load("native_observation_snapshot.v1.json")["namespace"]
    ).decode()
    graph = {
        "completion_pin": {**completion},
        "manifest": {
            "tables": {"native_bars": {"table_path": (reader.root / "bars.delta").as_posix()}}
        },
    }
    if mismatch == "root":
        graph["manifest"]["tables"]["native_bars"]["table_path"] = (
            tmp_path / "foreign" / "bars.delta"
        ).as_posix()
    else:
        graph["completion_pin"]["sha256"] = "b" * 64
    monkeypatch.setattr(
        data_owner, "open_pinned_table", lambda *args: pytest.fail("foreign graph reached Delta IO")
    )
    with pytest.raises(SchemaValidationError, match="foreign root/completion"):
        reader._compute_metadata(graph)
    with pytest.raises(SchemaValidationError, match="foreign root/completion"):
        reader._read_verified(graph, load("native_observation_request.v1.json"))


def test_private_data_batch_keeps_each_query_preflight(monkeypatch):
    from trading_advisor_3000.product_plane.data_plane import native_observations as data_owner

    reader = object.__new__(data_owner.NativeObservationSnapshot)
    reader._namespace_json = canonical_bytes(
        load("native_observation_snapshot.v1.json")["namespace"]
    ).decode()
    request = load("native_observation_request.v1.json")
    request["source_begin_from"] = "2020-02-30 00:00:00"
    monkeypatch.setattr(
        reader,
        "_require_graph_scope",
        lambda *args: pytest.fail("invalid query reached graph handling"),
    )
    with pytest.raises(SchemaValidationError):
        reader._read_verified({}, request)


def correctly_pinned_receipt_query(reader, query, receipt):
    raw = json.dumps(receipt, allow_nan=False).encode("utf-8")
    path = reader.root / "non-object-completion.json"
    path.write_bytes(raw)
    completion = {
        "path": path.relative_to(reader.root).as_posix(),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    assert pinned_bytes(reader.root, completion) == raw
    return {**query, "completion": completion}


@pytest.fixture(params=[[], None], ids=["array", "null"])
def non_object_receipt(native_reader, request):
    reader, query, _ = native_reader
    return reader, correctly_pinned_receipt_query(reader, query, request.param), RecordingCache()


def test_non_object_receipt_reader_is_unavailable_before_cache(non_object_receipt):
    reader, query, cache = non_object_receipt
    with pytest.raises(SnapshotUnavailableError, match="completion must be an object"):
        reader.read(query, cache=cache)
    assert cache.gets == 0


def test_non_object_receipt_http_returns_409_before_cache(non_object_receipt):
    from fastapi.testclient import TestClient

    from trading_advisor_3000.product_plane.interfaces.data_inspector import create_app

    reader, query, cache = non_object_receipt
    with TestClient(
        create_app(data_root=reader.root, research_reader=reader, snapshot_cache=cache),
        raise_server_exceptions=False,
    ) as client:
        response = client.post("/api/data-inspector/native-research-read", json=query)
    assert response.status_code == 409
    assert "completion must be an object" in response.json()["detail"]
    assert cache.gets == 0


@pytest.mark.parametrize(
    "receipt", ["receipt", 1, 1.5, True], ids=["string", "integer", "float", "boolean"]
)
def test_hash_pinned_scalar_receipt_is_unavailable_before_cache(native_reader, receipt):
    reader, query, _ = native_reader
    query = correctly_pinned_receipt_query(reader, query, receipt)
    cache = RecordingCache()
    with pytest.raises(SnapshotUnavailableError, match="completion must be an object"):
        reader.read(query, cache=cache)
    assert cache.gets == 0


def test_valid_hash_finite_metric_cache_poison_cannot_override_fresh_research(native_reader):
    reader, query, state = native_reader
    query = {**query, "projection": [*query["projection"], "true_range"]}
    cache = RecordingCache()
    cold = reader.read(query, cache=cache)
    assert cold.table["true_range"].to_pylist() == [0.0, 0.0]
    assert reader.read(query, cache=cache).table.equals(cold.table, check_metadata=True)
    poisoned = cold.table.set_column(
        cold.table.column_names.index("true_range"),
        cold.table.schema.field("true_range"),
        pa.array([0.0, 1.0], type=pa.float64()),
    )
    wire = owner.make_native_research_read(poisoned, cold.envelope).to_ipc()
    assert owner.NativeResearchRead.from_ipc(wire).table["true_range"].to_pylist() == [0.0, 1.0]
    cache.put(*cache.last_slot, wire)
    before = state["verifications"]
    with pytest.raises(SchemaValidationError, match="freshly verified role"):
        reader.read(query, cache=cache)
    assert state["verifications"] == before + 1
