"""CF wire guarantees with literal Arrow oracles; real graph proof is separate."""

import base64
import copy
import hashlib
import json
import runpy
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import Schema
from fastapi.testclient import TestClient

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
from trading_advisor_3000.product_plane.interfaces.data_inspector import create_app
from trading_advisor_3000.product_plane.research import native_cf_reads as owner
from trading_advisor_3000.product_plane.research.native_cf_policy import cf_layouts

# Reuse only the closed metadata shape, never the implementation's values as oracle.
CONTRACT_EXAMPLE = runpy.run_path(
    str(Path(__file__).parents[1] / "contracts/test_native_cf_contracts.py")
)["example"]
CLOSE = Decimal("1234567890123456789012345678.1234567890")


class RecordingCache(SnapshotReadCache):
    def __init__(self):
        super().__init__(max_bytes=16777216)
        self.gets = self.hits = 0
        self.slot = None

    def get(self, namespace, key):
        self.gets += 1
        value = super().get(namespace, key)
        self.hits += value is not None
        return value

    def put(self, namespace, key, wire):
        self.slot = (namespace, key)
        super().put(namespace, key, wire)


def blank(schema):
    return {
        f.name: None
        if f.nullable
        else "fixture"
        if pa.types.is_string(f.type)
        else False
        if pa.types.is_boolean(f.type)
        else Decimal(0)
        if pa.types.is_decimal(f.type)
        else 0
        for f in schema
    }


@pytest.fixture
def cf_reader(tmp_path, monkeypatch):
    namespace, payloads = CONTRACT_EXAMPLE()
    source = copy.deepcopy(payloads["compute"])
    context = source["context_payload"]
    definitions = context["definitions"]
    numerical = context["numerical_identity"]
    numerical["digest"] = owner.sha({k: v for k, v in numerical.items() if k != "digest"})
    context["used_references"] = [
        dict(
            copy.deepcopy(context["used_references"][0]),
            definition_json=canonical_bytes(definitions[0]).decode(),
            side=side,
            instrument_id=instrument,
            source_begin=begin,
            source_end=end,
        )
        for side, instrument, begin, end in [
            ("old", "SYNTH:SRU6", "2026-09-14 23:49:00", "2026-09-14 23:49:59"),
            ("new", "SYNTH:SRZ6", "2026-09-15 06:59:00", "2026-09-15 06:59:59"),
        ]
    ]
    context["cf_context_id"] = "CFCTX:" + owner.sha(
        {k: v for k, v in context.items() if k != "cf_context_id"}
    )
    layouts = cf_layouts("CF")
    tables = {}
    schema = owner.physical_schema(layouts["bars"])
    rows = []
    for definition in definitions:
        rolled = definition["roll"]["offset_sessions"] == 2
        for index, day in enumerate([14, 15]):
            component = definition["components"][int(rolled and index == 1)]
            row = blank(schema)
            row.update(
                namespace=namespace["namespace"],
                provider="moex_iss",
                venue="MOEX",
                engine="futures",
                market="forts",
                board="RFUD",
                asset_id="ASSET:SBER",
                source_instrument_id=component["instrument"]["instrument_id"],
                active_instrument_id=component["instrument"]["instrument_id"],
                instrument_id=None,
                source_symbol=component["source_symbol"],
                timeframe="1m",
                source_begin=f"2026-09-{day} " + ("23:49:00" if day == 14 else "06:59:00"),
                source_end=f"2026-09-{day} " + ("23:49:59" if day == 14 else "06:59:59"),
                open=CLOSE,
                high=CLOSE,
                low=CLOSE,
                close=CLOSE,
                volume=None,
                open_interest=None,
                volume_state="unavailable",
                volume_null_reason="not_reported_by_source",
                open_interest_state="unavailable",
                open_interest_null_reason="not_reported_by_source",
                source_units_json=canonical_bytes(component["units"]).decode(),
                value_origin="exchange_observation",
                simulation_id=None,
                overlay_sha256=None,
                body_sha256="b" * 64,
                receipt_sha256="c" * 64,
                source_block="candles",
                page_start=0,
                row_ordinal=index,
                normalization_recipe="synthetic-observation-only",
                cf_definition_json=canonical_bytes(definition).decode(),
                cf_context_id=context["cf_context_id"],
                cf_recipe_version="native_cf_composition.v1",
                planned_roll_ordinal=int(rolled and index == 1),
                roll_epoch=int(rolled and index == 1),
                is_first_bar_after_roll=bool(rolled and index == 1),
                reference_calculation_embargo_source_label=f"2026-09-{day} "
                + ("23:49:59" if day == 14 else "06:59:59"),
                true_range=None if index == 0 else 0.0,
                log_ret_1=None,
            )
            rows.append(row)
    tables["bars"] = pa.Table.from_pylist(rows, schema=schema)
    event_schema = owner.physical_schema(layouts["events"])
    event = blank(event_schema)
    event.update(
        cf_definition_json=canonical_bytes(definitions[0]).decode(),
        timeframe="1m",
        cf_context_id=context["cf_context_id"],
        roll_ordinal=1,
        planned_roll_ordinal=1,
        switch_source_date="2026-09-15",
        observed_switch_source_begin="2026-09-15 06:59:00",
        old_instrument_id="SYNTH:SRU6",
        new_instrument_id="SYNTH:SRZ6",
        old_source_begin="2026-09-14 23:49:00",
        old_source_end="2026-09-14 23:49:59",
        new_source_begin="2026-09-15 06:59:00",
        new_source_end="2026-09-15 06:59:59",
        old_body_sha256="b" * 64,
        new_body_sha256="b" * 64,
        old_receipt_sha256="c" * 64,
        new_receipt_sha256="c" * 64,
        old_reference_close=CLOSE,
        new_reference_close=CLOSE,
        additive_gap=0.0,
        reference_calculation_embargo_source_label="2026-09-15 06:59:59",
    )
    tables["events"] = pa.Table.from_pylist([event], schema=event_schema)
    tables["ladder"] = pa.Table.from_pylist(
        [event], schema=owner.physical_schema(layouts["ladder"])
    )
    qc_schema = owner.physical_schema(layouts["qc"])
    qcs = []
    for definition in definitions:
        row = blank(qc_schema)
        events = int(definition["roll"]["offset_sessions"] == 2)
        row.update(
            cf_definition_json=canonical_bytes(definition).decode(),
            timeframe="1m",
            cf_context_id=context["cf_context_id"],
            parent_inspected_rows=8,
            eligible_component_rows=6,
            selected_rows=2,
            analysis_rows=1,
            observed_events=events,
            ladder_rows=events,
            missing_reference_count=0,
            status="PASS",
        )
        qcs.append(row)
    tables["qc"] = pa.Table.from_pylist(qcs, schema=qc_schema)
    for item in source["tables"]:
        item["reference"]["row_count"] = tables[item["role"]].num_rows
    dependency = tmp_path / "source-fact.json"
    atomic_json(dependency, {"role": "unit source hash dependency"}, immutable=True)
    dependency_pin = pin(tmp_path, dependency)
    request = copy.deepcopy(payloads["read_request"])
    request["projection"] = ["close", "volume", "true_range"]
    state = {
        "verifications": 0,
        "role_reads": 0,
        "source": source,
        "tables": tables,
        "dependency": dependency,
    }

    class VerifiedRole:
        def _read_role_verified(self, graph, role):
            assert graph["token"] == state["verifications"]
            state["role_reads"] += 1
            return state["tables"][role]

    def verified(self, completion):
        state["verifications"] += 1
        try:
            pinned_bytes(self.root, dependency_pin)
        except (ValueError, OSError) as exc:
            raise SnapshotUnavailableError("source fact changed") from exc
        assert completion == state["source"]["completion"]
        return VerifiedRole(), {"token": state["verifications"]}, copy.deepcopy(state["source"])

    monkeypatch.setattr(owner.NativeCfReader, "_verified_source", verified)
    monkeypatch.setattr(owner, "cf_read_code_sha256", lambda: "a" * 64)
    return owner.NativeCfReader(tmp_path), request, state


def test_exact_decimal_NULL_component_lineage_and_two_definition_isolation(cf_reader):
    reader, request, state = cf_reader
    result = reader.read(request)
    decoded = owner.NativeCfRead.from_ipc(result.to_ipc())
    assert decoded.table["close"].to_pylist() == [CLOSE, CLOSE]
    assert decoded.table["close"].type == pa.decimal128(38, 10)
    assert decoded.table["volume"].null_count == 2
    assert decoded.table["true_range"].to_pylist() == [None, 0.0]
    assert decoded.table["instrument_id"].null_count == 2
    assert decoded.table["active_instrument_id"].to_pylist() == ["SYNTH:SRU6", "SYNTH:SRZ6"]
    assert decoded.namespace == state["source"]["namespace"]
    second = copy.deepcopy(request)
    second["definition"] = state["source"]["context_payload"]["definitions"][1]
    assert reader.read(second).table["active_instrument_id"].to_pylist() == [
        "SYNTH:SRU6",
        "SYNTH:SRU6",
    ]


def test_every_request_verifies_before_cache_and_decodes_hot_payload(cf_reader):
    reader, request, state = cf_reader
    cache = RecordingCache()
    cold = reader.read(request, cache=cache).to_ipc()
    assert reader.read(request, cache=cache).to_ipc() == cold
    assert state["verifications"] == 2 and cache.hits == 1
    state["dependency"].write_text("changed", encoding="utf-8")
    gets = cache.gets
    with pytest.raises(SnapshotUnavailableError):
        reader.read(request, cache=cache)
    assert cache.gets == gets


def test_empty_interval_auxiliary_E0_and_full_wire_or_row_overflow(cf_reader):
    reader, request, state = cf_reader
    empty = copy.deepcopy(request)
    empty["selection"] = {
        "kind": "source_key_interval",
        "source_begin_from": "2026-09-16 00:00:00",
        "source_begin_to": "2026-09-17 00:00:00",
    }
    assert reader.read(empty).table.num_rows == 0
    assert reader.read(empty).table.schema.field("close").type == pa.decimal128(38, 10)
    for role in ["events", "ladder", "qc"]:
        auxiliary = copy.deepcopy(request)
        auxiliary.update(
            role=role,
            definition=state["source"]["context_payload"]["definitions"][1],
            selection={"kind": "whole_role"},
            projection=["additive_gap"] if role != "qc" else ["status"],
        )
        result = reader.read(auxiliary)
        assert result.table.num_rows == (1 if role == "qc" else 0)
        assert "source_begin" not in result.table.column_names
        owner.NativeCfRead.from_ipc(result.to_ipc())
    with pytest.raises(SchemaValidationError, match="row budget"):
        reader.read({**request, "row_limit": 1})
    full = reader.read(request)
    payload_only_budget = len(owner._ipc(full.table)) + 128
    assert payload_only_budget < len(full.to_ipc())
    with pytest.raises(SchemaValidationError, match="full wire byte"):
        reader.read({**request, "byte_limit": payload_only_budget})


@pytest.mark.parametrize(
    "change",
    [
        {"root": "/foreign"},
        {"completion": {"path": "../escape.json", "sha256": "a" * 64}},
        {"layer": "L1"},
        {"projection": ["missing"]},
    ],
)
def test_closed_request_or_unknown_projection_cannot_hit_cache(cf_reader, change):
    reader, request, _ = cf_reader
    cache = RecordingCache()
    with pytest.raises(SchemaValidationError):
        reader.read({**request, **change}, cache=cache)
    assert cache.gets == 0


@pytest.mark.parametrize(
    "column,value",
    [
        ("active_instrument_id", "FOREIGN"),
        ("instrument_id", "SYNTH:SRU6"),
        ("cf_definition_json", "foreign-definition"),
        ("reference_calculation_embargo_source_label", "2026-09-13 00:00:00"),
    ],
)
def test_payload_FK_definition_or_embargo_tampering_rejects_even_with_new_hash(
    cf_reader, column, value
):
    reader, request, _ = cf_reader
    result = reader.read(request)
    table = result.table.set_column(
        result.table.column_names.index(column),
        result.table.schema.field(column),
        pa.array([value] * 2),
    )
    with pytest.raises(SchemaValidationError):
        owner.make_cf_read(table, result.envelope)


def test_nonfinite_or_float_replacement_and_duplicate_grain_reject(cf_reader):
    reader, request, _ = cf_reader
    result = reader.read(request)
    for column, values in [
        ("true_range", pa.array([float("nan"), 1.0])),
        ("close", pa.array([1.0, 2.0])),
    ]:
        table = result.table.set_column(result.table.column_names.index(column), column, values)
        with pytest.raises(SchemaValidationError):
            owner.make_cf_read(table, result.envelope)
    duplicate = pa.concat_tables([result.table, result.table.slice(0, 1)]).sort_by(
        [(n, "ascending") for n in ["cf_definition_json", "timeframe", "source_begin"]]
    )
    with pytest.raises(SchemaValidationError, match="duplicate CF grain"):
        owner.make_cf_read(duplicate, result.envelope)


@pytest.mark.parametrize(
    "metadata",
    [None, [], 1, True, {"source": None}, {"request": None, "source": {"namespace": {}}}],
)
def test_malformed_wire_metadata_is_controlled_schema_error(cf_reader, metadata):
    reader, request, _ = cf_reader
    result = reader.read(request)
    wire = owner._ipc(
        result.table.replace_schema_metadata(
            {owner.cf_read_rules()["metadata_key"].encode(): canonical_bytes(metadata)}
        )
    )
    with pytest.raises(SchemaValidationError):
        owner.NativeCfRead.from_ipc(wire)


def test_trailing_bytes_and_fresh_provenance_mismatch_reject(cf_reader):
    reader, request, _ = cf_reader
    cache = RecordingCache()
    result = reader.read(request, cache=cache)
    with pytest.raises(SchemaValidationError, match="trailing"):
        owner.NativeCfRead.from_ipc(result.to_ipc() + b"tail")
    altered = result.envelope
    altered["request"]["selection"]["source_begin_from"] = "2026-09-13 00:00:00"
    valid_wrong_request = owner.make_cf_read(result.table, altered).to_ipc()
    cache.put(*cache.slot, valid_wrong_request)
    with pytest.raises(SchemaValidationError, match="cache provenance"):
        reader.read(request, cache=cache)


def test_cache_namespace_definition_projection_and_current_code_are_distinct(
    cf_reader, monkeypatch
):
    reader, request, state = cf_reader
    cache = RecordingCache()
    reader.read(request, cache=cache)
    first = cache.slot
    assert first[0] == "ta3000.native_cf_read.v1"
    monkeypatch.setattr(owner, "cf_read_code_sha256", lambda: "b" * 64)
    reader.read(request, cache=cache)
    assert cache.slot != first and cache.hits == 0
    changed = copy.deepcopy(request)
    changed["definition"] = state["source"]["context_payload"]["definitions"][1]
    reader.read(changed, cache=cache)
    assert cache.hits == 0
    reader.read({**request, "projection": ["close"]}, cache=cache)
    assert cache.hits == 0


def test_opt_in_HTTP_CF_route_uses_public_reader_and_existing_error_statuses(cf_reader):
    reader, request, _ = cf_reader
    assert (
        TestClient(create_app(data_root=reader.root))
        .post("/api/data-inspector/native-cf-read", json=request)
        .status_code
        == 503
    )
    response = TestClient(
        create_app(data_root=reader.root, cf_reader=reader, snapshot_cache=RecordingCache())
    ).post("/api/data-inspector/native-cf-read", json=request)
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
    assert owner.NativeCfRead.from_ipc(response.content).table["close"].to_pylist() == [
        CLOSE,
        CLOSE,
    ]
    for error, status in [
        (SchemaValidationError("invalid"), 422),
        (SnapshotUnavailableError("changed pin"), 409),
    ]:

        class BrokenReader:
            def read(self, payload, *, cache=None):
                raise error

        assert (
            TestClient(create_app(data_root=reader.root, cf_reader=BrokenReader()))
            .post("/api/data-inspector/native-cf-read", json=request)
            .status_code
            == status
        )


def test_offline_rehashed_layout_cannot_omit_source_lineage(cf_reader):
    reader, request, _ = cf_reader
    result = reader.read(request)
    assert owner.NativeCfRead.from_ipc(result.to_ipc()).table.equals(result.table)
    envelope = result.envelope
    context = envelope["source"]["context_payload"]
    for layouts in [context["layouts"], context["policy_payload"]["layouts"]]:
        layouts["bars"]["fields"] = [
            f for f in layouts["bars"]["fields"] if f["name"] != "body_sha256"
        ]
    context["policy_sha256"] = owner.sha(context["policy_payload"])
    numerical = context["numerical_identity"]
    numerical["policy_sha256"] = context["policy_sha256"]
    numerical["digest"] = owner.sha({k: v for k, v in numerical.items() if k != "digest"})
    context["cf_context_id"] = "CFCTX:" + owner.sha(
        {k: v for k, v in context.items() if k != "cf_context_id"}
    )
    envelope["source"]["context"]["sha256"] = owner.sha(context)
    item = next(row for row in envelope["source"]["tables"] if row["role"] == "bars")
    item["layout"] = copy.deepcopy(context["layouts"]["bars"])
    item["reference"]["schema_hash"] = owner.sha(
        json.loads(Schema.from_pyarrow(owner.physical_schema(item["layout"])).to_json())
    )
    item["reference"]["layout_version"] = owner.sha({"partition_columns": []})
    table = result.table.drop(["body_sha256"])
    table = table.set_column(
        table.column_names.index("cf_context_id"),
        table.schema.field("cf_context_id"),
        pa.array([context["cf_context_id"]] * table.num_rows),
    )
    envelope["payload"] = {
        "row_count": table.num_rows,
        "columns": table.column_names,
        "schema_ipc_base64": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
        "ipc_sha256": hashlib.sha256(owner._ipc(table)).hexdigest(),
    }
    wire = owner._ipc(
        table.replace_schema_metadata(
            {owner.cf_read_rules()["metadata_key"].encode(): canonical_bytes(envelope)}
        )
    )
    with pytest.raises(SchemaValidationError, match="fixed family role layout"):
        owner.NativeCfRead.from_ipc(wire)


def test_cache_poisoned_finite_metric_cannot_override_fresh_source(cf_reader):
    reader, request, state = cf_reader
    cache = RecordingCache()
    cold = reader.read(request, cache=cache)
    assert cold.table["true_range"].to_pylist() == [None, 0.0]
    assert reader.read(request, cache=cache).table.equals(cold.table)
    poisoned = cold.table.set_column(
        cold.table.column_names.index("true_range"),
        cold.table.schema.field("true_range"),
        pa.array([None, 1.0], type=pa.float64()),
    )
    wire = owner.make_cf_read(poisoned, cold.envelope).to_ipc()
    assert owner.NativeCfRead.from_ipc(wire).table["true_range"].to_pylist() == [None, 1.0]
    cache.put(*cache.slot, wire)
    before = state["verifications"]
    with pytest.raises(SchemaValidationError, match="freshly verified role"):
        reader.read(request, cache=cache)
    assert state["verifications"] == before + 1


def own_sidecar_read(cf_reader, layer, role, metric):
    """Synthetic wire structure only; no source, formula or ancestor-auth proof."""
    _, _, state = cf_reader
    _, shapes = CONTRACT_EXAMPLE(layer)
    source = copy.deepcopy(shapes["compute"])
    context = source["context_payload"]
    context["used_references"] = copy.deepcopy(
        state["source"]["context_payload"]["used_references"]
    )
    numerical = context["numerical_identity"]
    numerical["digest"] = owner.sha({k: v for k, v in numerical.items() if k != "digest"})
    context["cf_context_id"] = "CFCTX:" + owner.sha(
        {k: v for k, v in context.items() if k != "cf_context_id"}
    )
    request = copy.deepcopy(shapes["read_request"])
    request.update(role=role, projection=["close", metric])
    schema = owner.physical_schema(context["layouts"][role])
    rows = []
    for parent in state["tables"]["bars"].to_pylist():
        if parent["cf_definition_json"] != canonical_bytes(request["definition"]).decode():
            continue
        row = blank(schema)
        row.update(parent)
        if role == "derived":
            row.update(
                indicators_context_id="CFCTX:" + "1" * 64,
                indicators_profile_version="native_cf_prefix_l1.v1",
            )
        row.update(
            {
                role + "_context_id": context["cf_context_id"],
                role + "_profile_version": context["policy"]["profile_version"],
                metric: 1.25,
            }
        )
        rows.append(row)
    assert len(rows) == 2
    full = pa.Table.from_pylist(rows, schema=schema)
    for item in source["tables"]:
        item["reference"]["row_count"] = 2
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
    return owner.make_cf_read(
        full.select(owner.cf_read_columns(request, context["layouts"][role])), envelope
    )


def rehashed_sidecar_wire(table, envelope):
    """Rebind every changed declared hash so negatives reach the family guard."""
    envelope = copy.deepcopy(envelope)
    context = envelope["source"]["context_payload"]
    policy = context["policy_payload"]
    policy["catalog_sha256"] = owner.sha(policy["catalog"])
    context["policy_sha256"] = owner.sha(policy)
    numerical = context["numerical_identity"]
    numerical["policy_sha256"] = context["policy_sha256"]
    numerical["digest"] = owner.sha({k: v for k, v in numerical.items() if k != "digest"})
    context["cf_context_id"] = "CFCTX:" + owner.sha(
        {k: v for k, v in context.items() if k != "cf_context_id"}
    )
    envelope["source"]["context"]["sha256"] = owner.sha(context)
    role = envelope["request"]["role"]
    item = next(row for row in envelope["source"]["tables"] if row["role"] == role)
    item["layout"] = copy.deepcopy(context["layouts"][role])
    item["reference"]["schema_hash"] = owner.sha(
        json.loads(Schema.from_pyarrow(owner.physical_schema(item["layout"])).to_json())
    )
    item["reference"]["layout_version"] = owner.sha({"partition_columns": []})
    name = role + "_context_id"
    table = table.set_column(
        table.column_names.index(name),
        table.schema.field(name),
        pa.array([context["cf_context_id"]] * table.num_rows),
    )
    envelope["payload"] = {
        "row_count": table.num_rows,
        "columns": table.column_names,
        "schema_ipc_base64": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
        "ipc_sha256": hashlib.sha256(owner._ipc(table)).hexdigest(),
    }
    owner.validate_identity_contract(
        "native_cf_read.v1.json", envelope, envelope["source"]["namespace"]
    )
    return owner._ipc(
        table.replace_schema_metadata(
            {owner.cf_read_rules()["metadata_key"].encode(): canonical_bytes(envelope)}
        )
    )


@pytest.mark.parametrize(
    "layer,role,metric",
    [("CF-L1", "indicators", "sma_20"), ("CF-L2", "derived", "rolling_high_20")],
)
@pytest.mark.parametrize("tamper", ["layout_and_catalog", "catalog_only"])
def test_own_role_fixed_layout_and_catalog_reject_rehashed_forgery(
    cf_reader, layer, role, metric, tamper
):
    valid = own_sidecar_read(cf_reader, layer, role, metric)
    assert owner.NativeCfRead.from_ipc(valid.to_ipc()).table[metric].to_pylist() == [1.25, 1.25]
    envelope, table = valid.envelope, valid.table
    context = envelope["source"]["context_payload"]
    entry = next(row for row in context["policy_payload"]["catalog"] if row["output"] == metric)
    if tamper == "layout_and_catalog":
        entry.update(
            materialized=False,
            admission_minimum=None,
            invocation_owner="not_admitted_no_formula_call",
            invocation_domain="not_admitted",
            mask_domain="not_admitted",
            quantity_domain="not_admitted",
        )
        for name in ["observed_call_safe", "configured_group_minimum"]:
            if name in entry:
                entry[name] = None
        for layouts in [context["layouts"], context["policy_payload"]["layouts"]]:
            layouts[role]["fields"] = [f for f in layouts[role]["fields"] if f["name"] != metric]
        envelope["request"]["projection"].remove(metric)
        table = table.drop([metric])
    else:
        entry["admission_minimum"] += 1
    wire = rehashed_sidecar_wire(table, envelope)
    message = (
        "fixed family role layout" if tamper == "layout_and_catalog" else "fixed family catalog"
    )
    with pytest.raises(SchemaValidationError, match=message):
        owner.NativeCfRead.from_ipc(wire)


@pytest.mark.parametrize(
    "layer,role,metric",
    [("CF-L1", "indicators", "sma_20"), ("CF-L2", "derived", "rolling_high_20")],
)
def test_own_role_wire_preserves_recorded_numerical_runtime_descriptors(
    cf_reader, layer, role, metric
):
    valid = own_sidecar_read(cf_reader, layer, role, metric)
    envelope = valid.envelope
    context = envelope["source"]["context_payload"]
    context["numerical_identity"]["kernels"]["adapter"] = "f" * 64
    context["runtime_code_sha256"] = "e" * 64
    result = owner.NativeCfRead.from_ipc(rehashed_sidecar_wire(valid.table, envelope))
    stored = result.envelope["source"]["context_payload"]
    assert stored["numerical_identity"]["kernels"]["adapter"] == "f" * 64
    assert stored["runtime_code_sha256"] == "e" * 64
    assert result.table[metric].to_pylist() == [1.25, 1.25]


@pytest.mark.parametrize(
    "layer,role,metric,minimum",
    [("CF-L1", "indicators", "sma_20", 20), ("CF-L2", "derived", "rolling_high_20", 300)],
)
@pytest.mark.parametrize("mutation", ["minimum", "materialized"])
def test_own_role_capability_cannot_contradict_fixed_catalog(
    cf_reader, layer, role, metric, minimum, mutation
):
    valid = own_sidecar_read(cf_reader, layer, role, metric)
    envelope = valid.envelope
    context = envelope["source"]["context_payload"]
    row = {
        "sequence": {
            "definition": envelope["request"]["definition"],
            "timeframe": envelope["request"]["timeframe"],
        },
        "feature": metric,
        "materialized": True,
        "analysis_rows": 2,
        "state": "ready",
        "reason": "structural metadata fixture; not compute proof",
        "required_observations": minimum,
        "finite_count": 2,
        "analysis_finite_count": 2,
        "first_finite_bar_index": 0,
        "first_finite_source_begin": valid.table["source_begin"][0].as_py(),
    }
    context["capabilities"] = [row]
    positive = owner.NativeCfRead.from_ipc(rehashed_sidecar_wire(valid.table, envelope))
    assert positive.table[metric].to_pylist() == [1.25, 1.25]
    if mutation == "minimum":
        row["required_observations"] += 1
    else:
        row = {
            **{key: row[key] for key in ["sequence", "feature", "analysis_rows", "reason"]},
            "materialized": False,
            "state": "unverified_policy",
        }
    context["capabilities"] = [row]
    wire = rehashed_sidecar_wire(valid.table, envelope)
    with pytest.raises(SchemaValidationError, match="feature capability contradicts fixed policy"):
        owner.NativeCfRead.from_ipc(wire)
