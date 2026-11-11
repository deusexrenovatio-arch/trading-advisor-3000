"""Literal source/prefix oracles and shared-protocol faults; real proof is separate."""

import copy
import json
import math
import runpy
from contextlib import nullcontext
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.common.immutable_files import canonical_bytes, pin
from trading_advisor_3000.product_plane.research import native_cf as cf
from trading_advisor_3000.product_plane.research import native_feature_materialization as writer
from trading_advisor_3000.product_plane.research.native_cf_policy import cf_layouts
from trading_advisor_3000.product_plane.research.native_observations import sha
from trading_advisor_3000.spark_jobs import continuous_front_job as calendar

SHAPES = runpy.run_path(str(Path(__file__).parents[1] / "contracts/test_native_cf_contracts.py"))


def inputs():
    seed = SHAPES["seed"]()
    definitions = [SHAPES["definition"](offset) for offset in [2, 1]]
    schedule = seed["calendar_roll_schedule"]
    group = copy.deepcopy(schedule["groups"][0])
    schedule["groups"] = []
    for definition in definitions:
        current = copy.deepcopy(group)
        current["group_key"] = cf.definition_json(definition)
        current["roll_offset_trading_days"] = definition["roll"]["offset_sessions"]
        schedule["groups"].append(current)
    rows = []
    for index, day in enumerate(["14", "15"]):
        for component, close in zip(seed["components"], [100 + index, 110 + index], strict=True):
            row = {
                "namespace": seed["namespace"]["namespace"],
                "asset_id": "ASSET:SBER",
                "instrument_id": component["instrument"]["instrument_id"],
                "provider": "moex_iss",
                "venue": "MOEX",
                "engine": "futures",
                "market": "forts",
                "board": "RFUD",
                "source_symbol": component["source_symbol"],
                "timeframe": "1d",
                "source_begin": f"2026-09-{day} 00:00:00",
                "source_end": f"2026-09-{day} 23:59:59",
                "open": Decimal(close - 1),
                "high": Decimal(close + 2),
                "low": Decimal(close - 2),
                "close": Decimal(close),
                "volume": Decimal(10),
                "open_interest": None,
                "volume_state": "known",
                "volume_null_reason": None,
                "open_interest_state": "unknown",
                "open_interest_null_reason": "source absent",
                "source_units_json": canonical_bytes(component["units"]).decode(),
                "normalization_recipe": "native",
                "value_origin": "observed",
                "simulation_id": None,
                "overlay_sha256": None,
                "body_sha256": "b" * 64,
                "receipt_sha256": "c" * 64,
                "source_block": "candles",
                "page_start": 0,
                "row_ordinal": index,
                "slice_role": "warmup" if index == 0 else "analysis",
            }
            rows.append(row)
    fields = copy.deepcopy(cf_layouts("CF")["bars"]["fields"])
    fields = fields[: next(i for i, f in enumerate(fields) if f["name"] == "cf_definition_json")]
    for field in fields:
        if field["name"] == "source_instrument_id":
            field["name"] = "instrument_id"
    fields.append({"name": "slice_role", "sql_type": "string", "nullable": False})
    source = pa.Table.from_pylist(rows, schema=cf.physical_schema({"fields": fields}))
    return {"tables": {"research_bar_views": source}}, {
        "definitions": definitions,
        "calendar_roll_schedule": schedule,
    }


def test_literal_two_definitions_selected_buffers_gap_and_composed_inputs():
    parent, intent = inputs()
    tables, metadata = cf.expected_cf_base(parent, intent, cf_layouts("CF"), "CFCTX:" + "a" * 64)
    primary = tables["bars"].filter(
        pa.compute.equal(
            tables["bars"]["cf_definition_json"], cf.definition_json(intent["definitions"][0])
        )
    )
    secondary = tables["bars"].filter(
        pa.compute.equal(
            tables["bars"]["cf_definition_json"], cf.definition_json(intent["definitions"][1])
        )
    )
    assert primary["source_symbol"].to_pylist() == ["SRU6", "SRZ6"]
    assert secondary["source_symbol"].to_pylist() == ["SRU6", "SRU6"]
    assert primary["close"].to_pylist() == [Decimal(100), Decimal(111)]
    assert primary["true_range"].to_pylist() == [4.0, 4.0]
    assert primary["log_ret_1"].to_pylist() == [None, 0.0]
    assert secondary["log_ret_1"].to_pylist() == [None, pytest.approx(math.log(1.01))]
    assert tables["events"]["additive_gap"].to_pylist() == [11.0]
    assert tables["ladder"].equals(tables["events"])
    assert metadata["selection"] == {
        "parent_inspected_rows": 8,
        "eligible_component_rows": 8,
        "selected_rows": 4,
        "analysis_rows": 2,
        "observed_events": 1,
        "ladder_rows": 1,
        "qc_rows": 2,
    }
    assert primary["open_interest"].null_count == 2
    assert primary["instrument_id"].null_count == 2
    assert len(metadata["used_references"]) == 2


def test_zero_event_roles_remain_typed_required_tables():
    parent, intent = inputs()
    intent["definitions"] = intent["definitions"][1:]
    intent["calendar_roll_schedule"]["groups"] = intent["calendar_roll_schedule"]["groups"][1:]
    layouts = cf_layouts("CF")
    tables, metadata = cf.expected_cf_base(parent, intent, layouts, "CFCTX:" + "a" * 64)
    assert tables["events"].num_rows == tables["ladder"].num_rows == 0
    assert tables["events"].schema.names == [f["name"] for f in layouts["events"]["fields"]]
    assert pa.types.is_decimal(tables["events"].schema.field("old_reference_close").type)
    assert metadata["selection"]["qc_rows"] == 1


def test_future_unused_gap_and_quantity_do_not_backfill_prefix():
    frame = pd.DataFrame(
        {
            "cf_definition_json": ["def"] * 3,
            "timeframe": ["1m"] * 3,
            "source_begin": ["2026-09-14 10:00:00", "2026-09-14 10:01:00", "2026-09-15 10:00:00"],
            "open": [99.0, 100.0, 199.0],
            "high": [102.0, 103.0, 202.0],
            "low": [98.0, 99.0, 198.0],
            "close": [100.0, 101.0, 200.0],
            "volume": [10.0, 11.0, None],
            "roll_epoch": [0, 0, 1],
            "is_first_bar_after_roll": [False, False, True],
            "_gap": [0.0, 0.0, 99.0],
        }
    )
    prefix = cf._cf_input_metrics(frame.iloc[:2])
    frame.loc[2, "_gap"] = 197.0
    frame.loc[2, "volume"] = 999.0
    actual = cf._cf_input_metrics(frame)
    pd.testing.assert_frame_equal(prefix, actual.iloc[:2].reset_index(drop=True))
    assert actual.loc[1, "log_ret_1"] == pytest.approx(math.log(1.01))


def test_changed_used_reference_changes_identity_and_can_change_later_values():
    parent, intent = inputs()
    before_tables, before = cf.expected_cf_base(
        parent, intent, cf_layouts("CF"), "CFCTX:" + "a" * 64
    )
    rows = parent["tables"]["research_bar_views"].to_pylist()
    rows[0]["body_sha256"] = "d" * 64
    rows[0]["close"] = Decimal(99)
    parent["tables"]["research_bar_views"] = pa.Table.from_pylist(rows)
    after_tables, after = cf.expected_cf_base(parent, intent, cf_layouts("CF"), "CFCTX:" + "a" * 64)
    assert before["used_references"] != after["used_references"]
    assert sha(before) != sha(after)
    assert before_tables["events"]["additive_gap"].to_pylist() == [11.0]
    assert after_tables["events"]["additive_gap"].to_pylist() == [12.0]


@pytest.mark.parametrize(
    "change",
    ["missing_date", "duplicate_order", "reverse_last_trade", "missing_rule", "short_coverage"],
)
def test_calendar_semantic_coverage_and_order_reject(change):
    _, intent = inputs()
    schedule = intent["calendar_roll_schedule"]
    group = schedule["groups"][0]
    if change == "missing_date":
        group["planned_dates"].pop(1)
    if change == "duplicate_order":
        group["components"][1]["source_order"] = 0
    if change == "reverse_last_trade":
        group["components"][1]["last_trade_date"] = "2026-09-16"
    if change == "missing_rule":
        group["planned_dates"][0]["governing_rule_ids"] = ["missing"]
    if change == "short_coverage":
        group["components"][0]["last_trade_date"] = "2026-09-14"
    with pytest.raises(ValueError):
        calendar.calendar_active_intervals(schedule)


def test_missing_calendar_facts_rejects_before_spark(tmp_path):
    calls = []
    with pytest.raises(ValueError, match="calendar_roll_schedule is required"):
        calendar.run_continuous_front_spark_job.__wrapped__(
            canonical_bars_path=tmp_path / "bars",
            canonical_session_calendar_path=tmp_path / "sessions",
            canonical_roll_map_path=tmp_path / "rolls",
            output_dir=tmp_path,
            dataset_version="x",
            spark_session_factory=lambda *_: calls.append("spark"),
        )
    assert not calls


@pytest.mark.parametrize("value", [[], None, "scalar", 7, True])
def test_public_cf_reader_non_object_receipt_is_unavailable(tmp_path, value):
    from trading_advisor_3000.product_plane.common.data_snapshots import SnapshotUnavailableError

    path = tmp_path / "receipt.json"
    path.write_bytes(canonical_bytes(value))
    reference = pin(tmp_path, path)
    with pytest.raises(SnapshotUnavailableError, match="CF completion must be an object"):
        cf.NativeCfSnapshot(tmp_path, reference)


@pytest.fixture
def protocol_root(tmp_path_factory):
    # Short native-writer paths also fit Windows' path limit in host tests.
    return tmp_path_factory.mktemp("cf")


def protocol(monkeypatch, tmp_path, *, family="price"):
    """Fake frames isolate control flow; no claim of physical/native admission."""
    realpath = Path
    monkeypatch.setattr(
        writer,
        "Path",
        lambda value: tmp_path if str(value) == "/research-trial" else realpath(value),
    )
    (tmp_path / "scope.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(writer, "command_lock", lambda _: nullcontext())
    monkeypatch.setattr(writer, "validate_identity_contract", lambda *args: None)
    parent = {
        "snapshot_id": "parent",
        "tables": {"parent": {"pin": "unchanged"}},
        "required_tables": ["parent"],
    }
    source = {"namespace": SHAPES["seed"]()["namespace"], "snapshot": {"snapshot_id": "parent"}}
    reader = SimpleNamespace(compute_metadata=lambda: source)
    monkeypatch.setattr(writer, "parent_reader", lambda *args: reader)
    intent = {
        "schema_version": 1,
        "source_root": "/research-trial",
        "parent_completion": {"path": "parent.json", "sha256": "a" * 64},
        "layer": "L1",
        "command_id": "same-command",
        "code_sha256": "b" * 64,
    }
    trace = []
    if family == "cf":
        intent.update(
            contract_version="native_cf_intent.v1",
            input_mode="native_cf_observation.v1",
            layer="CF",
            definitions=[],
            calendar_roll_schedule={},
            source_fact_pins=[],
        )
        monkeypatch.setattr(cf, "_parent", lambda *args: (reader, {}))
        monkeypatch.setattr(cf, "build_cf_command", lambda *args, **kwargs: copy.deepcopy(intent))
        monkeypatch.setattr(cf, "validate_cf_facts", lambda *args: trace.append("facts"))
        monkeypatch.setattr(
            cf,
            "build_cf_frames",
            lambda *args: (
                {r: object() for r in ["bars", "events", "ladder", "qc"]},
                {r: {} for r in ["bars", "events", "ladder", "qc"]},
                {"policy_payload": {"version": "CF"}},
                parent,
            ),
        )
    else:
        monkeypatch.setattr(
            writer, "build_feature_command", lambda *args, **kwargs: copy.deepcopy(intent)
        )
        monkeypatch.setattr(
            writer,
            "_build_feature_frames",
            lambda *args: ({"features": object()}, {"features": {}}, {}, parent),
        )
    monkeypatch.setattr(writer, "producer_code_sha256", lambda: intent["code_sha256"])

    def table(_spark, path, frame, layout):
        trace.append("table:" + path.stem)
        path.mkdir()
        return {"table_path": str(path)}

    monkeypatch.setattr(writer, "write_delta_once", table)
    monkeypatch.setattr(
        writer, "create_snapshot", lambda **kwargs: {**kwargs, "snapshot_id": "snapshot"}
    )

    def save(attempt, manifest):
        trace.append("snapshot")
        p = attempt / "manifests" / "snapshot.json"
        p.parent.mkdir()
        p.write_bytes(canonical_bytes(manifest))
        return p

    monkeypatch.setattr(writer, "save_snapshot", save)

    def graph(_root, **kwargs):
        manifest = json.loads((tmp_path / kwargs["snapshot_pin"]["path"]).read_bytes())
        return {"manifest": manifest}

    monkeypatch.setattr(writer, "validate_feature_graph", graph)
    monkeypatch.setattr(cf, "validate_cf_graph", graph)

    class Completed:
        def __init__(self, root, reference):
            self.reference = reference

        def verify(self):
            return {"completion": json.loads((tmp_path / self.reference["path"]).read_bytes())}

    monkeypatch.setattr(writer, "NativeFeatureSnapshot", Completed)
    monkeypatch.setattr(cf, "NativeCfSnapshot", Completed)
    return intent, trace


@pytest.mark.parametrize("family", ["price", "cf"])
def test_shared_protocol_lost_ack_replays_original_receipt(monkeypatch, protocol_root, family):
    intent, trace = protocol(monkeypatch, protocol_root, family=family)
    with pytest.raises(RuntimeError, match="lost acknowledgement"):
        writer.materialize_feature_command(
            protocol_root, intent, spark=object(), fault="after_completion"
        )
    first = [p for p in protocol_root.glob("commands/*/completion.json")][0].read_bytes()
    before = list(trace)
    monkeypatch.setattr(
        writer,
        "build_feature_command",
        lambda *a, **k: pytest.fail("replay recomputed current factory"),
    )
    monkeypatch.setattr(
        cf, "build_cf_command", lambda *a, **k: pytest.fail("replay recomputed current factory")
    )
    result = writer.materialize_feature_command(protocol_root, intent, spark=object())
    assert result["replayed"] and trace == before
    assert (protocol_root / result["completion"]["path"]).read_bytes() == first
    changed = {**intent, "code_sha256": "c" * 64}
    with pytest.raises(ValueError, match="conflicting canonical input"):
        writer.materialize_feature_command(protocol_root, changed, spark=object())
    assert trace == before


def test_cf_named_partial_write_has_no_manifest_then_fresh_attempt(monkeypatch, protocol_root):
    intent, trace = protocol(monkeypatch, protocol_root, family="cf")
    with pytest.raises(RuntimeError, match="after table"):
        writer.materialize_feature_command(
            protocol_root, intent, spark=object(), fault="after_table:events"
        )
    assert not list(protocol_root.glob("commands/*/attempts/*/manifests/*.json"))
    assert not list(protocol_root.glob("commands/*/completion.json"))
    result = writer.materialize_feature_command(protocol_root, intent, spark=object())
    assert not result["recovered"] and not result["replayed"]
    assert trace[-1] == "snapshot"
    assert sum(t == "table:continuous_front_bars" for t in trace) == 2


def test_shared_protocol_saved_one_recovers_two_rejects(monkeypatch, protocol_root):
    intent, trace = protocol(monkeypatch, protocol_root)
    with pytest.raises(RuntimeError, match="saved graph"):
        writer.materialize_feature_command(
            protocol_root, intent, spark=object(), fault="after_snapshot"
        )
    before = list(trace)
    result = writer.materialize_feature_command(protocol_root, intent, spark=object())
    assert result["recovered"] and trace == before
    (protocol_root / result["completion"]["path"]).unlink()
    saved = next(protocol_root.glob("commands/*/attempts/*/manifests/*.json"))
    another = saved.parents[2] / "another" / "manifests" / "snapshot.json"
    another.parent.mkdir(parents=True)
    another.write_bytes(saved.read_bytes())
    (another.parent.parent / "context.json").write_bytes(
        (saved.parent.parent / "context.json").read_bytes()
    )
    with pytest.raises(ValueError, match="ambiguous saved complete"):
        writer.materialize_feature_command(protocol_root, intent, spark=object())
    assert trace == before


def test_corrupt_present_saved_graph_is_not_ignored(monkeypatch, protocol_root):
    intent, trace = protocol(monkeypatch, protocol_root)
    with pytest.raises(RuntimeError, match="saved graph"):
        writer.materialize_feature_command(
            protocol_root, intent, spark=object(), fault="after_snapshot"
        )
    before = list(trace)

    def corrupt(*args, **kwargs):
        raise ValueError("corrupt saved graph")

    monkeypatch.setattr(writer, "validate_feature_graph", corrupt)
    with pytest.raises(ValueError, match="corrupt saved graph"):
        writer.materialize_feature_command(protocol_root, intent, spark=object())
    assert trace == before


@pytest.mark.parametrize("header", ["receipt", "context"])
@pytest.mark.parametrize("bad", [[], None])
def test_cf_parent_header_rejects_before_recursion_with_correct_pins(
    tmp_path, monkeypatch, header, bad
):
    def store(name, value):
        path = tmp_path / name
        path.write_bytes(canonical_bytes(value))
        return pin(tmp_path, path)

    context = {"layer": "L0", "contract_version": "native_research_l0_context.v1"}
    context_pin = store("context.json", context)
    receipt = {
        "layer": "L0",
        "contract_version": "native_research_l0_completion.v1",
        "context": context_pin,
    }
    receipt_pin = store("parent.json", receipt)

    def entered(*args, **kwargs):
        raise AssertionError("entered valid parent recursion")

    monkeypatch.setattr(cf.NativeResearchSnapshot, "_verified", entered)
    with pytest.raises(AssertionError, match="entered valid parent recursion"):
        cf._parent(tmp_path, receipt_pin, "CF")
    if header == "receipt":
        receipt_pin = store("parent.json", bad)
    else:
        receipt["context"] = store("context.json", bad)
        receipt_pin = store("parent.json", receipt)
    with pytest.raises(ValueError, match="CF parent " + header + " must be an object"):
        cf._parent(tmp_path, receipt_pin, "CF")


def test_cf_private_graph_cannot_cross_root_or_completion(tmp_path):
    reader = cf.NativeCfSnapshot.__new__(cf.NativeCfSnapshot)
    reader.root = tmp_path.resolve()
    reader._completion_json = canonical_bytes({"path": "receipt.json", "sha256": "a" * 64}).decode()
    graph = {
        "root": str(tmp_path / "foreign"),
        "completion_pin": json.loads(reader._completion_json),
    }
    with pytest.raises(ValueError, match="foreign CF graph"):
        reader._read_role_verified(graph, "bars")
    graph["root"] = reader.root.as_posix()
    graph["completion_pin"] = {"path": "other.json", "sha256": "a" * 64}
    with pytest.raises(ValueError, match="foreign CF graph"):
        reader._compute_metadata(graph)


def test_self_consistent_forged_layout_cannot_drop_native_lineage(monkeypatch, protocol_root):
    """Actual Delta and correct new pins; only external parent admission is stubbed."""
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common.data_snapshots import (
        capture_table,
        create_snapshot,
        save_snapshot,
    )

    parent, basic = inputs()
    _, bundle = SHAPES["example"]("CF")
    namespace = bundle["context"]["source"]["namespace"]
    sourcepath = protocol_root / "source.delta"
    write_deltalake(sourcepath, parent["tables"]["research_bar_views"])
    source_ref = capture_table(sourcepath)
    parent.update(
        completion={
            "layer": "L0",
            "contract_version": "native_research_l0_completion.v1",
            "snapshot": bundle["context"]["source"]["parent_snapshot"],
            "context": bundle["context"]["source"]["parent_context"],
        },
        context={"source": {"namespace": namespace}},
        manifest={
            "tables": {"research_bar_views": source_ref},
            "required_tables": ["research_bar_views"],
        },
    )
    monkeypatch.setattr(cf, "_parent", lambda *args: (None, parent))
    monkeypatch.setattr(cf, "validate_cf_facts", lambda *args: None)

    def saved(label, drop):
        intent = copy.deepcopy(bundle["intent"])
        intent.update(basic)
        context = copy.deepcopy(bundle["context"])
        context.update(
            definitions=intent["definitions"],
            calendar_roll_schedule=intent["calendar_roll_schedule"],
            source=cf._source(intent["parent_completion"], parent),
        )
        layouts = context["layouts"]
        if drop:
            layouts["bars"]["fields"] = [
                f for f in layouts["bars"]["fields"] if f["name"] != "body_sha256"
            ]
        context["policy_payload"]["layouts"] = copy.deepcopy(layouts)
        context["policy_sha256"] = intent["policy_sha256"] = sha(context["policy_payload"])
        numerical = context["numerical_identity"]
        numerical["policy_sha256"] = context["policy_sha256"]
        numerical["digest"] = sha({k: v for k, v in numerical.items() if k != "digest"})
        intent["numerical_identity"] = copy.deepcopy(numerical)
        _, counts = cf.expected_cf_base(parent, intent, layouts, "")
        context.update(counts)
        context["cf_context_id"] = "CFCTX:" + sha(
            {k: v for k, v in context.items() if k != "cf_context_id"}
        )
        tables, _ = cf.expected_cf_base(parent, intent, layouts, context["cf_context_id"])
        attempt = protocol_root / label
        attempt.mkdir()
        refs = {"research_bar_views": source_ref}
        for role, table in tables.items():
            path = attempt / (role + ".delta")
            write_deltalake(path, table)
            refs[cf.TABLES[role]] = capture_table(path)

        def store(name, value):
            path = attempt / name
            path.write_bytes(canonical_bytes(value))
            return pin(protocol_root, path)

        cp = store("context.json", context)
        ip = store("intent.json", intent)
        params = {
            "input_sha256": sha(intent),
            "context": cp,
            "intent": ip,
            "source_root": "/research-trial",
            "parent_completion": intent["parent_completion"],
        }
        manifest = create_snapshot(
            tables=refs,
            required_tables=list(refs),
            dataset_version=context["policy_payload"]["version"],
            run_id=label,
            code_version=intent["code_sha256"],
            quality_checks={k: "PASS" for k in ["parent", "schema", "grain", "capabilities"]},
            parameters=params,
            parents=[context["source"]["parent_snapshot"]["snapshot_id"]],
        )
        sp = pin(protocol_root, save_snapshot(attempt, manifest))
        return dict(snapshot_pin=sp, context_pin=cp, intent_pin=ip, input_sha256=sha(intent))

    valid = saved("valid", False)
    graph = cf.validate_cf_graph(protocol_root, **valid)
    assert "body_sha256" in graph["tables"][cf.TABLES["bars"]].column_names
    forged = saved("forged", True)
    with pytest.raises(ValueError, match="CF fixed family role layout mismatch"):
        cf.validate_cf_graph(protocol_root, **forged)


@pytest.mark.parametrize("change", ["family", "profile"])
def test_cf_parent_family_and_profile_checked_before_recursion(tmp_path, monkeypatch, change):
    def store(name, value):
        path = tmp_path / name
        path.write_bytes(canonical_bytes(value))
        return pin(tmp_path, path)

    context = {
        "layer": "CF",
        "contract_version": "native_cf_context.v1",
        "policy": {"profile_version": "native_cf_composition.v1"},
    }
    receipt = {
        "layer": "CF",
        "contract_version": "native_cf_completion.v1",
        "context": store("context.json", context),
    }
    reference = store("receipt.json", receipt)

    def entered(*args, **kwargs):
        raise AssertionError("entered valid parent recursion")

    monkeypatch.setattr(cf.NativeCfSnapshot, "_verified", entered)
    with pytest.raises(AssertionError, match="entered valid parent recursion"):
        cf._parent(tmp_path, reference, "CF-L1")
    if change == "family":
        context["contract_version"] = "native_research_feature_context.v1"
    else:
        context["policy"]["profile_version"] = "native_core_l1.v1"
    receipt["context"] = store("context.json", context)
    reference = store("receipt.json", receipt)
    with pytest.raises(ValueError, match="CF parent (context family|profile) mismatch"):
        cf._parent(tmp_path, reference, "CF-L1")


def test_original_expiration_field_is_separate_from_last_trade(tmp_path):
    body = {
        "description": {
            "columns": ["name", "value"],
            "data": [["SECID", "SRU6"], ["LSTTRADE", "2026-09-17"], ["LSTDELDATE", "2026-09-18"]],
        }
    }
    body_path = tmp_path / "body.json"
    body_path.write_bytes(canonical_bytes(body))
    body_pin = pin(tmp_path, body_path)
    receipt = {
        "status": 200,
        "body": body_pin,
        "body_bytes": body_path.stat().st_size,
        "final_url": "https://iss.moex.com/iss/securities/SRU6.json",
    }
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_bytes(canonical_bytes(receipt))
    component = {
        "source_symbol": "SRU6",
        "last_trade_date": "2026-09-17",
        "expiration_date": "2026-09-18",
        "last_trade_source": {
            "kind": "moex_iss_description",
            "body": body_pin,
            "receipt": pin(tmp_path, receipt_path),
            "block": "description",
        },
    }
    cf._component_expiration(tmp_path, component)
    component["expiration_date"] = "2026-09-17"
    with pytest.raises(ValueError, match="original delivery field"):
        cf._component_expiration(tmp_path, component)


def source_admission_inputs(tmp_path, monkeypatch):
    """Synthetic honest source pins; only reviewed calendar/packet interpretation stubbed.

    All symbol/control/parent associations, source pairs, values, lineage and
    source-binding JSON hashes run through the real validate_cf_facts owner.
    Authentic retained-fact characterization is separate OUT-only evidence.
    """
    from trading_advisor_3000.product_plane.common.immutable_files import pinned_bytes

    parent, basic = inputs()
    namespace, bundle = SHAPES["example"]("CF")
    refs = {}

    def store(relative, value):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_bytes(value))
        reference = pin(tmp_path, path)
        refs[relative] = reference
        return reference

    def pair(relative, payload, url):
        body = store(relative + ".json", payload)
        receipt = store(
            relative + ".receipt.json",
            {
                "status": 200,
                "body": body,
                "body_bytes": len(canonical_bytes(payload)),
                "final_url": url,
            },
        )
        return body, receipt

    spec = store("f/spec.json", {"quote": "synthetic RUB per contract lot"})
    md = store("f/md.json", {"source": "synthetic MD parent"})
    assignments = store("f/assignments.json", {"namespace": namespace["namespace"]})
    manifest = store("f/source.json", {"source": "synthetic source manifest"})
    l0 = store("f/l0.json", {"parent": md})
    packet_pin = {
        "path": "source-facts/temporal/facts.json",
        "sha256": "c018f490796c58c3816152d61db40cdab8d20f09e86c61ce896d2107b090219d",
    }
    packet = {"direct_facts": {"dated_sbrf_specification": {"sha256": spec["sha256"]}}}
    store(packet_pin["path"], packet)
    refs[packet_pin["path"]] = packet_pin

    def fixture_packet(root, reference):
        if reference == packet_pin:
            assert root == tmp_path
            return canonical_bytes(packet)
        return pinned_bytes(root, reference)

    monkeypatch.setattr(cf, "pinned_bytes", fixture_packet)
    monkeypatch.setattr(
        cf,
        "validate_calendar_sources",
        lambda root, schedule: calendar.calendar_active_intervals(schedule),
    )
    meta = {}
    for component in basic["definitions"][0]["components"]:
        symbol = component["source_symbol"]
        body, receipt = pair(
            "f/" + symbol,
            {
                "description": {
                    "columns": ["name", "value"],
                    "data": [
                        ["SECID", symbol],
                        ["LSTTRADE", component["last_trade_date"]],
                        ["LSTDELDATE", component["expiration_date"]],
                    ],
                }
            },
            "https://iss.moex.com/iss/securities/" + symbol + ".json",
        )
        meta[symbol] = {**component["last_trade_source"], "body": body, "receipt": receipt}
    rows = parent["tables"]["research_bar_views"].to_pylist()
    controls = []
    for symbol in ["SRU6", "SRZ6"]:
        own = [r for r in rows if r["source_symbol"] == symbol]
        columns = ["begin", "end", "open", "high", "low", "close", "volume"]
        data = [
            [r["source_begin"], r["source_end"], *[int(r[n]) for n in columns[2:]]] for r in own
        ]
        body, receipt = pair(
            "f/" + symbol + "_candles",
            {"candles": {"columns": columns, "data": data}},
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/"
            + symbol
            + "/candles.json",
        )
        hcolumns = ["SECID", "TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
        hdata = [
            [symbol, r["source_begin"][:10], *[int(r[n.lower()]) for n in hcolumns[2:]]]
            for r in own
        ]
        pair(
            "f/iss_" + symbol.lower() + "_history_sep14_15",
            {"history": {"columns": hcolumns, "data": hdata}},
            "https://iss.moex.com/iss/history/engines/futures/markets/forts/boards/RFUD/securities/"
            + symbol
            + ".json",
        )
        units = copy.deepcopy(
            next(
                c["units"]
                for c in basic["definitions"][0]["components"]
                if c["source_symbol"] == symbol
            )
        )
        units["price_unit"]["value"] = "RUB" if symbol == "SRU6" else "В RUB за лот"
        for r in own:
            r.update(
                body_sha256=body["sha256"],
                receipt_sha256=receipt["sha256"],
                source_units_json=canonical_bytes(units).decode(),
            )
            controls.append(
                {
                    "source_symbol": symbol,
                    "instrument_id": r["instrument_id"],
                    "source_begin": r["source_begin"],
                    "source_end": r["source_end"],
                    "original_candle_body": body,
                    "original_candle_receipt": receipt,
                    "OHLCV_exact_native_source_values": {n: str(r[n]) for n in columns[2:]},
                }
            )
    parent["tables"]["research_bar_views"] = pa.Table.from_pylist(
        rows, schema=parent["tables"]["research_bar_views"].schema
    )
    parent.update(context={"source": {"namespace": namespace}}, intent={"source_completion": md})
    admission = {
        "schema": "sbrf_numeric_quote_original_MD_binding.v1",
        "source_MD_completion": md,
        "source_manifest": manifest,
        "assignments": assignments,
        "namespace": namespace["namespace"],
        "dated_specification": spec,
        "four_daily_original_byte_comparisons": controls,
    }
    binding = store("f/binding.json", admission)
    rule_body, rule_receipt = pair(
        "f/calendar",
        {"interpretation": "synthetic complete planned14..17"},
        "https://www.moex.com/synthetic",
    )
    schedule = basic["calendar_roll_schedule"]
    schedule["governing_rules"][0].update(body=rule_body, receipt=rule_receipt)
    for definition, group in zip(basic["definitions"], schedule["groups"], strict=True):
        definition["numeric_price_basis"].update(
            parent_l0_completion=l0, source_binding=binding, dated_specification=spec
        )
        for c in definition["components"]:
            c["last_trade_source"] = meta[c["source_symbol"]]
            c["units"] = json.loads(
                next(
                    r["source_units_json"]
                    for r in rows
                    if r["instrument_id"] == c["instrument"]["instrument_id"]
                )
            )
        group["group_key"] = cf.definition_json(definition)
        for g, c in zip(group["components"], definition["components"], strict=True):
            g["last_trade_source"] = c["last_trade_source"]
    intent = {
        **bundle["intent"],
        **basic,
        "parent_completion": l0,
        "source_fact_pins": list(refs.values()),
    }
    return parent, intent, admission, store


def test_symbol_instrument_permutation_rejects_after_genuine_admission_control(
    tmp_path, monkeypatch
):
    from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract

    parent, intent, _, _ = source_admission_inputs(tmp_path, monkeypatch)
    namespace = parent["context"]["source"]["namespace"]
    validate_identity_contract("native_cf_intent.v1.json", intent, namespace)
    cf.validate_cf_facts(tmp_path, intent, parent)
    changed = copy.deepcopy(intent)
    for definition, group in zip(
        changed["definitions"], changed["calendar_roll_schedule"]["groups"], strict=True
    ):
        before = {c["instrument"]["instrument_id"] for c in definition["components"]}
        a, b = definition["components"]
        a["instrument"], b["instrument"] = b["instrument"], a["instrument"]
        a["units"], b["units"] = b["units"], a["units"]
        assert (
            len(before) == 2
            and {c["instrument"]["instrument_id"] for c in definition["components"]} == before
        )
        group["group_key"] = cf.definition_json(definition)
        for g, c in zip(group["components"], definition["components"], strict=True):
            g["component_key"] = c["instrument"]["instrument_id"]
    assert changed["source_fact_pins"] == intent["source_fact_pins"]
    validate_identity_contract("native_cf_intent.v1.json", changed, namespace)
    with pytest.raises(ValueError, match="source symbol.*trusted parent instrument"):
        cf.validate_cf_facts(tmp_path, changed, parent)


def test_daily_controls_need_exact_unique_parent_coverage(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.common.immutable_files import pinned_bytes
    from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract

    parent, intent, admission, store = source_admission_inputs(tmp_path, monkeypatch)
    namespace = parent["context"]["source"]["namespace"]
    validate_identity_contract("native_cf_intent.v1.json", intent, namespace)
    cf.validate_cf_facts(tmp_path, intent, parent)
    changed = copy.deepcopy(intent)
    original = changed["definitions"][0]["numeric_price_basis"]["source_binding"]
    admission["four_daily_original_byte_comparisons"] = [
        copy.deepcopy(admission["four_daily_original_byte_comparisons"][0]) for _ in range(4)
    ]
    replacement = store(original["path"], admission)
    assert (
        replacement["sha256"] != original["sha256"]
        and json.loads(pinned_bytes(tmp_path, replacement)) == admission
    )
    changed["source_fact_pins"] = [
        replacement if p == original else p for p in changed["source_fact_pins"]
    ]
    for definition, group in zip(
        changed["definitions"], changed["calendar_roll_schedule"]["groups"], strict=True
    ):
        definition["numeric_price_basis"]["source_binding"] = replacement
        group["group_key"] = cf.definition_json(definition)
    validate_identity_contract("native_cf_intent.v1.json", changed, namespace)
    with pytest.raises(ValueError, match="daily control coverage"):
        cf.validate_cf_facts(tmp_path, changed, parent)


@pytest.mark.parametrize("family", ["cf", "price", "core"])
def test_shared_writer_uses_real_completion_contract_without_widening_old_shape(
    monkeypatch, protocol_root, family
):
    """Keep actual completion validation; unrelated frames/parents remain protocol doubles."""
    from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract

    intent, _ = protocol(monkeypatch, protocol_root, family=family)
    if family == "core":
        intent.update(
            contract_version="native_research_core_intent.v1",
            profile_version="native_core_l1.v1",
            input_mode="native_observation_research.v1",
        )
    observed = []

    def validate_completion(name, payload, namespace):
        if name.endswith("completion.v1.json"):
            observed.append(name)
            validate_identity_contract(name, payload, namespace)

    monkeypatch.setattr(writer, "validate_identity_contract", validate_completion)
    result = writer.materialize_feature_command(protocol_root, intent, spark=object())
    receipt = json.loads((protocol_root / result["completion"]["path"]).read_bytes())
    old_fields = {
        "schema_version",
        "contract_version",
        "status",
        "layer",
        "command_id",
        "input_sha256",
        "attempt_id",
        "snapshot",
        "context",
        "intent",
        "source_root",
        "parent_completion",
        "created_at_utc",
    }
    expected_contract = {
        "cf": "native_cf_completion.v1",
        "price": "native_research_feature_completion.v1",
        "core": "native_research_core_completion.v1",
    }[family]
    assert observed == [expected_contract + ".json"]
    assert receipt["contract_version"] == expected_contract
    assert set(receipt) == old_fields | ({"input_mode"} if family == "cf" else set())
    if family == "cf":
        assert receipt["input_mode"] == intent["input_mode"] == "native_cf_observation.v1"
    else:
        assert "input_mode" not in receipt


def test_cf_saved_graph_finalization_keeps_recorded_producer_binding(monkeypatch, protocol_root):
    from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract

    intent, trace = protocol(monkeypatch, protocol_root, family="cf")
    build = cf.build_cf_frames

    def recorded_context(*args):
        frames, layouts, context, parent = build(*args)
        context.update(input_mode=intent["input_mode"], runtime_code_sha256=intent["code_sha256"])
        return frames, layouts, context, parent

    monkeypatch.setattr(cf, "build_cf_frames", recorded_context)

    def completion_validator(name, payload, namespace):
        if name.endswith("completion.v1.json"):
            validate_identity_contract(name, payload, namespace)

    monkeypatch.setattr(writer, "validate_identity_contract", completion_validator)
    with pytest.raises(RuntimeError, match="saved graph"):
        writer.materialize_feature_command(
            protocol_root, intent, spark=object(), fault="after_snapshot"
        )
    before = {p: p.read_bytes() for p in protocol_root.rglob("*") if p.is_file()}
    prior_trace = list(trace)
    monkeypatch.setattr(writer, "producer_code_sha256", lambda: "c" * 64)
    monkeypatch.setattr(
        cf, "build_cf_command", lambda *a, **k: pytest.fail("recovery rebuilt current factory")
    )

    class NoSpark:
        def __getattribute__(self, name):
            raise AssertionError("recovery touched Spark")

    result = writer.materialize_feature_command(protocol_root, intent, spark=NoSpark())
    receipt = json.loads((protocol_root / result["completion"]["path"]).read_bytes())
    assert result["recovered"] and trace == prior_trace
    assert all(p.read_bytes() == raw for p, raw in before.items())
    context = json.loads((protocol_root / receipt["context"]["path"]).read_bytes())
    snapshot = json.loads((protocol_root / receipt["snapshot"]["path"]).read_bytes())
    assert receipt["input_mode"] == context["input_mode"] == intent["input_mode"]
    assert (
        snapshot["code_version"]
        == context["runtime_code_sha256"]
        == intent["code_sha256"]
        == "b" * 64
    )
    assert receipt["intent"] == pin(
        protocol_root, next(protocol_root.glob("commands/*/intent.json"))
    )
