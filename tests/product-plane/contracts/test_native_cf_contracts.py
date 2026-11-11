"""Independent CF boundary contrasts; fixtures do not admit exchange facts."""

import copy
import hashlib
import json
import re
from datetime import date
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    _source_date,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research.native_cf_policy import (
    LIMITATIONS,
    LIMITS,
    PREFIX_SCOPE,
    TABLES,
    cf_layouts,
    cf_policy,
    cf_policy_ref,
)
from trading_advisor_3000.product_plane.research.native_core_policy import core_policy

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"


def seed():
    return json.loads((FIXTURES / "native_cf_inputs.v1.json").read_text(encoding="utf-8"))


def pin(name):
    return {"path": f"synthetic/{name}.json", "sha256": "a" * 64}


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def definition(offset=2):
    inputs = seed()
    return {
        "input_mode": "native_cf_observation.v1",
        "source": {
            "namespace": inputs["namespace"]["namespace"],
            "provider": "moex_iss",
            "venue": "MOEX",
            "engine": "futures",
            "market": "forts",
            "board": "RFUD",
        },
        "asset_id": "ASSET:SBER",
        "components": inputs["components"],
        "numeric_price_basis": {
            "version": "sbrf_iss_numeric_quote.v1",
            "parent_l0_completion": inputs["parent_l0_completion"],
            "source_binding": inputs["source_fact_pins"][0],
            "dated_specification": inputs["source_fact_pins"][1],
            "source_price_columns": ["open", "high", "low", "close"],
            "quote_currency": "RUB",
            "quote_basis": "RUB_per_contract_lot",
            "conversion": "none",
            "full_specification_verified": False,
        },
        "roll": {
            "method": "calendar_last_trade_source_date_offset.v1",
            "offset_sessions": offset,
            "date_policy": "source_label_date_selection.v1",
            "switch": "first_selected_source_label_on_or_after_planned_switch_date",
            "reference": "last_selected_old_close_and_first_selected_new_close",
        },
        "adjustment": {
            "method": "additive",
            "direction": "causal_forward",
            "anchor": "current_epoch",
            "knowledge_policy": "max_own_and_USED_reference_source_end_labels_no_UTC_or_PIT",
        },
        "source_key_basis": "native_source_begin.v1",
        "profiles": {
            "l1": "native_cf_prefix_l1.v1",
            "l2": "native_cf_prefix_l2.v1",
            "roll_rules": "roll_rules_hybrid_v1",
        },
    }


def example(layer="CF"):
    inputs = seed()
    policy = cf_policy(layer)
    numerical = {
        "version": "native_cf_numerical.v1",
        "layer": layer,
        "basis": "observed_cf_composed_numeric_sequence.v1",
        "policy_sha256": digest(policy),
        "kernels": {k: "b" * 64 for k in ["selector", "adapter", "indicators", "derived"]},
        "engines": {
            "python": "3.11.15",
            "numpy": "2.4.6",
            "pandas": "2.3.3",
            "pandas_ta_classic": "0.4.47",
            "spark": "3.5.8",
            "delta": "3.3.2",
            "arrow": "24.0.0",
            "talib_absent": True,
            "numba": "0.65.1",
            "scipy": "1.17.1",
            "ta_numba_enabled": True,
            "ta_scipy_enabled": True,
        },
        "digest": "c" * 64,
    }
    definitions = [definition(2), definition(1)]
    schedule = inputs["calendar_roll_schedule"]
    schedule["groups"] = [
        dict(
            copy.deepcopy(schedule["groups"][0]),
            group_key=json.dumps(d, sort_keys=True),
            roll_offset_trading_days=d["roll"]["offset_sessions"],
        )
        for d in definitions
    ]
    parent = inputs["parent_l0_completion"] if layer == "CF" else pin(policy["parent_layer"])
    intent = {
        "schema_version": 1,
        "contract_version": "native_cf_intent.v1",
        "input_mode": "native_cf_observation.v1",
        "layer": layer,
        "command_id": "synthetic-shapes-only",
        "source_root": "/research-trial",
        "parent_completion": parent,
        "definitions": definitions,
        "calendar_roll_schedule": schedule,
        "source_fact_pins": inputs["source_fact_pins"],
        "policy": cf_policy_ref(layer),
        "policy_sha256": digest(policy),
        "numerical_identity": numerical,
        "code_sha256": "d" * 64,
    }
    context = {
        "schema_version": 1,
        "contract_version": "native_cf_context.v1",
        "input_mode": intent["input_mode"],
        "layer": layer,
        "cf_context_id": "CFCTX:" + "e" * 64,
        "source": {
            "parent_completion": parent,
            "parent_layer": policy["parent_layer"],
            "parent_contract_version": policy["parent_contract_version"],
            "parent_profile_version": {
                "CF": None,
                "CF-L1": "native_cf_composition.v1",
                "CF-L2": "native_cf_prefix_l1.v1",
            }[layer],
            "l0_completion": inputs["parent_l0_completion"],
            "parent_snapshot": {**pin("parent-snapshot"), "snapshot_id": "synthetic-parent"},
            "parent_context": pin("parent-context"),
            "namespace": inputs["namespace"],
        },
        "definitions": definitions,
        "calendar_roll_schedule": schedule,
        "source_fact_pins": intent["source_fact_pins"],
        "policy": intent["policy"],
        "policy_sha256": intent["policy_sha256"],
        "policy_payload": policy,
        "layouts": cf_layouts(layer),
        "numerical_identity": numerical,
        "runtime_code_sha256": intent["code_sha256"],
        "quality": {
            "version": "native_cf_comparison.v1",
            "parent_fields": "exact",
            "masks": "exact",
            "discrete": "exact",
            "nonfinite": "forbidden",
            "new_feature_values": {"rtol": 1e-12, "atol": 1e-10},
        },
        "selection": {
            "parent_inspected_rows": 8,
            "eligible_component_rows": 6,
            "selected_rows": 4,
            "analysis_rows": 2,
            "observed_events": 1,
            "ladder_rows": 1,
            "qc_rows": 2,
        },
        "sequences": [
            {
                "definition": d,
                "timeframe": "1m",
                "selected_rows": 2,
                "analysis_rows": 1,
                "first_source_begin": "2026-09-14 09:00:00",
                "last_source_begin": "2026-09-15 09:00:00",
                "last_source_end": "2026-09-15 09:00:59",
                "planned_roll_ordinals": [0, 1] if d["roll"]["offset_sessions"] == 2 else [0],
                "observed_events": 1 if d["roll"]["offset_sessions"] == 2 else 0,
            }
            for d in definitions
        ],
        "used_references": [
            inputs["used_reference"],
            {
                **inputs["used_reference"],
                "side": "new",
                "instrument_id": "SYNTH:SRZ6",
                "source_begin": "2026-09-15 06:59:00",
                "source_end": "2026-09-15 06:59:59",
            },
        ],
        "capabilities": [],
        "limits": LIMITS,
        "limitations": LIMITATIONS,
    }
    completion = {
        "schema_version": 1,
        "contract_version": "native_cf_completion.v1",
        "input_mode": intent["input_mode"],
        "status": "complete_unpublished",
        "layer": layer,
        "command_id": intent["command_id"],
        "input_sha256": digest(intent),
        "attempt_id": "f" * 32,
        "snapshot": {**pin("snapshot"), "snapshot_id": "synthetic-CF"},
        "context": pin("context"),
        "intent": pin("intent"),
        "source_root": "/research-trial",
        "parent_completion": parent,
        "created_at_utc": "2026-11-11T00:00:00Z",
    }
    reference = json.loads(
        (FIXTURES / "native_research_read_inputs.v1.json").read_text(encoding="utf-8")
    )["table"]
    tables = [
        {
            "role": role,
            "table_name": TABLES[role],
            "reference": dict(
                copy.deepcopy(reference),
                table_path=f"/research-trial/synthetic/{TABLES[role]}.delta",
                row_count=0 if role in {"events", "ladder"} else 4,
            ),
            "layout": layout,
        }
        for role, layout in cf_layouts(layer).items()
    ]
    compute = {
        "schema_version": 1,
        "contract_version": "native_cf_compute.v1",
        "input_mode": intent["input_mode"],
        "layer": layer,
        "root": "/research-trial",
        "completion": pin("completion"),
        "context": completion["context"],
        "snapshot": completion["snapshot"],
        "parent_completion": parent,
        "l0_completion": inputs["parent_l0_completion"],
        "namespace": inputs["namespace"],
        "context_payload": context,
        "tables": tables,
        "limits": LIMITS,
        "limitations": LIMITATIONS,
    }
    request = {
        "schema_version": 1,
        "contract_version": "native_cf_read_request.v1",
        "input_mode": intent["input_mode"],
        "completion": compute["completion"],
        "layer": layer,
        "role": {"CF": "bars", "CF-L1": "indicators", "CF-L2": "derived"}[layer],
        "definition": definitions[0],
        "timeframe": "1m",
        "selection": {
            "kind": "source_key_interval",
            "source_begin_from": "2026-09-14 00:00:00",
            "source_begin_to": "2026-09-16 00:00:00",
        },
        "projection": ["close"],
        "row_limit": 5000,
        "byte_limit": 16777216,
    }
    envelope = {
        "schema_version": 1,
        "contract_version": "native_cf_read.v1",
        "request": request,
        "source": compute,
        "identity": {
            "definition": definitions[0],
            "instrument_id": None,
            "tradable": False,
            "execution_eligible": False,
        },
        "limitations": LIMITATIONS,
        "payload": {
            "row_count": 0,
            "columns": ["cf_definition_json", "timeframe", "source_begin", "close"],
            "schema_ipc_base64": "synthetic-shape-only",
            "ipc_sha256": "f" * 64,
        },
    }
    return inputs["namespace"], {
        "policy": policy,
        "intent": intent,
        "context": context,
        "completion": completion,
        "compute": compute,
        "read_request": request,
        "read": envelope,
    }


def validate(kind, payload, namespace):
    validate_identity_contract(f"native_cf_{kind}.v1.json", payload, namespace)


@pytest.mark.parametrize("layer", ["CF", "CF-L1", "CF-L2"])
def test_closed_real_factory_metadata_and_producer_shaped_boundary_family(layer):
    namespace, payloads = example(layer)
    for kind, payload in payloads.items():
        validate(kind, payload, namespace)


def test_source_date_is_calendar_valid_and_never_a_UTC_instant():
    assert _source_date("2026-09-17") == date(2026, 9, 17)
    for value in ["2026-02-30", "2026-09-17T00:00:00Z", "2026-9-17"]:
        with pytest.raises(SchemaValidationError):
            _source_date(value)


@pytest.mark.parametrize(
    "mutation",
    [
        "date",
        "matdate",
        "expiry",
        "venue",
        "descriptor",
        "quote",
        "offset",
        "family",
        "duplicate_component",
    ],
)
def test_observed_family_cannot_invent_source_or_inherit_strict_CF_admission(mutation):
    namespace, payloads = example()
    intent = payloads["intent"]
    d = intent["definitions"][0]
    if mutation == "date":
        d["components"][0]["last_trade_date"] = "2026-02-30"
    elif mutation == "matdate":
        d["components"][0]["last_trade_source"]["row_value"] = "MATDATE"
    elif mutation == "expiry":
        d["components"][0]["expiry_utc"] = "2026-09-17T16:00:00Z"
    elif mutation == "venue":
        d["source"]["venue"] = "FOREIGN"
    elif mutation == "descriptor":
        d["components"][0]["instrument"]["size_variant"] = {"state": "known", "value": "mini"}
    elif mutation == "quote":
        d["numeric_price_basis"]["conversion"] = "multiply_by_lot"
    elif mutation == "offset":
        d["roll"]["offset_sessions"] = 3
    elif mutation == "family":
        intent["input_mode"] = "native_observation_research.v1"
    else:
        d["components"][1] = copy.deepcopy(d["components"][0])
    with pytest.raises(SchemaValidationError):
        validate("intent", intent, namespace)


@pytest.mark.parametrize(
    "mutation", ["parent", "own_output", "clock", "selection", "ladder", "namespace"]
)
def test_non_circular_context_and_selected_cardinality_remain_explicit(mutation):
    namespace, payloads = example("CF-L1")
    context = payloads["context"]
    if mutation == "parent":
        context["source"]["parent_layer"] = "L0"
    elif mutation == "own_output":
        context["output_active_files_sha256"] = "f" * 64
    elif mutation == "clock":
        context["limitations"] = {**context["limitations"], "timezone_verified": True}
    elif mutation == "selection":
        context["selection"]["selected_rows"] = 9
    elif mutation == "ladder":
        context["selection"]["ladder_rows"] = 0
    else:
        context["source"]["namespace"]["instruments"][0]["asset_id"] = "FOREIGN"
    with pytest.raises(SchemaValidationError):
        validate("context", context, namespace)


def test_full_profile_policy_separates_actual_reset_invocation_from_broad_flags():
    policy = cf_policy("CF-L1")
    rows = {row["output"]: row for row in policy["catalog"]}
    assert {row["operation_key"] for row in rows.values() if row["actual_reset"]} == {
        "obv",
        "ad",
        "adosc",
        "pvt",
        "pvo",
    }
    for name in ["cmf_20", "mfi_14", "force_index_13"]:
        assert rows[name]["declared_group_reset_on_roll"] and not rows[name]["actual_reset"]
        assert rows[name]["invocation_domain"] == "composed_prefix"
    assert rows["rvol_20"]["invocation_domain"] == "composed_prefix"
    assert rows["rvol_20"]["mask_domain"] == "current_epoch_prefix"
    assert (
        rows["macd_12_26_9"]["observed_call_safe"],
        rows["macd_12_26_9"]["configured_group_minimum"],
    ) == (26, 34)
    assert (rows["pvt"]["observed_call_safe"], rows["pvt"]["configured_group_minimum"]) == (1, 2)
    assert rows["true_range"]["passthrough"] == "cf_l0__true_range"
    assert rows["true_range"]["invocation_owner"] == "composed_CF_L0_existing_P0_metric_kernel"
    assert re.fullmatch("[0-9a-f]{64}", policy["rule_set_sha256"])
    assert re.fullmatch("[0-9A-F]{16}", policy["legacy_rule_set_fingerprint"])


def test_L2_admission_passthrough_and_prefix_scope_do_not_reinterpret_core():
    original = core_policy("L2")
    rows = {row["output"]: row for row in cf_policy("CF-L2")["catalog"]}
    assert rows["close_change_1"]["admission_minimum"] == 2
    assert rows["volume_change_1"]["admission_minimum"] == 2
    assert rows["volume_change_1"]["mask_domain"] == "current_epoch_prefix"
    for name, minimum in [("close_slope_20", 21), ("rvol_20", 20)]:
        assert rows[name]["admission_minimum"] == minimum
        assert rows[name]["invocation_owner"] == "exact_CF_L1_passthrough"
        assert rows[name]["origin_formula_warmup_bars"] == 300
    assert core_policy("L2") == original
    assert "used_reference_pins" in PREFIX_SCOPE["same_inputs"]
    assert "values_may_differ" in PREFIX_SCOPE["changed_used_references"]
    assert "no_future_veto" in rows["volume_change_1"]["quantity_admission"]


def test_two_semantically_distinct_R_definitions_and_typed_empty_auxiliary_reads():
    namespace, payloads = example()
    d2, d1 = payloads["intent"]["definitions"]
    assert d2["roll"]["offset_sessions"] == 2 and d1["roll"]["offset_sessions"] == 1
    assert digest(d2) != digest(d1)
    for role in ["events", "ladder", "qc"]:
        request = copy.deepcopy(payloads["read_request"])
        request.update(role=role, selection={"kind": "whole_role"})
        validate("read_request", request, namespace)
    compute = payloads["compute"]
    assert all(
        t["reference"]["row_count"] == 0
        for t in compute["tables"]
        if t["role"] in {"events", "ladder"}
    )
    validate("compute", compute, namespace)
    assert all(t["layout"]["fields"] for t in compute["tables"])


@pytest.mark.parametrize(
    "mutation", ["role", "fake_qc_time", "unbounded", "cross_layer", "tradable", "latest"]
)
def test_public_CF_read_uses_its_own_role_and_identity_boundary(mutation):
    namespace, payloads = example()
    read = payloads["read"]
    if mutation == "role":
        read["request"]["role"] = "derived"
    elif mutation == "fake_qc_time":
        read["request"]["role"] = "qc"
    elif mutation == "unbounded":
        read["request"]["row_limit"] = 5001
    elif mutation == "cross_layer":
        read["source"]["layer"] = "CF-L2"
    elif mutation == "tradable":
        read["identity"]["tradable"] = True
    else:
        read["request"]["latest"] = True
    with pytest.raises(SchemaValidationError):
        validate("read", read, namespace)


def test_strict_native_and_v3_readers_do_not_accept_observed_CF_payloads():
    namespace, payloads = example()
    for name, payload in [
        ("native_research_compute.v1.json", payloads["compute"]),
        ("native_research_read_request.v1.json", payloads["read_request"]),
        ("continuous_front_bars.v3.json", payloads["read"]),
    ]:
        with pytest.raises(SchemaValidationError):
            validate_identity_contract(name, payload, namespace)


def test_securities_locator_must_name_the_actual_component_symbol():
    namespace, payloads = example()
    component = payloads["intent"]["definitions"][0]["components"][0]
    component["last_trade_source"].update(
        kind="moex_iss_securities",
        block="securities",
        row_key="SECID",
        row_value="SRU6",
        value_column="LASTTRADEDATE",
    )
    validate("intent", payloads["intent"], namespace)
    component["last_trade_source"]["row_value"] = "SRZ6"
    with pytest.raises(SchemaValidationError):
        validate("intent", payloads["intent"], namespace)


def test_missing_required_auxiliary_role_cannot_hide_behind_an_extra_table():
    namespace, payloads = example()
    compute = payloads["compute"]
    compute["tables"][-1]["role"] = "derived"
    compute["tables"][-1]["table_name"] = TABLES["derived"]
    with pytest.raises(SchemaValidationError):
        validate("compute", compute, namespace)


@pytest.mark.parametrize("layer", ["CF", "CF-L1", "CF-L2"])
def test_cf_role_layouts_reach_existing_physical_adapter(layer):
    import pyarrow as pa

    from trading_advisor_3000.product_plane.data_plane.native_observations import physical_schema

    for role, layout in cf_layouts(layer).items():
        schema = physical_schema(layout)
        assert schema.field("cf_definition_json").type == pa.string()
        assert not schema.field("cf_definition_json").nullable
        ordinal = (
            "roll_ordinal"
            if role in {"events", "ladder"}
            else "selected_rows"
            if role == "qc"
            else "planned_roll_ordinal"
        )
        assert schema.field(ordinal).type == pa.int64()
        assert not schema.field(ordinal).nullable
