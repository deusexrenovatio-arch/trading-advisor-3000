"""Independent closed wire examples; no storage, reader or formula oracle."""

import copy
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"
TABLES = {
    "L0": "research_bar_views",
    "L1": "research_indicator_frames",
    "L2": "research_derived_indicator_frames",
}


def load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def pin(layer):
    return {"path": f"commands/synthetic-{layer.lower()}/completion.json", "sha256": "a" * 64}


def example(layer="L0"):
    seed = load("native_research_read_inputs.v1.json")
    context = load(
        "native_research_l0_context.v1.json"
        if layer == "L0"
        else f"native_research_feature_context.{layer.lower()}.v1.json"
    )
    if layer != "L0":
        context["source"]["completion"] = pin("L0" if layer == "L1" else "L1")
    request = {**seed["request"], "layer": layer, "completion": pin(layer)}
    table = {**seed["table"], "table_path": f"/research-trial/synthetic/{TABLES[layer]}.delta"}
    source = {
        "layer": layer,
        "completion": pin(layer),
        "context": {"path": f"synthetic/{layer}.context.json", "sha256": "b" * 64},
        "intent": {"path": f"synthetic/{layer}.intent.json", "sha256": "c" * 64},
        "snapshot": {
            "path": f"synthetic/{layer}.snapshot.json",
            "sha256": "d" * 64,
            "snapshot_id": f"synthetic-{layer}",
        },
        "table_name": TABLES[layer],
        "table": table,
        "context_payload": context,
    }
    namespace = context["source"]["namespace"]
    envelope = {
        "schema_version": 1,
        "contract_version": "native_research_read.v1",
        "request": request,
        "source": source,
        "identity": seed["identity"],
        "limitations": context["source"]["limitations"],
        "payload": seed["payload"],
    }
    return namespace, envelope


def source_key_example():
    namespace, lower = example()
    _, higher = example("L1")
    higher["request"].update(
        timeframe="1d", source_begin_from="2020-01-01 00:00:00", projection=["sma_20"]
    )
    policy = load("native_research_read_inputs.v1.json")["source_key_policy"]
    request = {
        "schema_version": 1,
        "input_mode": "native_observation_research.v1",
        "mode": "SOURCE_KEY",
        "lower": lower["request"],
        "higher": higher["request"],
        "policy": policy,
    }
    envelope = {
        "schema_version": 1,
        "contract_version": "native_research_source_key_read.v1",
        "request": request,
        "lower": lower,
        "higher": higher,
        "policy": policy,
        "limitations": lower["limitations"],
        "payload": copy.deepcopy(lower["payload"]),
    }
    return namespace, envelope


def shadow_example():
    namespace, lower = example()
    _, l1 = example("L1")
    _, l2 = example("L2")
    for child, parent in [(l1, lower), (l2, l1)]:
        for field in ["snapshot", "context"]:
            child["source"]["context_payload"]["source"][field] = copy.deepcopy(
                parent["source"][field]
            )
    original = lower["source"]["context_payload"]["source"]
    data = {
        k: copy.deepcopy(original[k])
        for k in ["completion", "snapshot", "metadata", "assignments", "source_manifest"]
    }
    data.update(
        intent={"path": "synthetic/data.intent.json", "sha256": "f" * 64},
        table={**lower["source"]["table"], "table_path": "/trial/synthetic/native_bars.delta"},
    )
    descriptor = {
        "schema_version": 1,
        "contract_version": "native_research_shadow.v1",
        "input_mode": "native_observation_research.v1",
        "namespace": namespace,
        "data": data,
        "research": {"L0": lower["source"], "L1": l1["source"], "L2": l2["source"]},
        "limitations": lower["limitations"],
    }
    return namespace, descriptor


def validate(name, value, namespace):
    validate_identity_contract(f"native_research_{name}.v1.json", value, namespace)


@pytest.mark.parametrize("layer", ["L0", "L1", "L2"])
def test_closed_frame_request_and_complete_original_context_envelope(layer):
    namespace, envelope = example(layer)
    validate("read_request", envelope["request"], namespace)
    validate("read", envelope, namespace)


@pytest.mark.parametrize(
    "change",
    [
        {"root": "/foreign"},
        {"latest": True},
        {"known_at": "2020-01-01T00:00:00Z"},
        {"series_type": "continuous_front"},
        {"layer": "bars"},
        {"timeframe": "15m"},
        {"source_begin_from": "2020-01-02T00:00:00Z"},
        {"source_begin_from": "2020-02-30 00:00:00"},
        {"source_begin_from": "2020-01-03 00:00:00"},
        {"projection": ["close", "close"]},
        {"projection": ["higher__close"]},
        {"row_limit": 5001},
        {"byte_limit": 16777217},
        {"engine": "futures"},
        {"instrument_id": "INSTR:foreign"},
    ],
)
def test_source_local_request_cannot_widen_or_alias_the_read_boundary(change):
    namespace, envelope = example()
    request = {**envelope["request"], **change}
    with pytest.raises(SchemaValidationError):
        validate("read_request", request, namespace)


@pytest.mark.parametrize(
    "path",
    [
        "/absolute/file.json",
        "D:/foreign.json",
        "../foreign.json",
        "a/../foreign.json",
        "a\\foreign.json",
    ],
)
def test_completion_pin_is_relative_to_the_service_root(path):
    namespace, envelope = example()
    envelope["request"]["completion"]["path"] = path
    with pytest.raises(SchemaValidationError):
        validate("read_request", envelope["request"], namespace)


@pytest.mark.parametrize(
    "mutation",
    [
        "request_pin",
        "source_layer",
        "context_layer",
        "table",
        "count",
        "execution",
        "identity",
        "duplicate_payload_column",
        "utc_claim",
    ],
)
def test_envelope_rejects_cross_request_layer_identity_or_unproved_admission(mutation):
    namespace, envelope = example()
    if mutation == "request_pin":
        envelope["source"]["completion"]["sha256"] = "f" * 64
    elif mutation == "source_layer":
        envelope["source"]["layer"] = "L2"
    elif mutation == "context_layer":
        envelope["source"]["context_payload"]["layer"] = "L2"
    elif mutation == "table":
        envelope["source"]["table_name"] = TABLES["L2"]
    elif mutation == "count":
        envelope["payload"]["row_count"] = 5001
    elif mutation == "execution":
        envelope["identity"]["execution_eligible"] = True
    elif mutation == "identity":
        envelope["identity"]["tradable"] = False
    elif mutation == "duplicate_payload_column":
        envelope["payload"]["columns"] += ["close"]
    else:
        envelope["limitations"]["timezone_verified"] = True
    with pytest.raises(SchemaValidationError):
        validate("read", envelope, namespace)


def test_source_key_request_is_a_distinct_explicit_retrospective_mode():
    namespace, envelope = source_key_example()
    validate("source_key_request", envelope["request"], namespace)
    validate("source_key_read", envelope, namespace)
    temporal = copy.deepcopy(envelope["request"])
    temporal["mode"] = "TEMPORAL"
    validate("source_key_request", temporal, namespace)
    envelope["request"] = temporal
    with pytest.raises(SchemaValidationError):
        validate("source_key_read", envelope, namespace)


@pytest.mark.parametrize(
    "mutation",
    [
        "lag",
        "clock",
        "context",
        "instrument",
        "timeframe",
        "window",
        "missing_side",
        "outer_rows",
        "inner_selector",
    ],
)
def test_mtf_requires_both_selectors_windows_and_fixed_same_context_policy(mutation):
    namespace, envelope = source_key_example()
    if mutation == "lag":
        envelope["request"]["policy"] = {**envelope["request"]["policy"], "lag_minutes": 0}
    elif mutation == "clock":
        envelope["request"]["policy"] = {**envelope["request"]["policy"], "clock": "UTC"}
    elif mutation == "context":
        envelope["request"]["higher"]["context"] = {
            **envelope["request"]["higher"]["context"],
            "provider": "foreign",
        }
    elif mutation == "instrument":
        envelope["request"]["higher"]["instrument_id"] = "INSTR:foreign"
    elif mutation == "timeframe":
        envelope["request"]["higher"]["timeframe"] = "1m"
    elif mutation == "window":
        envelope["request"]["higher"].pop("source_begin_from")
    elif mutation == "missing_side":
        envelope["request"].pop("higher")
    elif mutation == "outer_rows":
        envelope["payload"]["row_count"] = 1
    else:
        envelope["higher"] = copy.deepcopy(envelope["higher"])
        envelope["higher"]["request"]["source_begin_from"] = "2019-12-31 00:00:00"
    with pytest.raises(SchemaValidationError):
        validate("source_key_read", envelope, namespace)


def test_shadow_has_only_existing_exact_pins_and_no_generated_identity_or_time():
    namespace, shadow = shadow_example()
    validate("shadow", shadow, namespace)
    for extra in [
        {"created_at_utc": "2026-01-01T00:00:00Z"},
        {"snapshot_id": "new-random-id"},
        {"published_current": "shadow"},
    ]:
        with pytest.raises(SchemaValidationError):
            validate("shadow", {**shadow, **extra}, namespace)


@pytest.mark.parametrize(
    "mutation", ["mixed_l0", "mixed_l1", "wrong_layer", "source_pin", "missing_layer"]
)
def test_shadow_rejects_individually_valid_but_mixed_generations(mutation):
    namespace, shadow = shadow_example()
    if mutation in {"mixed_l0", "mixed_l1"}:
        node = shadow["research"]["L1" if mutation == "mixed_l0" else "L2"]
        node["context_payload"]["source"]["completion"] = {
            "path": "foreign/completion.json",
            "sha256": "f" * 64,
        }
    elif mutation == "wrong_layer":
        shadow["research"]["L0"] = shadow["research"]["L1"]
    elif mutation == "source_pin":
        shadow["data"]["metadata"]["sha256"] = "f" * 64
    else:
        shadow["research"].pop("L2")
    with pytest.raises(SchemaValidationError):
        validate("shadow", shadow, namespace)


def test_new_read_side_l2_does_not_expand_the_old_compute_parent_contract():
    source = load("native_research_core_inputs.v1.json")["source"]
    namespace = source["namespace"]
    validate_identity_contract("native_research_compute.v1.json", source, namespace)
    source["layer"] = "L2"
    source["table_name"] = TABLES["L2"]
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_research_compute.v1.json", source, namespace)
