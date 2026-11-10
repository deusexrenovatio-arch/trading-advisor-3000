"""Closed opt-in shapes; actual data and computation need separate native proof."""

import copy
import json
import subprocess
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"
NAMES = (
    "native_observation_compute",
    "native_research_l0_context",
    "native_research_l0_intent",
    "native_research_l0_completion",
)


def fixture(name):
    return json.loads((FIXTURES / (name + ".v1.json")).read_bytes())


def namespace():
    return fixture("native_observation_compute")["namespace"]


@pytest.mark.parametrize("name", NAMES)
def test_synthetic_native_research_shape(name):
    validate_identity_contract(name + ".v1.json", fixture(name), namespace())


@pytest.mark.parametrize(
    "field,value",
    [("row_count", 20001), ("file_count", 129), ("physical_bytes", 67108865)],
)
def test_compute_bounds_are_separate_closed_limits(field, value):
    payload = fixture("native_observation_compute")
    payload[field] = value
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_observation_compute.v1.json", payload, namespace())


@pytest.mark.parametrize(
    "field",
    [
        "canonical_Q_verified",
        "historical_PIT_verified",
        "timezone_verified",
        "utc_closure_verified",
        "session_complete_verified",
        "execution_eligible",
    ],
)
def test_observation_compute_cannot_admit_missing_time_or_execution_facts(field):
    payload = fixture("native_observation_compute")
    payload["limitations"][field] = True
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_observation_compute.v1.json", payload, namespace())


def test_research_cannot_choose_another_numerical_or_history_basis():
    payload = fixture("native_research_l0_context")
    payload["policy"]["history_origin"] = "invented_pre_capture_state"
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_research_l0_context.v1.json", payload, namespace())


@pytest.mark.parametrize("state", ["filled_zero", "ready_for_execution", "unknown"])
def test_capability_reasons_are_not_interchangeable(state):
    payload = fixture("native_research_l0_context")
    payload["capabilities"][0]["state"] = state
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_research_l0_context.v1.json", payload, namespace())


def test_compute_sequence_cannot_refer_to_foreign_instrument():
    payload = fixture("native_observation_compute")
    payload["sequences"][0]["instrument_id"] = "INSTR:foreign"
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_observation_compute.v1.json", payload, namespace())


@pytest.mark.parametrize("name", NAMES)
def test_no_legacy_ids_unpinned_mode_or_price_json_extension(name):
    original = fixture(name)
    for field, value in [
        ("series_id", "legacy"),
        ("known_at", "2020-01-01T00:00:00Z"),
        ("close", 1.2),
    ]:
        payload = copy.deepcopy(original)
        payload[field] = value
        with pytest.raises(SchemaValidationError):
            validate_identity_contract(name + ".v1.json", payload, namespace())


def test_stage31_and_legacy_definitions_are_unchanged():
    path = "src/trading_advisor_3000/product_plane/contracts/schemas/market_identity.v3.json"
    previous = json.loads(
        subprocess.check_output(["git", "show", "2c1eac080e3a938fbcfcab68d82c0fbf532f6735:" + path])
    )
    current = json.loads(Path(path).read_bytes())
    assert all(current["$defs"][key] == value for key, value in previous["$defs"].items())


@pytest.mark.parametrize(
    "mutation",
    [
        "execution_ready",
        "zero_ready",
        "overcount",
        "wrong_required",
        "foreign_asset",
        "wrong_engine",
        "wrong_policy_hash",
    ],
)
def test_l0_closed_claims_reject_inconsistent_metadata(mutation):
    payload = fixture("native_research_l0_context")
    cap = payload["capabilities"][0]
    if mutation == "execution_ready":
        cap = next(c for c in payload["capabilities"] if c["feature"] == "execution")
        cap.update(state="ready", finite_count=1)
    elif mutation == "zero_ready":
        cap["finite_count"] = 0
    elif mutation == "overcount":
        cap["finite_count"] = 999
    elif mutation == "wrong_required":
        cap["required_observations"] = 1
    elif mutation == "foreign_asset":
        cap["sequence"]["asset_id"] = "ASSET:foreign"
    elif mutation == "wrong_engine":
        cap["sequence"]["engine"] = "futures"
    else:
        payload["numerical_identity"]["policy_sha256"] = "f" * 64
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_research_l0_context.v1.json", payload, namespace())
