"""Opt-in feature shapes; fixtures do not establish native computation readiness."""

import json
import subprocess
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"


def fixture(name, layer=None):
    suffix = "." + layer.lower() if layer else ""
    return json.loads((FIXTURES / (name + suffix + ".v1.json")).read_bytes())


def namespace():
    return fixture("native_research_compute")["namespace"]


def check(name, value):
    validate_identity_contract(name + ".v1.json", value, namespace())


def test_closed_compute_shape():
    check("native_research_compute", fixture("native_research_compute"))


@pytest.mark.parametrize("layer", ["L1", "L2"])
@pytest.mark.parametrize("kind", ["context", "intent", "completion"])
def test_closed_feature_shapes(kind, layer):
    name = "native_research_feature_" + kind
    check(name, fixture(name, layer))


@pytest.mark.parametrize(
    "mutation",
    [
        "backend",
        "parent",
        "root",
        "history",
        "operation",
        "tolerance",
        "legacy_id",
        "price_json",
        "hash",
        "foreign_asset",
        "engine",
    ],
)
def test_features_cannot_admit_another_basis_or_unbound_claim(mutation):
    value = fixture("native_research_feature_context", "L1")
    if mutation == "backend":
        value["numerical_identity"]["engines"]["talib_absent"] = False
    elif mutation == "parent":
        value["source"]["layer"] = "L1"
    elif mutation == "root":
        value["source"]["root"] = "/outside"
    elif mutation == "history":
        value["policy"]["history_origin"] = "invented_pre_capture_seed"
    elif mutation == "operation":
        value["capabilities"][0]["feature"] = "volume"
    elif mutation == "tolerance":
        value["quality"]["policy"]["new_feature_values"]["rtol"] = 1.0
    elif mutation == "legacy_id":
        value["series_id"] = "legacy"
    elif mutation == "price_json":
        value["close"] = 1.2
    elif mutation == "hash":
        value["numerical_identity"]["policy_sha256"] = "f" * 64
    elif mutation == "foreign_asset":
        value["capabilities"][0]["sequence"]["asset_id"] = "ASSET:foreign"
    else:
        value["capabilities"][0]["sequence"]["engine"] = "futures"
    with pytest.raises(SchemaValidationError):
        check("native_research_feature_context", value)


@pytest.mark.parametrize(
    "mutation",
    [
        "ready_zero",
        "over_analysis",
        "over_total",
        "wrong_history",
        "missing_first",
        "wrong_layer_feature",
    ],
)
def test_analysis_capabilities_have_fixed_history_and_finite_bounds(mutation):
    value = fixture("native_research_feature_context", "L2")
    cap = next(c for c in value["capabilities"] if c["feature"] == "close_change_1")
    if mutation == "ready_zero":
        cap["analysis_finite_count"] = 0
    elif mutation == "over_analysis":
        cap["analysis_finite_count"] = 2
        cap["finite_count"] = 2
    elif mutation == "over_total":
        cap["finite_count"] = 3
    elif mutation == "wrong_history":
        cap["required_observations"] = 1
    elif mutation == "missing_first":
        cap["first_finite_source_begin"] = None
    else:
        cap["feature"] = "sma_20"
        cap["required_observations"] = 20
    with pytest.raises(SchemaValidationError):
        check("native_research_feature_context", value)


@pytest.mark.parametrize(
    "field,value",
    [("row_count", 20001), ("file_count", 129), ("physical_bytes", 67108865), ("layer", "L2")],
)
def test_compute_limits_and_parent_layers_are_closed(field, value):
    payload = fixture("native_research_compute")
    payload[field] = value
    with pytest.raises(SchemaValidationError):
        check("native_research_compute", payload)


def test_l0_and_all_prior_shared_definitions_unchanged():
    path = "src/trading_advisor_3000/product_plane/contracts/schemas/market_identity.v3.json"
    previous = json.loads(subprocess.check_output(["git", "show", "9fbc8aaa:" + path]))
    current = json.loads(Path(path).read_bytes())
    assert all(current["$defs"][key] == value for key, value in previous["$defs"].items())
