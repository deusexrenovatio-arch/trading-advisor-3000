"""Oracles: specified identity invariants and independently chosen interval edges.

Fixtures are synthetic, as documented in fixtures/contracts/market_identity-fixtures.md.
These tests prove no runtime producer, exchange specification or migration.
"""

from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts import identity_v3 as identity
from trading_advisor_3000.product_plane.contracts.schema_validation import (
    SchemaValidationError,
    load_schema,
    validate_schema,
)

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests/product-plane/fixtures/contracts"
SCHEMAS = ROOT / "src/trading_advisor_3000/product_plane/contracts/schemas"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def namespace() -> dict:
    return fixture("instrument_namespace.v1.json")


@pytest.fixture
def cf() -> dict:
    return fixture("continuous_front_bars.v3.json")


def validate(payload: dict, namespace: dict) -> None:
    name = (
        "canonical_bar.v3.json"
        if payload["definition"]["series_type"] == "native"
        else "continuous_front_bars.v3.json"
    )
    identity.validate_identity_contract(name, payload, namespace)


@pytest.mark.parametrize(
    "name",
    [
        "canonical_bar.v3.json",
        "canonical_bar.v3.imoex.json",
        "canonical_bar.v3.srz6.json",
        "continuous_front_bars.v3.json",
    ],
)
def test_native_equity_index_future_and_multicomponent_cf(name: str, namespace: dict) -> None:
    payload = fixture(name)
    original = copy.deepcopy(payload)
    validate(payload, namespace)
    identity.logical_key(payload, namespace)
    assert payload == original  # pure validation/key extraction cannot rewrite input
    assert "contract_id" not in payload
    if payload["definition"]["series_type"] == "continuous_front":
        assert {x["instrument_id"] for x in payload["components"]} == {"SRZ6@MOEX", "SRH7@MOEX"}


@pytest.mark.parametrize("missing", [None, "absent"])
def test_native_requires_concrete_instrument(namespace: dict, missing: str | None) -> None:
    payload = fixture("canonical_bar.v3.json")
    if missing == "absent":
        del payload["definition"]["instrument_id"]
    else:
        payload["definition"]["instrument_id"] = missing
    with pytest.raises(SchemaValidationError):
        validate(payload, namespace)


@pytest.mark.parametrize(
    "field,value",
    [
        ("tradable", True),
        ("execution_eligible", True),
        ("economics_state", "known"),
        ("instrument_role", "derivative"),
    ],
)
def test_reference_index_never_gets_execution_admission(
    namespace: dict, field: str, value: object
) -> None:
    namespace["instruments"][1][field] = value
    with pytest.raises(SchemaValidationError):
        validate(fixture("canonical_bar.v3.imoex.json"), namespace)


def test_active_contract_and_lineage_not_cf_key(cf: dict, namespace: dict) -> None:
    before = identity.logical_key(cf, namespace)
    cf["active_instrument_id"] = "SRH7@MOEX"
    cf["active_applicability"] = {
        "as_of": "2026-11-02T10:30:00Z",
        "valid_from": "2026-11-02T10:30:00Z",
        "valid_to": cf["ts_close"],
    }
    cf["components"].reverse()
    assert identity.logical_key(cf, namespace) == before
    cf["active_instrument_id"] = None
    cf["active_applicability"] = None
    assert identity.logical_key(cf, namespace) == before


@pytest.mark.parametrize(
    "axis", ["roll", "adjustment", "structural", "venue", "size", "deliverable", "quantity", "q"]
)
def test_policy_structural_venue_and_actual_size_changes_separate_cf(
    axis: str, cf: dict, namespace: dict
) -> None:
    before = identity.logical_key(cf, namespace)
    d = cf["definition"]
    if axis == "roll":
        d["roll"]["switch_rule"]["version"] = "fixture-2"
    elif axis == "adjustment":
        d["adjustment"].update(
            method="additive", direction="backward", anchor="first_observed_close"
        )
    elif axis == "q":
        d["q"]["quality"]["version"] = "fixture-2"
    elif axis == "venue":
        d["q"]["venue"] = "OTHER_VENUE"
        d["scope"]["eligibility"][0]["venue"] = "OTHER_VENUE"
        for inst in namespace["instruments"][2:]:
            inst["venue"] = "OTHER_VENUE"
    else:
        spec = d["scope"]["specification"]
        if axis == "structural":
            spec["settlement"] = "physical"
        elif axis == "size":
            spec["contract_size"]["value"] = 100  # same standard label, different actual size
        elif axis == "deliverable":
            spec["deliverable_or_benchmark"]["value"] = "illustrative different share class"
        elif axis == "quantity":
            spec["quantity_unit"] = "lots"
            d["q"]["quantity_unit"] = "lots"
        for inst in namespace["instruments"][2:]:
            inst["specification"] = copy.deepcopy(spec)
    assert identity.logical_key(cf, namespace) != before


def test_native_instruments_and_boards_remain_distinct(namespace: dict) -> None:
    original = fixture("canonical_bar.v3.json")
    other = copy.deepcopy(original)
    descriptor = copy.deepcopy(namespace["instruments"][0])
    descriptor.update(instrument_id="SBER@OTHER", venue="OTHER", board="OTHER_BOARD")
    namespace["instruments"].append(descriptor)
    other["definition"]["instrument_id"] = descriptor["instrument_id"]
    other["definition"]["q"].update(venue="OTHER", board="OTHER_BOARD")
    assert identity.logical_key(other, namespace) != identity.logical_key(original, namespace)


def test_unknown_does_not_prove_equal_scopes(cf: dict, namespace: dict) -> None:
    definition = cf["definition"]
    definition["scope"]["specification"]["contract_size"] = {"state": "unknown", "value": None}
    identity.validate_identity_contract(identity.AUTHORITY, definition, namespace)
    # Even identical unknown definitions cannot establish an economic equivalence key.
    for candidate in (definition, copy.deepcopy(definition)):
        with pytest.raises(SchemaValidationError, match="unknown"):
            identity.definition_key(candidate, namespace)
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)  # known component cannot prove the unknown scope


def test_cf_definition_requires_existing_underlying_asset(cf: dict, namespace: dict) -> None:
    cf["definition"]["asset_id"] = "ABSENT"
    cf["definition"]["scope"]["asset_id"] = "ABSENT"
    with pytest.raises(SchemaValidationError):
        identity.definition_key(cf["definition"], namespace)


@pytest.mark.parametrize(
    "state,value", [("unknown", "mini"), ("known", None), ("not_applicable", "micro")]
)
def test_size_fact_states_cannot_be_conflated(
    state: str, value: object, cf: dict, namespace: dict
) -> None:
    cf["definition"]["scope"]["specification"]["size_variant"] = {"state": state, "value": value}
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_fk",
        "wrong_asset",
        "wrong_size",
        "expiry",
        "observation_end",
        "component_outside",
        "empty_interval",
        "namespace",
        "duplicate_instrument",
        "missing_asset",
    ],
)
def test_namespace_and_applicability_fail_closed(mutation: str, cf: dict, namespace: dict) -> None:
    if mutation == "missing_fk":
        cf["active_instrument_id"] = "MISSING"
    elif mutation == "wrong_asset":
        namespace["instruments"][2]["asset_id"] = "IMOEX"
    elif mutation == "wrong_size":
        namespace["instruments"][2]["specification"]["contract_size"]["value"] = 1
    elif mutation == "expiry":
        namespace["instruments"][2]["expiry"] = "2020-01-01T00:00:00Z"
    elif mutation == "observation_end":
        cf["active_applicability"]["as_of"] = cf["active_applicability"]["valid_to"]
    elif mutation == "component_outside":
        cf["components"][0]["valid_from"] = "2026-11-02T09:59:59Z"
    elif mutation == "empty_interval":
        cf["components"][0]["valid_to"] = cf["components"][0]["valid_from"]
    elif mutation == "namespace":
        cf["definition"]["q"]["namespace"] = "OTHER"
    elif mutation == "duplicate_instrument":
        namespace["instruments"].append(copy.deepcopy(namespace["instruments"][2]))
    elif mutation == "missing_asset":
        namespace["assets"] = [namespace["assets"][1]]
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


@pytest.mark.parametrize("forbidden", ["series_id", "family_id", "product_id", "contract_id"])
def test_no_opaque_grouping_ids_in_new_payload(forbidden: str, cf: dict, namespace: dict) -> None:
    cf["definition"][forbidden] = "opaque"
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


def resolve(records: list[dict], namespace: dict, **overrides: str) -> dict:
    args = dict(
        namespace=namespace,
        source_symbol="SBER",
        source_context=fixture("temporal_mapping.v1.json")["source_context"],
        target_kind="instrument",
        relation="alias",
        valid_at="2026-11-02T10:00:00Z",
        known_at="2026-11-02T10:00:00Z",
    )
    args.update(overrides)
    return identity.resolve_temporal_mapping(records, **args)


@pytest.mark.parametrize(
    "axis,start,end",
    [("valid", "valid_from", "valid_to"), ("known", "recorded_from", "recorded_to")],
)
def test_temporal_axes_are_independent_half_open(
    axis: str, start: str, end: str, namespace: dict
) -> None:
    mapping = fixture("temporal_mapping.v1.json")
    mapping[start], mapping[end] = "2026-11-02T10:00:00Z", "2026-11-02T11:00:00Z"
    assert resolve([mapping], namespace, **{axis + "_at": mapping[start]}) == mapping
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve([mapping], namespace, **{axis + "_at": mapping[end]})
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve([mapping], namespace, **{axis + "_at": "2026-11-02T09:59:59Z"})


def test_retrieval_is_not_historical_applicability(namespace: dict) -> None:
    mapping = fixture("temporal_mapping.v1.json")
    mapping["retrieved_at"] = "2000-01-01T00:00:00Z"
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve([mapping], namespace, known_at="2026-09-30T23:59:59Z")
    mapping["retrieved_at"] = "2099-01-01T00:00:00Z"
    assert resolve([mapping], namespace) == mapping


def test_ambiguity_rejected_even_identical_target_and_latest_revision(namespace: dict) -> None:
    old = fixture("temporal_mapping.v1.json")
    new = copy.deepcopy(old)
    new["mapping_revision"] = 2
    new["recorded_from"] = "2026-11-01T00:00:00Z"
    with pytest.raises(SchemaValidationError, match="ambiguous"):
        resolve([old, new], namespace)
    old["recorded_to"] = new["recorded_from"]
    assert resolve([old, new], namespace)["mapping_revision"] == 2
    assert resolve([old, new], namespace, known_at="2026-10-31T23:59:59Z")["mapping_revision"] == 1


def test_source_context_and_empty_intervals_rejected(namespace: dict) -> None:
    mapping = fixture("temporal_mapping.v1.json")
    mapping["source_context"]["board"] = "OTHER"
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve([mapping], namespace)
    mapping["valid_to"] = mapping["valid_from"]
    with pytest.raises(SchemaValidationError, match="interval"):
        resolve([mapping], namespace)


def test_rolling_symbol_cannot_alias_a_dated_instrument(cf: dict, namespace: dict) -> None:
    mapping = fixture("temporal_mapping.v1.json")
    mapping["source_symbol"] = "FUT_SBER"
    mapping["target"]["instrument_id"] = "SRZ6@MOEX"
    with pytest.raises(SchemaValidationError):
        resolve([mapping], namespace, source_symbol="FUT_SBER")
    mapping["target"] = {"kind": "continuous_front", "definition": cf["definition"]}
    assert (
        resolve([mapping], namespace, source_symbol="FUT_SBER", target_kind="continuous_front")[
            "target"
        ]["definition"]["instrument_id"]
        is None
    )


def test_index_membership_preserves_component_asset(namespace: dict) -> None:
    mapping = fixture("temporal_mapping.v1.json")
    mapping.update(
        relation="membership",
        component_asset_id="SBER",
        target={"kind": "instrument", "instrument_id": "IMOEX@MOEX"},
    )
    resolved = resolve([mapping], namespace, relation="membership")
    assert resolved["component_asset_id"] == "SBER"
    assert namespace["instruments"][0]["asset_id"] == "SBER"
    mapping["component_asset_id"] = "IMOEX"
    with pytest.raises(SchemaValidationError):
        resolve([mapping], namespace, relation="membership")


def test_research_versions_extend_key_but_materialization_does_not(
    cf: dict, namespace: dict
) -> None:
    market_key = identity.logical_key(cf, namespace)
    cf["materialization"] = {
        "snapshot_reference": "fixture:1",
        "delta_references": [{"table_uri": "fixture://delta", "version": 1}],
        "produced_at": "2026-11-02T12:00:00Z",
    }
    assert identity.logical_key(cf, namespace) == market_key
    cf["research"] = {
        "dataset": "fixture_dataset",
        "contour": "research",
        "set_versions": [{"name": "indicators", "version": "1"}],
        "profile_versions": [{"name": "session", "version": "1"}],
    }
    before = identity.logical_key(cf, namespace)
    assert before != market_key
    cf["research"]["profile_versions"][0]["version"] = "2"
    assert identity.logical_key(cf, namespace) != before


def test_eligibility_set_order_is_not_identity(cf: dict, namespace: dict) -> None:
    other = {"venue": "OTHER", "market": "futures", "board": "OTHER_BOARD"}
    cf["definition"]["scope"]["eligibility"].append(other)
    before = identity.logical_key(cf, namespace)
    cf["definition"]["scope"]["eligibility"].reverse()
    assert identity.logical_key(cf, namespace) == before
    cf["definition"]["scope"]["eligibility"].append(other)
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


def test_equal_eligibility_projections_do_not_mean_equal_combinations(
    cf: dict, namespace: dict
) -> None:
    first = cf["definition"]
    first["scope"]["eligibility"].extend(
        [
            {"venue": "A", "market": "futures", "board": "BOARD_A"},
            {"venue": "B", "market": "futures", "board": "BOARD_B"},
        ]
    )
    second = copy.deepcopy(first)
    second["scope"]["eligibility"][1]["board"] = "BOARD_B"
    second["scope"]["eligibility"][2]["board"] = "BOARD_A"
    assert identity.definition_key(first, namespace) != identity.definition_key(second, namespace)


def test_cf_fk_requires_complete_eligible_tuple(cf: dict, namespace: dict) -> None:
    cf["definition"]["scope"]["eligibility"].append(
        {"venue": "OTHER", "market": "futures", "board": "OTHER_BOARD"}
    )
    # Every field occurs in an allowed tuple, but this combination never does.
    namespace["instruments"][2]["board"] = "OTHER_BOARD"
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


def test_active_fk_requires_matching_applicability(cf: dict, namespace: dict) -> None:
    cf["active_applicability"] = None
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


def test_schema_assertion_drift_rejected_in_unchosen_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, namespace: dict
) -> None:
    for name in identity.CONTRACTS:
        shutil.copyfile(SCHEMAS / name, tmp_path / name)
    authority = tmp_path / identity.AUTHORITY
    schema = json.loads(authority.read_text(encoding="utf-8"))
    schema["$defs"]["definition"]["allOf"][1]["then"]["unsupportedAssertion"] = True
    authority.write_text(json.dumps(schema), encoding="utf-8")
    monkeypatch.setattr(identity, "SCHEMAS", tmp_path)
    with pytest.raises(SchemaValidationError, match="unsupported schema"):
        validate(fixture("canonical_bar.v3.json"), namespace)


def test_conditional_cf_lineage_minimum_is_executable(cf: dict, namespace: dict) -> None:
    cf["components"] = []
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


def test_schema_object_additional_properties_is_not_silently_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, namespace: dict
) -> None:
    for name in identity.CONTRACTS:
        shutil.copyfile(SCHEMAS / name, tmp_path / name)
    authority = tmp_path / identity.AUTHORITY
    schema = json.loads(authority.read_text(encoding="utf-8"))
    schema["$defs"]["q"]["additionalProperties"] = {"type": "integer"}
    authority.write_text(json.dumps(schema), encoding="utf-8")
    monkeypatch.setattr(identity, "SCHEMAS", tmp_path)
    payload = fixture("canonical_bar.v3.json")
    payload["definition"]["q"]["extra"] = "not an integer"
    with pytest.raises(SchemaValidationError, match="unsupported"):
        validate(payload, namespace)


@pytest.mark.parametrize(
    "field,assertion,value", [("source", {"minLength": 4}, ""), ("price_basis", {"minimum": 4}, 1)]
)
def test_schema_primitive_assertions_execute_without_explicit_type(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    namespace: dict,
    field: str,
    assertion: dict,
    value: object,
) -> None:
    for name in identity.CONTRACTS:
        shutil.copyfile(SCHEMAS / name, tmp_path / name)
    authority = tmp_path / identity.AUTHORITY
    schema = json.loads(authority.read_text(encoding="utf-8"))
    schema["$defs"]["q"]["properties"][field] = assertion
    authority.write_text(json.dumps(schema), encoding="utf-8")
    monkeypatch.setattr(identity, "SCHEMAS", tmp_path)
    payload = fixture("canonical_bar.v3.json")
    payload["definition"]["q"][field] = value
    with pytest.raises(SchemaValidationError):
        validate(payload, namespace)


def test_json_boolean_cannot_impersonate_numeric_schema_version(namespace: dict) -> None:
    namespace["schema_version"] = True
    with pytest.raises(SchemaValidationError):
        validate(fixture("canonical_bar.v3.json"), namespace)


def test_equivalent_utc_timestamp_spellings_have_same_row_key(cf: dict, namespace: dict) -> None:
    before = identity.logical_key(cf, namespace)
    cf["ts_open"] = "2026-11-02T10:00:00.000000Z"
    assert identity.logical_key(cf, namespace) == before


@pytest.mark.parametrize("field", ["open", "volume"])
def test_nonfinite_numbers_rejected(cf: dict, namespace: dict, field: str) -> None:
    cf[field] = float("nan")
    with pytest.raises(SchemaValidationError):
        validate(cf, namespace)


@pytest.mark.parametrize("name", ["canonical_bar", "continuous_front_bars"])
@pytest.mark.parametrize("version", [1, 2])
def test_legacy_schema_fixture_pairs_remain_accepted(name: str, version: int) -> None:
    filename = f"{name}.v{version}.json"
    validate_schema(load_schema(SCHEMAS / filename), fixture(filename))
