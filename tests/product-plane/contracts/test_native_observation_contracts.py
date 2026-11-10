"""Synthetic shape tests; these fixtures establish no MOEX/source admission."""

import copy
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError

FIXTURES = Path(__file__).parents[1] / "fixtures" / "contracts"


def fixture(name):
    return json.loads((FIXTURES / f"native_observation_{name}.v1.json").read_bytes())


def test_snapshot_and_source_time_request_use_one_shared_namespace():
    metadata = fixture("snapshot")
    namespace = metadata["namespace"]
    validate_identity_contract("native_observation_snapshot.v1.json", metadata, namespace)
    validate_identity_contract("native_observation_request.v1.json", fixture("request"), namespace)
    fields = metadata["storage_layout"]["native_bars"]["fields"]
    assert next(f for f in fields if f["name"] == "close") == {
        "name": "close",
        "sql_type": "decimal(38,10)",
        "nullable": False,
    }


@pytest.mark.parametrize(
    "flag",
    ["canonical_Q_verified", "historical_PIT_verified", "timezone_verified", "execution_eligible"],
)
def test_observation_cannot_admit_unverified_canonical_time_or_execution(flag):
    metadata = fixture("snapshot")
    metadata["limitations"][flag] = True
    with pytest.raises(SchemaValidationError):
        validate_identity_contract(
            "native_observation_snapshot.v1.json", metadata, metadata["namespace"]
        )


def test_storage_contract_rejects_float_price_and_loosened_nullability():
    original = fixture("snapshot")
    for change in [{"sql_type": "double"}, {"nullable": True}]:
        metadata = copy.deepcopy(original)
        next(
            f for f in metadata["storage_layout"]["native_bars"]["fields"] if f["name"] == "close"
        ).update(change)
        with pytest.raises(SchemaValidationError):
            validate_identity_contract(
                "native_observation_snapshot.v1.json", metadata, metadata["namespace"]
            )


@pytest.mark.parametrize(
    "change",
    [
        {"series_type": "continuous_front"},
        {"known_at": "2020-01-01T00:00:00Z"},
        {"source_begin_from": "2020-01-01T00:00:00Z"},
        {"columns": ["close", "close"]},
        {"row_limit": 5001},
        {"instrument_id": "INSTR:absent"},
        {"namespace": "foreign"},
        {"context": {"provider": "moex_iss", "venue": "MOEX", "market": "shares", "board": "RFUD"}},
        {"engine": "futures"},
        {"source_begin_from": "2020-02-30 00:00:00"},
        {"source_begin_from": "2020-01-03 00:00:00"},
        {"source_begin_from": "2020-01-02 00:00:00"},
        {"columns": ["arbitrary_field"]},
    ],
)
def test_source_time_mode_rejects_utc_cf_pit_ambiguous_projection_and_unbounded_reads(change):
    request = fixture("request")
    request.update(change)
    with pytest.raises(SchemaValidationError):
        validate_identity_contract(
            "native_observation_request.v1.json", request, fixture("snapshot")["namespace"]
        )


def test_raw_and_quarantine_grain_distinguish_identical_bytes_from_different_requests():
    layout = fixture("snapshot")["storage_layout"]
    for name in ["raw_observations", "quarantine"]:
        assert layout[name]["grain"] == ["receipt_sha256", "source_block", "row_ordinal"]


@pytest.mark.parametrize("name", ["intent", "completion", "units", "read", "overlay"])
def test_closed_control_and_read_metadata_shapes(name):
    validate_identity_contract(
        f"native_observation_{name}.v1.json", fixture(name), fixture("snapshot")["namespace"]
    )


def test_simulation_intent_requires_explicit_parent_and_overlay():
    payload = fixture("intent")
    payload["operation"] = "simulated_correction"
    with pytest.raises(SchemaValidationError):
        validate_identity_contract(
            "native_observation_intent.v1.json", payload, fixture("snapshot")["namespace"]
        )


def test_completion_never_means_publication_and_has_exact_control_fields():
    for change in [{"status": "published"}, {"published_current": "synthetic"}]:
        payload = fixture("completion")
        payload.update(change)
        with pytest.raises(SchemaValidationError):
            validate_identity_contract(
                "native_observation_completion.v1.json", payload, fixture("snapshot")["namespace"]
            )


def test_units_unknown_must_not_carry_a_fabricated_value():
    payload = fixture("units")
    payload["quantity_unit"]["value"] = "contracts"
    with pytest.raises(SchemaValidationError):
        validate_identity_contract(
            "native_observation_units.v1.json", payload, fixture("snapshot")["namespace"]
        )
