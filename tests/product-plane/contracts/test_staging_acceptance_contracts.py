"""Transport contract checks; these fixtures are not native route acceptance proof."""

from __future__ import annotations

import base64
import copy
import json
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator

from trading_advisor_3000.product_plane.common import staging_contracts
from trading_advisor_3000.product_plane.common.staging_contracts import (
    SCHEMA_PATH,
    canonical_bytes,
    digest,
    load_plan,
    sign_budget_approval,
    sign_receipt,
    sign_reference_authorization,
    validate_document,
    verify_budget_approval,
    verify_receipt,
    verify_reference_authorization,
)

FIXTURE = Path(__file__).parents[1] / "fixtures/contracts/staging_acceptance.v1.json"
NOW = datetime(2030, 1, 1, 12, tzinfo=UTC)


def receipt():
    return {
        "schema_version": "staging_receipt.v1",
        "task_id": "contract-test:staging",
        "stage": "acceptance",
        "candidate": load_plan(FIXTURE)["candidate"],
        "plan_sha256": digest(load_plan(FIXTURE)),
        "issued_at_utc": "2030-01-01T00:00:00Z",
        "expires_at_utc": "2030-01-02T00:00:00Z",
        "verifier_revision": "7" * 40,
        "source": {"test_fixture": True},
        "outputs": {},
        "runs": {"fixture_job": {}},
        "schedule": {},
        "changed_windows": 1,
        "qc": {},
        "consumer": {},
        "coverage": {},
        "runtime": {},
    }


def signed():
    key = Ed25519PrivateKey.generate()
    ring = {"test-only": base64.b64encode(key.public_key().public_bytes_raw()).decode()}
    return sign_receipt(receipt(), key.private_bytes_raw(), "test-only"), ring


def test_authority_accepts_fixture_and_all_envelope_definitions():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    grant = {
        "schema_version": "staging_grant.v1",
        "plan_sha256": "8" * 64,
        "root_reservation_id": "9" * 32,
        "run_id": "test-run",
        "native_entry": "test_entry",
        "output_path": "/stage/output",
        "nonce": "a" * 32,
        "deadline_utc": "2030-01-02T00:00:00Z",
    }
    envelope, _ = signed()
    for kind, value in (
        ("plan", load_plan(FIXTURE)),
        ("grant", grant),
        ("receipt", receipt()),
        ("signed_receipt", envelope),
    ):
        validator.validate(value)
        assert validate_document(kind, value) == value


@pytest.mark.parametrize("mutation", ["unknown", "zero-native", "cycle", "quota-in-data", "full"])
def test_plan_refuses_ambiguous_or_unbounded_control(mutation):
    value = load_plan(FIXTURE)
    if mutation == "unknown":
        value["allow_unbounded"] = True
    elif mutation == "zero-native":
        value["native_steps"]["test_native_entry"] = 0
    elif mutation == "cycle":
        value["jobs"]["consumer_test_job"]["upstream_job"] = "consumer_test_job"
    elif mutation == "quota-in-data":
        value["quota_home"] = value["data_root"] + "/disposable"
    else:
        value["scope"]["full_history"] = True
    with pytest.raises(ValueError):
        validate_document("plan", value)


def test_receipt_verification_is_canonical_and_returns_an_independent_payload():
    envelope, ring = signed()
    value = verify_receipt(envelope, ring, NOW)
    assert value == receipt()
    value["candidate"]["head_sha"] = "0" * 40
    assert envelope["payload"]["candidate"]["head_sha"] == "2" * 40
    assert canonical_bytes({"ю": 1, "a": 2}) == canonical_bytes({"a": 2, "ю": 1})


@pytest.mark.parametrize("mutation", ["payload", "key", "signature", "unknown-key"])
def test_receipt_rejects_tampering_and_untrusted_keys(mutation):
    envelope, ring = signed()
    if mutation == "payload":
        envelope["payload"]["candidate"]["tree_sha"] = "0" * 40
    elif mutation == "key":
        wrong = Ed25519PrivateKey.generate().public_key().public_bytes_raw()
        ring["test-only"] = base64.b64encode(wrong).decode()
    elif mutation == "signature":
        envelope["signature"] = "not-base64"
    else:
        envelope["key_id"] = "untrusted"
    with pytest.raises(ValueError, match="signature|trusted"):
        verify_receipt(envelope, ring, NOW)


@pytest.mark.parametrize(
    "now", [datetime(2029, 12, 31, tzinfo=UTC), datetime(2030, 1, 2, tzinfo=UTC)]
)
def test_receipt_requires_an_active_window(now):
    envelope, ring = signed()
    with pytest.raises(ValueError, match="active|expired"):
        verify_receipt(envelope, ring, now)


def test_canonical_transport_rejects_nonfinite_values_and_schema_rejects_noop():
    with pytest.raises(ValueError, match="finite"):
        canonical_bytes({"duration": float("nan")})
    with pytest.raises(ValueError, match="keys"):
        canonical_bytes({"source": {1: "ambiguous-key"}})
    value = copy.deepcopy(receipt())
    value["changed_windows"] = 0
    with pytest.raises(ValueError):
        validate_document("receipt", value)


def test_plan_loader_rejects_duplicate_json_fields(tmp_path):
    path = tmp_path / "duplicate.json"
    path.write_text('{"task_id":"first","task_id":"second"}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        load_plan(path)


def test_reattestation_origin_requires_one_original_signed_envelope():
    original, _ = signed()
    value = receipt()
    value["compute_origin"] = {"receipt_sha256": digest(original), "signed_receipt": original}
    assert validate_document("receipt", value) == value
    nested = copy.deepcopy(original)
    nested["payload"]["compute_origin"] = copy.deepcopy(value["compute_origin"])
    value["compute_origin"]["signed_receipt"] = nested
    with pytest.raises(ValueError):
        validate_document("receipt", value)


def test_budget_extension_is_operator_signed_and_durable():
    key = Ed25519PrivateKey.generate()
    ring = {"operator": base64.b64encode(key.public_key().public_bytes_raw()).decode()}
    value = {
        "schema_version": "staging_budget_approval.v1",
        "task_id": "contract-test:staging",
        "authorization_id": "operator-approved-1",
        "reason": "Diagnosed failure; one bounded retry",
        "previous_policy_sha256": "a" * 64,
        "new_limits": {"small": 2, "acceptance": 2, "rebuild": 0},
        "issued_at_utc": "2030-01-01T00:00:00Z",
    }
    signed_value = sign_budget_approval(value, key.private_bytes_raw(), "operator")
    assert verify_budget_approval(signed_value, ring, datetime(2035, 1, 1, tzinfo=UTC)) == value
    with pytest.raises(ValueError, match="active"):
        verify_budget_approval(signed_value, ring, datetime(2029, 1, 1, tzinfo=UTC))
    with pytest.raises(ValueError):
        verify_receipt(signed_value, ring, NOW)
    signed_value["payload"]["reason"] = "unsigned change"
    with pytest.raises(ValueError, match="signature"):
        verify_budget_approval(signed_value, ring, NOW)


def test_reference_plan_is_explicit_and_cannot_change_default_candidate_mode():
    value = load_plan(FIXTURE)
    assert "mode" not in value
    value["mode"] = "reference"
    with pytest.raises(ValueError):
        validate_document("plan", value)
    original, _ = signed()
    value["reference"] = {"main_sha": "a" * 40, "receipt": original, "timezone": "Europe/Moscow"}
    assert validate_document("plan", value) == value
    for change in ({"mode": "candidate"}, {"require_scheduled": False}):
        with pytest.raises(ValueError):
            validate_document("plan", {**value, **change})


def test_reference_authorization_is_signed_time_bound_and_domain_separated():
    key = Ed25519PrivateKey.generate()
    ring = {"operator": base64.b64encode(key.public_key().public_bytes_raw()).decode()}
    value = {
        "schema_version": "staging_reference_authorization.v1",
        "task_id": "contract-test:staging",
        "plan_sha256": "a" * 64,
        "main_sha": "b" * 40,
        "receipt_sha256": "c" * 64,
        "authorization_id": "accepted-main-1",
        "reason": "Continue the accepted main reference",
        "issued_at_utc": "2030-01-01T00:00:00Z",
    }
    envelope = sign_reference_authorization(value, key.private_bytes_raw(), "operator")
    assert verify_reference_authorization(envelope, ring, NOW) == value
    assert verify_reference_authorization(envelope, ring, datetime(2035, 1, 1, tzinfo=UTC)) == value
    with pytest.raises(ValueError, match="active"):
        verify_reference_authorization(envelope, ring, datetime(2029, 1, 1, tzinfo=UTC))
    with pytest.raises(ValueError):
        verify_budget_approval(envelope, ring, NOW)
    envelope["payload"]["main_sha"] = "d" * 40
    with pytest.raises(ValueError, match="signature"):
        verify_reference_authorization(envelope, ring, NOW)


def test_plan_can_bind_producer_parameters_as_named_objects():
    value = load_plan(FIXTURE)
    value["producer_parameters"] = {"test_native_entry": {"timeframes": ["1d"]}}
    assert validate_document("plan", value) == value
    value["producer_parameters"]["test_native_entry"] = ["unbound-parameters"]
    with pytest.raises(ValueError):
        validate_document("plan", value)


@pytest.mark.parametrize("invalid", ["backwards", "after-deadline", "local-time"])
def test_protected_execution_window_is_utc_and_within_deadline(invalid):
    value = load_plan(FIXTURE)
    value["execution_window"] = {
        "start_utc": "2030-01-01T12:00:00Z",
        "end_utc": "2030-01-01T13:00:00Z",
    }
    assert validate_document("plan", value) == value
    if invalid == "backwards":
        value["execution_window"]["end_utc"] = "2030-01-01T12:00:00Z"
    elif invalid == "after-deadline":
        value["execution_window"]["end_utc"] = "2030-01-03T00:00:00Z"
    else:
        value["execution_window"]["start_utc"] = "2030-01-01T12:00:00+03:00"
    with pytest.raises(ValueError):
        validate_document("plan", value)


def test_staging_dependencies_are_opt_in_and_schema_is_packaged():
    config = tomllib.loads(
        (Path(__file__).parents[3] / "pyproject.toml").read_text(encoding="utf-8")
    )
    extras = config["project"]["optional-dependencies"]
    names = ("cryptography", "jsonschema", "dagster-postgres")
    for name in names:
        assert any(dependency.startswith(name) for dependency in extras["staging-control"])
        assert not any(dependency.startswith(name) for dependency in extras["data-proof"])
        assert not any(dependency.startswith(name) for dependency in extras["dev"])
    assert (
        "product_plane/contracts/schemas/staging_acceptance.v1.json"
        in config["tool"]["setuptools"]["package-data"]["trading_advisor_3000"]
    )


def test_daily_execution_window_is_additive_and_mutually_exclusive():
    value = load_plan(FIXTURE)
    value["daily_execution_window_utc"] = {"start": "22:00", "end": "02:00"}
    assert validate_document("plan", value) == value
    value["execution_window"] = {
        "start_utc": "2030-01-01T12:00:00Z",
        "end_utc": "2030-01-01T13:00:00Z",
    }
    with pytest.raises(ValueError):
        validate_document("plan", value)


@pytest.mark.parametrize(
    "start", ["24:00", "0:00", "23:60", "10:00:00", "10:00Z", " 10:00", "10:00\n", "02:00"]
)
def test_daily_execution_window_rejects_invalid_or_equal_clock_bounds(start):
    value = load_plan(FIXTURE)
    value["daily_execution_window_utc"] = {"start": start, "end": "02:00"}
    with pytest.raises(ValueError):
        validate_document("plan", value)


@pytest.mark.parametrize(
    "start,end,instant,expected",
    [
        ("06:00", "08:00", "2030-01-01T05:59:59.999999Z", None),
        ("06:00", "08:00", "2030-01-01T06:00:00Z", "2030-01-01T08:00:00Z"),
        ("06:00", "08:00", "2030-01-01T07:59:59.999999Z", "2030-01-01T08:00:00Z"),
        ("06:00", "08:00", "2030-01-01T08:00:00Z", None),
        ("22:00", "02:00", "2030-12-31T22:00:00Z", "2031-01-01T02:00:00Z"),
        ("22:00", "02:00", "2031-01-01T01:59:59.999999Z", "2031-01-01T02:00:00Z"),
        ("22:00", "02:00", "2031-01-01T02:00:00Z", None),
        ("22:00", "02:00", "2031-01-01T12:00:00Z", None),
        ("22:00", "02:00", "2031-01-01T02:30:00+03:00", "2031-01-01T02:00:00Z"),
    ],
)
def test_execution_window_end_uses_utc_and_half_open_daily_bounds(start, end, instant, expected):
    plan = {"daily_execution_window_utc": {"start": start, "end": end}}
    observed = datetime.fromisoformat(instant.replace("Z", "+00:00"))
    result = staging_contracts.execution_window_end(plan, observed)
    assert result == (datetime.fromisoformat(expected.replace("Z", "+00:00")) if expected else None)
    assert result is None or result.tzinfo is UTC


def test_execution_window_end_preserves_absolute_missing_and_separate_deadline():
    assert staging_contracts.execution_window_end({}, NOW) is None
    absolute = {
        "execution_window": {"start_utc": "2030-01-01T12:00:00Z", "end_utc": "2030-01-01T13:00:00Z"}
    }
    assert staging_contracts.execution_window_end(absolute, NOW) == datetime(
        2030, 1, 1, 13, tzinfo=UTC
    )
    assert (
        staging_contracts.execution_window_end(absolute, datetime(2030, 1, 1, 13, tzinfo=UTC))
        is None
    )
    with pytest.raises(ValueError, match="aware"):
        staging_contracts.execution_window_end({}, NOW.replace(tzinfo=None))
    plan = {
        "deadline_utc": "2029-01-01T00:00:00Z",
        "daily_execution_window_utc": {"start": "12:00", "end": "13:00"},
    }
    assert staging_contracts.execution_window_end(plan, NOW) == datetime(2030, 1, 1, 13, tzinfo=UTC)
    with pytest.raises(ValueError):
        staging_contracts.execution_window_end({**plan, **absolute}, NOW)
