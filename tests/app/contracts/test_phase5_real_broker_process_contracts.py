from __future__ import annotations

import json
from pathlib import Path

import yaml

from tests.app.contracts.test_phase3_contract_freeze import _assert_schema_valid


ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "app" / "contracts" / "schemas"
FIXTURES = ROOT / "tests" / "app" / "fixtures" / "contracts"
CONFIG_PROFILE = ROOT / "configs" / "broker_staging_connector_profile.v1.json"
MANIFEST_PATH = SCHEMAS / "release_blocking_contracts.v1.yaml"
CONTRACT_SURFACES_PATH = ROOT / "docs" / "architecture" / "app" / "CONTRACT_SURFACES.md"


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict), f"expected JSON object: {path}"
    return payload


def test_phase5_connector_profile_schema_and_fixture_match() -> None:
    schema = _load_json(SCHEMAS / "broker_staging_connector_profile.v1.json")
    fixture = _load_json(FIXTURES / "broker_staging_connector_profile.v1.json")
    _assert_schema_valid(schema, fixture)

    config_payload = _load_json(CONFIG_PROFILE)
    _assert_schema_valid(schema, config_payload)


def test_phase5_real_broker_report_schema_and_fixture_match() -> None:
    schema = _load_json(SCHEMAS / "real_broker_process_report.v1.json")
    fixture = _load_json(FIXTURES / "real_broker_process_report.v1.json")
    _assert_schema_valid(schema, fixture)


def test_phase5_release_blocking_manifest_references_new_contracts() -> None:
    manifest = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert isinstance(manifest, dict)
    boundaries = manifest.get("release_blocking_boundaries")
    assert isinstance(boundaries, list)

    rollout_boundary = None
    for item in boundaries:
        if isinstance(item, dict) and str(item.get("boundary_id", "")).strip() == "rollout_and_connectivity":
            rollout_boundary = item
            break
    assert isinstance(rollout_boundary, dict), "rollout_and_connectivity boundary must exist"
    contracts = rollout_boundary.get("contracts")
    assert isinstance(contracts, list)
    contract_ids = {str(item.get("contract_id", "")).strip() for item in contracts if isinstance(item, dict)}

    assert "broker_staging_connector_profile" in contract_ids
    assert "real_broker_process_report" in contract_ids


def test_phase5_contract_surfaces_docs_include_connector_profile_and_report() -> None:
    text = CONTRACT_SURFACES_PATH.read_text(encoding="utf-8")
    assert "broker_staging_connector_profile.v1" in text
    assert "real_broker_process_report.v1" in text
