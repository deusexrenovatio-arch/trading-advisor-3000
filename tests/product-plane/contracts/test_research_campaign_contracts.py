from __future__ import annotations

import json
from pathlib import Path

import yaml

from trading_advisor_3000.product_plane.contracts.schema_validation import load_schema, validate_schema


ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "product_plane" / "contracts" / "schemas"
FIXTURES = ROOT / "tests" / "product-plane" / "fixtures" / "contracts"
CAMPAIGNS = ROOT / "product-plane" / "campaigns"
CONTRACT_SURFACES = ROOT / "docs" / "architecture" / "product-plane" / "CONTRACT_SURFACES.md"


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict), f"expected JSON object: {path}"
    return payload


def test_research_campaign_schema_and_fixture_match() -> None:
    schema = load_schema(SCHEMAS / "research_campaign.v1.json")
    fixture = _load_json(FIXTURES / "research_campaign.v1.json")
    validate_schema(schema, fixture)


def test_research_run_summary_schema_and_fixture_match() -> None:
    schema = load_schema(SCHEMAS / "research_run_summary.v1.json")
    fixture = _load_json(FIXTURES / "research_run_summary.v1.json")
    validate_schema(schema, fixture)


def test_research_campaign_example_yaml_matches_schema() -> None:
    schema = load_schema(SCHEMAS / "research_campaign.v1.json")
    payload = yaml.safe_load((CAMPAIGNS / "fut_br_base_15m.explore.yaml").read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    validate_schema(schema, payload)


def test_research_contract_schema_snapshots_exist() -> None:
    expected = {
        "research_campaign.v1.json": "contracts/research_campaign.v1.json",
        "research_run_summary.v1.json": "contracts/research_run_summary.v1.json",
    }
    for file_name, schema_id in expected.items():
        payload = _load_json(SCHEMAS / file_name)
        assert payload["$id"] == schema_id


def test_research_run_summary_schema_allows_missing_result_digest() -> None:
    schema = load_schema(SCHEMAS / "research_run_summary.v1.json")
    fixture = _load_json(FIXTURES / "research_run_summary.v1.json")
    fixture.pop("result_digest")
    validate_schema(schema, fixture)


def test_research_orchestration_contracts_are_not_listed_as_release_blocking() -> None:
    text = CONTRACT_SURFACES.read_text(encoding="utf-8")
    release_blocking_section = text.split("## Release-Blocking Boundary Inventory", maxsplit=1)[1].split(
        "## Runtime API Inventory Scope Decision",
        maxsplit=1,
    )[0]

    assert "research_campaign.v1" not in release_blocking_section
    assert "research_run_summary.v1" not in release_blocking_section
    assert "## Internal Product Plane Orchestration Contracts" in text
