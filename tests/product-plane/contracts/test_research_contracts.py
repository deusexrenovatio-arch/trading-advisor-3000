from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "product_plane" / "contracts" / "schemas"
FIXTURES = ROOT / "tests" / "product-plane" / "fixtures" / "contracts"
MANIFEST = SCHEMAS / "release_blocking_contracts.v1.yaml"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict), f"yaml payload must be object: {path}"
    return payload


def _is_type(expected: str, value: object) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    raise AssertionError(f"unsupported schema type `{expected}`")


def _assert_schema_valid(schema: dict[str, Any], value: object, *, path: str = "$") -> None:
    if "enum" in schema:
        assert value in schema["enum"], f"{path}: enum mismatch"
    if "const" in schema:
        assert value == schema["const"], f"{path}: const mismatch"

    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        assert any(_is_type(item, value) for item in schema_type), f"{path}: union type mismatch"
    elif isinstance(schema_type, str):
        assert _is_type(schema_type, value), f"{path}: expected `{schema_type}`"

    if isinstance(value, str):
        min_length = schema.get("minLength")
        if min_length is not None:
            assert len(value) >= int(min_length), f"{path}: minLength violation"

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        if minimum is not None:
            assert float(value) >= float(minimum), f"{path}: minimum violation"
        maximum = schema.get("maximum")
        if maximum is not None:
            assert float(value) <= float(maximum), f"{path}: maximum violation"

    if isinstance(value, list):
        min_items = schema.get("minItems")
        if min_items is not None:
            assert len(value) >= int(min_items), f"{path}: minItems violation"
        items_schema = schema.get("items")
        if isinstance(items_schema, dict):
            for index, item in enumerate(value):
                _assert_schema_valid(items_schema, item, path=f"{path}[{index}]")

    if isinstance(value, dict):
        properties = schema.get("properties", {})
        assert isinstance(properties, dict), f"{path}: properties must be object"
        required = schema.get("required", [])
        assert isinstance(required, list), f"{path}: required must be list"
        for key in required:
            assert key in value, f"{path}: missing required field `{key}`"

        additional_properties = schema.get("additionalProperties", True)
        for key, item in value.items():
            if key in properties:
                child_schema = properties[key]
                assert isinstance(child_schema, dict), f"{path}.{key}: child schema must be object"
                _assert_schema_valid(child_schema, item, path=f"{path}.{key}")
                continue
            if additional_properties is False:
                raise AssertionError(f"{path}: unexpected field `{key}`")
            if isinstance(additional_properties, dict):
                _assert_schema_valid(additional_properties, item, path=f"{path}.{key}")


def test_research_research_release_blocking_boundary_registered() -> None:
    manifest = _load_yaml(MANIFEST)
    boundaries = manifest.get("release_blocking_boundaries")
    assert isinstance(boundaries, list), "release_blocking_boundaries must be list"

    boundary = next(
        (
            item
            for item in boundaries
            if isinstance(item, dict) and str(item.get("boundary_id", "")).strip() == "research_strategy_governance"
        ),
        None,
    )
    assert boundary is not None, "research_strategy_governance boundary must be registered"
    contracts = boundary.get("contracts")
    assert isinstance(contracts, list) and contracts
    ids = {str(item.get("contract_id", "")).strip() for item in contracts if isinstance(item, dict)}
    assert ids == {
        "gold_feature_snapshot",
        "strategy_candidate_projection",
        "strategy_scorecard",
        "strategy_promotion_decision",
    }


def test_research_research_contract_schemas_and_fixtures_validate() -> None:
    matrix = {
        "gold_feature_snapshot.v1.json": "contracts/gold_feature_snapshot.v1.json",
        "strategy_candidate_projection.v1.json": "contracts/strategy_candidate_projection.v1.json",
        "strategy_scorecard.v1.json": "contracts/strategy_scorecard.v1.json",
        "strategy_promotion_decision.v1.json": "contracts/strategy_promotion_decision.v1.json",
    }
    for file_name, schema_id in matrix.items():
        schema = _load_json(SCHEMAS / file_name)
        fixture = _load_json(FIXTURES / file_name)
        assert schema["$id"] == schema_id
        _assert_schema_valid(schema, fixture)


def test_research_technical_indicator_schema_and_fixture_validate() -> None:
    schema = _load_json(SCHEMAS / "technical_indicator_snapshot.v1.json")
    fixture = _load_json(FIXTURES / "technical_indicator_snapshot.v1.json")
    assert schema["$id"] == "contracts/technical_indicator_snapshot.v1.json"
    _assert_schema_valid(schema, fixture)
