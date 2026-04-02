from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from trading_advisor_3000.product_plane.contracts import DecisionCandidate, FeatureSnapshotRef, Mode, Timeframe, TradeSide
from trading_advisor_3000.product_plane.interfaces.api import RuntimeAPI
from trading_advisor_3000.product_plane.interfaces.asgi import create_app
from trading_advisor_3000.product_plane.runtime import build_runtime_stack, read_runtime_bootstrap_config
from trading_advisor_3000.product_plane.runtime.config import StrategyVersion
from trading_advisor_3000.product_plane.runtime.ops import build_runtime_operational_snapshot
from trading_advisor_3000.product_plane.runtime.signal_store import PostgresSignalStore


ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "product_plane" / "contracts" / "schemas"
FIXTURES = ROOT / "tests" / "product-plane" / "fixtures" / "contracts"
MANIFEST_PATH = SCHEMAS / "release_blocking_contracts.v1.yaml"
CHANGE_POLICY_PATH = ROOT / "docs" / "architecture" / "product-plane" / "contract-change-policy.md"
CONTRACT_SURFACES_PATH = ROOT / "docs" / "architecture" / "product-plane" / "CONTRACT_SURFACES.md"
MIGRATION_SQL_PATH = ROOT / "src" / "trading_advisor_3000" / "migrations" / "0002_signal_runtime_state.sql"
MIGRATION_RUNNER_PATH = ROOT / "scripts" / "apply_app_migrations.py"


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
        enum_values = schema["enum"]
        assert value in enum_values, f"{path}: {value!r} is not in enum {enum_values!r}"
    if "const" in schema:
        expected_const = schema["const"]
        assert value == expected_const, f"{path}: {value!r} != const {expected_const!r}"

    schema_type = schema.get("type")
    resolved_type: str | None = None
    if isinstance(schema_type, list):
        for candidate in schema_type:
            assert isinstance(candidate, str), f"{path}: union type member must be string"
            if _is_type(candidate, value):
                resolved_type = candidate
                break
        assert resolved_type is not None, f"{path}: value does not match any type in {schema_type!r}"
    elif isinstance(schema_type, str):
        assert _is_type(schema_type, value), f"{path}: expected type `{schema_type}`, got {type(value).__name__}"
        resolved_type = schema_type
    elif schema_type is not None:
        raise AssertionError(f"{path}: unsupported type declaration {schema_type!r}")

    if resolved_type == "string":
        min_length = schema.get("minLength")
        if min_length is not None:
            assert isinstance(min_length, int), f"{path}: minLength must be integer"
            assert len(str(value)) >= min_length, f"{path}: string shorter than minLength={min_length}"
        return

    if resolved_type in {"integer", "number"}:
        minimum = schema.get("minimum")
        if minimum is not None:
            assert float(value) >= float(minimum), f"{path}: {value} < minimum {minimum}"
        exclusive_minimum = schema.get("exclusiveMinimum")
        if exclusive_minimum is not None:
            assert float(value) > float(exclusive_minimum), f"{path}: {value} <= exclusiveMinimum {exclusive_minimum}"
        maximum = schema.get("maximum")
        if maximum is not None:
            assert float(value) <= float(maximum), f"{path}: {value} > maximum {maximum}"
        return

    if resolved_type == "array":
        assert isinstance(value, list), f"{path}: expected list"
        min_items = schema.get("minItems")
        if min_items is not None:
            assert len(value) >= int(min_items), f"{path}: expected at least {min_items} items"
        items_schema = schema.get("items")
        if items_schema is not None:
            assert isinstance(items_schema, dict), f"{path}: items must be schema object"
            for index, item in enumerate(value):
                _assert_schema_valid(items_schema, item, path=f"{path}[{index}]")
        return

    if resolved_type == "object":
        assert isinstance(value, dict), f"{path}: expected object"
        properties = schema.get("properties", {})
        assert isinstance(properties, dict), f"{path}: properties must be object"
        required = schema.get("required", [])
        assert isinstance(required, list), f"{path}: required must be list"
        for key in required:
            assert key in value, f"{path}: missing required field `{key}`"

        additional_properties = schema.get("additionalProperties", True)
        for key, item in value.items():
            child_path = f"{path}.{key}"
            if key in properties:
                child_schema = properties[key]
                assert isinstance(child_schema, dict), f"{child_path}: child schema must be object"
                _assert_schema_valid(child_schema, item, path=child_path)
                continue
            if additional_properties is False:
                raise AssertionError(f"{path}: unexpected field `{key}`")
            if isinstance(additional_properties, dict):
                _assert_schema_valid(additional_properties, item, path=child_path)
        return


def _manifest_contract_rows() -> tuple[dict[str, Any], list[tuple[str, dict[str, Any]]]]:
    manifest = _load_yaml(MANIFEST_PATH)
    boundaries = manifest.get("release_blocking_boundaries")
    assert isinstance(boundaries, list), "release_blocking_boundaries must be list"
    rows: list[tuple[str, dict[str, Any]]] = []
    for boundary in boundaries:
        assert isinstance(boundary, dict), "boundary row must be object"
        boundary_id = str(boundary.get("boundary_id", "")).strip()
        assert boundary_id, "boundary_id is required"
        contracts = boundary.get("contracts")
        assert isinstance(contracts, list), f"contracts list missing for boundary `{boundary_id}`"
        for contract in contracts:
            assert isinstance(contract, dict), f"contract row must be object in boundary `{boundary_id}`"
            rows.append((boundary_id, contract))
    return manifest, rows


def _parse_prometheus_gauges(metrics_text: str, names: tuple[str, ...]) -> dict[str, int]:
    pattern = re.compile(r"^(ta3000_[a-z0-9_]+)\s+(-?\d+)$", re.MULTILINE)
    parsed: dict[str, int] = {}
    for match in pattern.finditer(metrics_text):
        metric_name = str(match.group(1)).strip()
        if metric_name not in names:
            continue
        parsed[metric_name] = int(match.group(2))
    return parsed


def _active_candidate(*, signal_id: str, ts_decision: str) -> DecisionCandidate:
    return DecisionCandidate(
        signal_id=signal_id,
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="trend-follow-v1",
        mode=Mode.SHADOW,
        side=TradeSide.LONG,
        entry_ref=82.45,
        stop_ref=81.70,
        target_ref=83.95,
        confidence=0.77,
        ts_decision=ts_decision,
        feature_snapshot=FeatureSnapshotRef(
            dataset_version="bars-whitelist-v1",
            snapshot_id="FS-20260316-0001",
        ),
    )


def _runtime_api() -> RuntimeAPI:
    stack = build_runtime_stack(telegram_channel="@ta3000_signals")
    stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id="trend-follow-v1",
            status="active",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-16T09:00:00Z",
        )
    )
    return RuntimeAPI(runtime_stack=stack)


def test_phase3_manifest_covers_release_blocking_boundaries_and_policy() -> None:
    manifest, rows = _manifest_contract_rows()
    assert rows, "release_blocking contract manifest must not be empty"

    compatibility = manifest.get("compatibility_classes")
    assert isinstance(compatibility, dict), "compatibility_classes must be object"
    assert {"strict", "additive"} <= set(compatibility), "missing required compatibility classes"

    required_boundaries = {
        "runtime_api",
        "telegram_publication_path",
        "sidecar_http_wire",
        "runtime_configuration",
        "persistence_and_migrations",
        "rollout_and_connectivity",
    }
    present_boundaries = {boundary_id for boundary_id, _ in rows}
    assert required_boundaries <= present_boundaries

    change_policy_ref = str(manifest.get("change_policy_ref", "")).strip()
    assert change_policy_ref == "docs/architecture/product-plane/contract-change-policy.md"
    assert CHANGE_POLICY_PATH.exists()
    policy_text = CHANGE_POLICY_PATH.read_text(encoding="utf-8")
    assert "schema, fixture, and tests are updated together" in policy_text


def test_phase3_manifest_declares_runtime_api_inventory_scope_decision() -> None:
    manifest, _rows = _manifest_contract_rows()
    decision = manifest.get("runtime_api_inventory_scope_decision")
    assert isinstance(decision, dict), "runtime_api_inventory_scope_decision must be object"
    assert str(decision.get("decision_id", "")).strip() == "F1-C-RUNTIME-API-INVENTORY-SCOPE-V1"
    assert str(decision.get("status", "")).strip() == "approved"
    assert str(decision.get("rationale", "")).strip()

    exclusions = decision.get("exclusions")
    assert isinstance(exclusions, list) and exclusions, "runtime API exclusions must be non-empty list"
    by_path = {
        str(item.get("path", "")).strip(): item
        for item in exclusions
        if isinstance(item, dict)
    }
    required = {"/runtime/signal-events", "/runtime/strategy-registry"}
    assert required <= set(by_path), "runtime API exclusion decision must cover both public projection endpoints"
    for path in required:
        row = by_path[path]
        assert str(row.get("reason", "")).strip(), f"missing rationale for excluded endpoint `{path}`"
        covered = row.get("covered_by_contracts")
        assert isinstance(covered, list) and covered, f"missing contract linkage for excluded endpoint `{path}`"


def test_phase3_manifest_contract_files_and_coverage_exist() -> None:
    manifest, rows = _manifest_contract_rows()
    compatibility = set((manifest.get("compatibility_classes") or {}).keys())
    for _boundary_id, contract in rows:
        schema_name = str(contract.get("schema", "")).strip()
        fixture_name = str(contract.get("fixture", "")).strip()
        compatibility_class = str(contract.get("compatibility", "")).strip()
        coverage_tests = contract.get("coverage_tests_any")

        assert schema_name, "schema is required"
        assert fixture_name, "fixture is required"
        assert compatibility_class in compatibility
        assert isinstance(coverage_tests, list) and coverage_tests, "coverage_tests_any must be non-empty list"

        schema_path = SCHEMAS / schema_name
        fixture_path = FIXTURES / fixture_name
        assert schema_path.exists(), f"missing schema file: {schema_path}"
        assert fixture_path.exists(), f"missing fixture file: {fixture_path}"

        schema_payload = _load_json(schema_path)
        assert schema_payload["$id"] == f"contracts/{schema_name}"

        for test_path_text in coverage_tests:
            test_path = ROOT / str(test_path_text)
            assert test_path.exists(), f"missing coverage test path: {test_path_text}"


def test_phase3_manifest_fixtures_validate_against_schemas() -> None:
    _manifest, rows = _manifest_contract_rows()
    for _boundary_id, contract in rows:
        schema = _load_json(SCHEMAS / str(contract["schema"]))
        fixture = _load_json(FIXTURES / str(contract["fixture"]))
        _assert_schema_valid(schema, fixture)


def test_phase3_runtime_api_responses_match_versioned_contracts() -> None:
    replay_schema = _load_json(SCHEMAS / "runtime_api_replay_candidates_response.v1.json")
    close_schema = _load_json(SCHEMAS / "runtime_api_close_signal_response.v1.json")
    cancel_schema = _load_json(SCHEMAS / "runtime_api_cancel_signal_response.v1.json")

    api = _runtime_api()
    replay_payload = api.replay_candidates(
        [_active_candidate(signal_id="SIG-20260316-0001", ts_decision="2026-03-16T10:16:00Z")]
    )
    _assert_schema_valid(replay_schema, replay_payload)

    close_payload = api.close_signal(
        signal_id="SIG-20260316-0001",
        closed_at="2026-03-16T10:30:00Z",
        reason_code="manual_close",
    )
    _assert_schema_valid(close_schema, close_payload)

    cancel_api = _runtime_api()
    cancel_api.replay_candidates(
        [_active_candidate(signal_id="SIG-20260316-0002", ts_decision="2026-03-16T10:17:00Z")]
    )
    cancel_payload = cancel_api.cancel_signal(
        signal_id="SIG-20260316-0002",
        canceled_at="2026-03-16T10:31:00Z",
        reason_code="manual_cancel",
    )
    _assert_schema_valid(cancel_schema, cancel_payload)


def test_phase3_fastapi_probe_envelopes_match_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PostgresSignalStore, "list_publication_events", lambda self: [])
    health_schema = _load_json(SCHEMAS / "runtime_api_health_response.v1.json")
    ready_schema = _load_json(SCHEMAS / "runtime_api_ready_response.v1.json")

    env = {
        "TA3000_RUNTIME_PROFILE": "staging",
        "TA3000_SIGNAL_STORE_BACKEND": "postgres",
        "TA3000_APP_DSN": "postgresql://postgres:postgres@127.0.0.1:5432/ta3000",
        "TA3000_TELEGRAM_CHANNEL": "@ta3000_runtime",
    }
    with TestClient(create_app(env=env)) as client:
        health = client.get("/health")
        ready = client.get("/ready")
    assert health.status_code == 200
    assert ready.status_code == 200
    _assert_schema_valid(health_schema, health.json())
    _assert_schema_valid(ready_schema, ready.json())


def test_phase3_runtime_bootstrap_config_matches_contract() -> None:
    schema = _load_json(SCHEMAS / "runtime_bootstrap_config.v1.json")
    config = read_runtime_bootstrap_config(
        {
            "TA3000_RUNTIME_PROFILE": "staging",
            "TA3000_SIGNAL_STORE_BACKEND": "postgres",
            "TA3000_APP_DSN": "postgresql://postgres:postgres@127.0.0.1:5432/ta3000",
            "TA3000_TELEGRAM_CHANNEL": "@ta3000_runtime",
        }
    )
    _assert_schema_valid(schema, config.to_dict())


def test_phase3_persistence_manifest_is_aligned_with_migration_and_runner() -> None:
    schema = _load_json(SCHEMAS / "runtime_signal_store_persistence_manifest.v1.json")
    fixture = _load_json(FIXTURES / "runtime_signal_store_persistence_manifest.v1.json")
    _assert_schema_valid(schema, fixture)

    sql_text = MIGRATION_SQL_PATH.read_text(encoding="utf-8").lower()
    for table in fixture["tables"]:
        table_name = str(table["name"]).lower()
        assert f"create table if not exists {table_name}" in sql_text
        for column in table["columns"]:
            assert re.search(rf"\b{re.escape(str(column).lower())}\b", sql_text), f"missing column `{column}` in migration"
        for index_name in table["indexes"]:
            assert str(index_name).lower() in sql_text

    runner_text = MIGRATION_RUNNER_PATH.read_text(encoding="utf-8").lower()
    tracking = fixture["migration_tracking_table"]
    assert str(tracking["name"]).split(".")[-1].lower() in runner_text
    for column in tracking["columns"]:
        assert str(column).lower() in runner_text


def test_phase3_runtime_operational_snapshot_matches_contract() -> None:
    schema = _load_json(SCHEMAS / "runtime_operational_snapshot.v1.json")
    payload = build_runtime_operational_snapshot(
        {
            "TA3000_ENVIRONMENT": "staging-real-transport",
            "TA3000_ENABLE_LIVE_EXECUTION": "1",
            "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
            "TA3000_ENABLE_QUIK_CONNECTOR": "1",
            "TA3000_ENABLE_FINAM_TRANSPORT": "1",
            "TA3000_ENFORCE_LIVE_SECRETS": "1",
            "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
            "TA3000_FINAM_API_TOKEN": "finam-secret-001",
        }
    )
    _assert_schema_valid(schema, payload)


def test_phase3_sidecar_metrics_envelope_matches_contract() -> None:
    schema = _load_json(SCHEMAS / "sidecar_metrics_response.v1.json")
    fixture = _load_json(FIXTURES / "sidecar_metrics_response.v1.json")
    _assert_schema_valid(schema, fixture)

    metrics_text = str(fixture["metrics_text"])
    expected_metrics = tuple(sorted(str(key) for key in fixture["parsed_metrics"].keys()))
    parsed_metrics = _parse_prometheus_gauges(metrics_text, expected_metrics)

    for metric_name in expected_metrics:
        assert metric_name in parsed_metrics, f"missing metric `{metric_name}` in metrics_text"
        expected_value = int(fixture["parsed_metrics"][metric_name])
        assert parsed_metrics[metric_name] == expected_value


def test_phase3_contract_surfaces_references_manifest_and_change_policy() -> None:
    text = CONTRACT_SURFACES_PATH.read_text(encoding="utf-8")
    assert "release_blocking_contracts.v1.yaml" in text
    assert "contract-change-policy.md" in text
    assert "Runtime API envelopes" in text
    assert "Sidecar HTTP wire envelopes" in text
    assert "sidecar_metrics_response.v1" in text
    assert "F1-C-RUNTIME-API-INVENTORY-SCOPE-V1" in text
    assert "/runtime/signal-events" in text
    assert "/runtime/strategy-registry" in text


def test_phase3_disprover_payload_mutation_without_contract_update_fails() -> None:
    schema = _load_json(SCHEMAS / "sidecar_submit_intent_response.v1.json")
    payload = _load_json(FIXTURES / "sidecar_submit_intent_response.v1.json")
    payload["ack"]["unexpected"] = "mutation-without-schema"

    with pytest.raises(AssertionError):
        _assert_schema_valid(schema, payload)
