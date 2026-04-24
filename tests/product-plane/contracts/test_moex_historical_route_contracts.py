from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import (
    acquire_technical_route_lease,
    build_parity_manifest_v1,
    build_raw_ingest_run_report_v2,
    heartbeat_technical_route_lease,
    record_technical_route_blocked_conflict,
    read_technical_route_run_ledger,
    release_technical_route_lease,
    takeover_technical_route_lease,
)


ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "product_plane" / "contracts" / "schemas"
FIXTURES = ROOT / "tests" / "product-plane" / "fixtures" / "contracts"
MANIFEST_PATH = SCHEMAS / "release_blocking_contracts.v1.yaml"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict), f"expected object payload: {path.as_posix()}"
    return payload


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict), f"expected object payload: {path.as_posix()}"
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
        max_length = schema.get("maxLength")
        if max_length is not None:
            assert isinstance(max_length, int), f"{path}: maxLength must be integer"
            assert len(str(value)) <= max_length, f"{path}: string longer than maxLength={max_length}"
        pattern = schema.get("pattern")
        if pattern is not None:
            assert isinstance(pattern, str), f"{path}: pattern must be string"
            assert re.fullmatch(pattern, str(value)), f"{path}: value does not match pattern {pattern!r}"
        return

    if resolved_type in {"integer", "number"}:
        minimum = schema.get("minimum")
        if minimum is not None:
            assert float(value) >= float(minimum), f"{path}: {value} < minimum {minimum}"
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


def _moex_handoff_contract_ids() -> set[str]:
    manifest = _load_yaml(MANIFEST_PATH)
    boundaries = manifest.get("release_blocking_boundaries")
    assert isinstance(boundaries, list), "release_blocking_boundaries must be list"
    for row in boundaries:
        if not isinstance(row, dict):
            continue
        if str(row.get("boundary_id", "")).strip() != "moex_historical_handoff_contracts":
            continue
        contracts = row.get("contracts")
        assert isinstance(contracts, list), "contracts must be list"
        return {
            str(item.get("contract_id", "")).strip()
            for item in contracts
            if isinstance(item, dict)
        }
    raise AssertionError("moex_historical_handoff_contracts boundary must exist")


def test_raw_route_manifest_registers_handoff_contract_boundary() -> None:
    contract_ids = _moex_handoff_contract_ids()
    expected = {
        "raw_ingest_run_report",
        "parity_manifest",
        "technical_route_run_ledger",
        "technical_route_lease_acquire_request",
        "technical_route_lease_acquire_response",
        "technical_route_lease_heartbeat_request",
        "technical_route_lease_heartbeat_response",
        "technical_route_lease_takeover_request",
        "technical_route_lease_takeover_response",
        "technical_route_lease_release_request",
        "technical_route_lease_release_response",
        "technical_route_lease_record_blocked_conflict_request",
        "technical_route_lease_record_blocked_conflict_response",
    }
    assert expected <= contract_ids


def test_raw_route_schema_fixtures_validate() -> None:
    pairs = {
        "raw_ingest_run_report.v2.json": "raw_ingest_run_report.v2.json",
        "parity_manifest.v1.json": "parity_manifest.v1.json",
        "technical_route_run_ledger.v1.json": "technical_route_run_ledger.v1.json",
        "technical_route_lease_acquire_request.v1.json": "technical_route_lease_acquire_request.v1.json",
        "technical_route_lease_acquire_response.v1.json": "technical_route_lease_acquire_response.v1.json",
        "technical_route_lease_heartbeat_request.v1.json": "technical_route_lease_heartbeat_request.v1.json",
        "technical_route_lease_heartbeat_response.v1.json": "technical_route_lease_heartbeat_response.v1.json",
        "technical_route_lease_takeover_request.v1.json": "technical_route_lease_takeover_request.v1.json",
        "technical_route_lease_takeover_response.v1.json": "technical_route_lease_takeover_response.v1.json",
        "technical_route_lease_release_request.v1.json": "technical_route_lease_release_request.v1.json",
        "technical_route_lease_release_response.v1.json": "technical_route_lease_release_response.v1.json",
        "technical_route_lease_record_blocked_conflict_request.v1.json": "technical_route_lease_record_blocked_conflict_request.v1.json",
        "technical_route_lease_record_blocked_conflict_response.v1.json": "technical_route_lease_record_blocked_conflict_response.v1.json",
    }
    for schema_name, fixture_name in pairs.items():
        schema = _load_json(SCHEMAS / schema_name)
        fixture = _load_json(FIXTURES / fixture_name)
        _assert_schema_valid(schema, fixture)
        assert schema["$id"] == f"contracts/{schema_name}"


def test_raw_route_raw_ingest_report_status_semantics_are_unambiguous() -> None:
    changed_window = [
        {
            "internal_id": "BR-6.26",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6",
            "window_start_utc": "2026-04-01T10:00:00Z",
            "window_end_utc": "2026-04-01T10:19:59Z",
            "incremental_rows": 2,
        }
    ]
    pass_report = build_raw_ingest_run_report_v2(
        run_id="phase01-pass1",
        ingest_till_utc="2026-04-01T10:30:00Z",
        source_rows=3,
        incremental_rows=2,
        deduplicated_rows=0,
        stale_rows=1,
        watermark_by_key={"BR-6.26|1m|BRM6": "2026-04-01T10:19:59Z"},
        raw_table_path="artifacts/codex/moex-raw-ingest/pass1/delta/raw_moex_history.delta",
        raw_ingest_progress_path="artifacts/codex/moex-raw-ingest/pass1/raw-ingest-progress.jsonl",
        raw_ingest_error_path="artifacts/codex/moex-raw-ingest/pass1/raw-ingest-errors.jsonl",
        raw_ingest_error_latest_path="artifacts/codex/moex-raw-ingest/pass1/raw-ingest-error.latest.json",
        changed_windows=changed_window,
        generated_at_utc="2026-04-01T10:25:00Z",
    )
    assert pass_report["status"] == "PASS"
    assert pass_report["changed_windows"]

    pass_noop_report = build_raw_ingest_run_report_v2(
        run_id="phase01-pass2",
        ingest_till_utc="2026-04-01T10:40:00Z",
        source_rows=2,
        incremental_rows=0,
        deduplicated_rows=2,
        stale_rows=0,
        watermark_by_key={"BR-6.26|1m|BRM6": "2026-04-01T10:19:59Z"},
        raw_table_path="artifacts/codex/moex-raw-ingest/pass2/delta/raw_moex_history.delta",
        raw_ingest_progress_path="artifacts/codex/moex-raw-ingest/pass2/raw-ingest-progress.jsonl",
        raw_ingest_error_path="artifacts/codex/moex-raw-ingest/pass2/raw-ingest-errors.jsonl",
        raw_ingest_error_latest_path="artifacts/codex/moex-raw-ingest/pass2/raw-ingest-error.latest.json",
        changed_windows=[],
        generated_at_utc="2026-04-01T10:41:00Z",
    )
    assert pass_noop_report["status"] == "PASS-NOOP"
    assert pass_noop_report["changed_windows"] == []

    with pytest.raises(ValueError, match="PASS-NOOP requires `changed_windows` to be empty"):
        build_raw_ingest_run_report_v2(
            run_id="phase01-pass2-invalid",
            ingest_till_utc="2026-04-01T10:40:00Z",
            source_rows=2,
            incremental_rows=0,
            deduplicated_rows=2,
            stale_rows=0,
            watermark_by_key={"BR-6.26|1m|BRM6": "2026-04-01T10:19:59Z"},
            raw_table_path="artifacts/codex/moex-raw-ingest/pass2/delta/raw_moex_history.delta",
            raw_ingest_progress_path="artifacts/codex/moex-raw-ingest/pass2/raw-ingest-progress.jsonl",
            raw_ingest_error_path="artifacts/codex/moex-raw-ingest/pass2/raw-ingest-errors.jsonl",
            raw_ingest_error_latest_path="artifacts/codex/moex-raw-ingest/pass2/raw-ingest-error.latest.json",
            changed_windows=changed_window,
            generated_at_utc="2026-04-01T10:41:00Z",
        )

    schema = _load_json(SCHEMAS / "raw_ingest_run_report.v2.json")
    status_enum = schema["properties"]["status"]["enum"]
    assert "FAILED" in status_enum
    assert "failed_condition" in pass_report["status_semantics"]


def test_raw_route_parity_manifest_is_deterministic_for_same_windows() -> None:
    raw_report = _load_json(FIXTURES / "raw_ingest_run_report.v2.json")
    raw_report["changed_windows_hash_sha256"] = ""
    changed_windows_unsorted = [
        {
            "internal_id": "Si-6.26",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "SIH6",
            "window_start_utc": "2026-04-01T10:00:00Z",
            "window_end_utc": "2026-04-01T10:19:59Z",
            "incremental_rows": 1,
        },
        {
            "internal_id": "BR-6.26",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6",
            "window_start_utc": "2026-04-01T10:00:00Z",
            "window_end_utc": "2026-04-01T10:19:59Z",
            "incremental_rows": 2,
        },
    ]
    manifest_a = build_parity_manifest_v1(
        run_id="phase01-parity-A",
        raw_ingest_run_report=raw_report,
        changed_windows=changed_windows_unsorted,
        generated_at_utc="2026-04-01T10:26:00Z",
    )
    manifest_b = build_parity_manifest_v1(
        run_id="phase01-parity-B",
        raw_ingest_run_report=raw_report,
        changed_windows=list(reversed(changed_windows_unsorted)),
        generated_at_utc="2026-04-01T10:27:00Z",
    )
    assert manifest_a["changed_windows"] == manifest_b["changed_windows"]
    assert manifest_a["changed_windows_hash_sha256"] == manifest_b["changed_windows_hash_sha256"]
    assert manifest_a["window_count"] == 2
    assert manifest_a["status"] == "PASS"


def test_raw_route_parity_manifest_rejects_invalid_changed_windows_hash() -> None:
    raw_report = _load_json(FIXTURES / "raw_ingest_run_report.v2.json")
    raw_report["changed_windows_hash_sha256"] = "BAD-HASH"

    with pytest.raises(
        ValueError,
        match="`raw_ingest_run_report.changed_windows_hash_sha256` must be 64-char lowercase sha256 hex",
    ):
        build_parity_manifest_v1(
            run_id="phase01-parity-invalid-hash",
            raw_ingest_run_report=raw_report,
            generated_at_utc="2026-04-01T10:30:00Z",
        )


def test_raw_route_lease_conflict_is_blocked_and_single_writer_state_is_preserved(tmp_path: Path) -> None:
    ledger = tmp_path / "technical-route-ledger.delta"
    acquire_a = acquire_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        requested_at_utc="2026-04-01T10:00:00Z",
        ttl_seconds=600,
        run_id="phase01-pass1",
        changed_windows_hash="1cf970cb9e4c7540f94381f54d1ff43f867ba5e56560418210fc2c50bccc9f0e",
        metadata={"purpose": "raw-ingest"},
    )
    assert acquire_a["status"] == "PASS"
    lease_token_a = str(acquire_a["lease_token"])
    assert lease_token_a

    acquire_b = acquire_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-B",
        requested_at_utc="2026-04-01T10:01:00Z",
        ttl_seconds=600,
    )
    assert acquire_b["status"] == "BLOCKED"
    assert acquire_b["reason_code"] == "lease_conflict"
    assert acquire_b["blocking_holder_id"] == "writer-A"

    acquire_a_again = acquire_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        requested_at_utc="2026-04-01T10:02:00Z",
        ttl_seconds=600,
        expected_lease_token=lease_token_a,
    )
    assert acquire_a_again["status"] == "PASS-NOOP"
    assert acquire_a_again["lease_token"] == lease_token_a

    release_wrong = release_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-B",
        lease_token=lease_token_a,
        requested_at_utc="2026-04-01T10:03:00Z",
    )
    assert release_wrong["status"] == "BLOCKED"
    assert release_wrong["reason_code"] == "lease_conflict"

    release_a = release_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        lease_token=lease_token_a,
        requested_at_utc="2026-04-01T10:04:00Z",
    )
    assert release_a["status"] == "PASS"
    assert release_a["released_at_utc"] == "2026-04-01T10:04:00Z"

    acquire_b_after_release = acquire_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-B",
        requested_at_utc="2026-04-01T10:05:00Z",
        ttl_seconds=600,
    )
    assert acquire_b_after_release["status"] == "PASS"

    ledger_rows = read_technical_route_run_ledger(ledger_table_path=ledger, route_id="moex_historical_route")
    statuses = [str(row["status"]) for row in ledger_rows]
    assert statuses == ["PASS", "BLOCKED", "PASS-NOOP", "BLOCKED", "PASS", "PASS"]


def test_raw_route_cas_lease_state_machine_supports_heartbeat_takeover_and_conflict_record(tmp_path: Path) -> None:
    ledger = tmp_path / "technical-route-ledger-cas.delta"

    acquire = acquire_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        owner_job="dagster-moex-historical-nightly",
        requested_at_utc="2026-04-01T10:00:00Z",
        ttl_seconds=60,
        run_id="phase01-pass1",
    )
    assert acquire["status"] == "PASS"
    token_a = str(acquire["lease_token"])
    version_a = int(acquire["lease_version"])

    heartbeat = heartbeat_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        owner_job="dagster-moex-historical-nightly",
        lease_token=token_a,
        requested_at_utc="2026-04-01T10:00:30Z",
        ttl_seconds=60,
        expected_lease_version=version_a,
        run_id="phase01-pass1",
    )
    assert heartbeat["status"] == "PASS"
    assert heartbeat["lease_state"] == "HEARTBEATING"
    assert int(heartbeat["lease_version"]) == version_a + 1
    version_hb = int(heartbeat["lease_version"])

    takeover_not_stale = takeover_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-B",
        owner_job="dagster-moex-historical-recovery",
        requested_at_utc="2026-04-01T10:00:40Z",
        ttl_seconds=60,
        expected_lease_version=version_hb,
        previous_lease_token=token_a,
        run_id="phase01-recovery1",
    )
    assert takeover_not_stale["status"] == "BLOCKED"
    assert takeover_not_stale["reason_code"] == "lease_not_stale"

    takeover = takeover_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-B",
        owner_job="dagster-moex-historical-recovery",
        requested_at_utc="2026-04-01T10:01:31Z",
        ttl_seconds=60,
        expected_lease_version=version_hb,
        previous_lease_token=token_a,
        run_id="phase01-recovery1",
        retry_of_run_id="phase01-pass1",
    )
    assert takeover["status"] == "PASS"
    assert takeover["lease_state"] == "TAKEN_OVER"
    assert takeover["previous_lease_owner"] == "writer-A"
    assert takeover["retry_of_run_id"] == "phase01-pass1"
    token_b = str(takeover["lease_token"])

    release_old_holder = release_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        owner_job="dagster-moex-historical-nightly",
        lease_token=token_a,
        requested_at_utc="2026-04-01T10:01:32Z",
    )
    assert release_old_holder["status"] == "BLOCKED"
    assert release_old_holder["reason_code"] == "lease_conflict"

    conflict_record = record_technical_route_blocked_conflict(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        owner_job="dagster-moex-historical-nightly",
        requested_at_utc="2026-04-01T10:01:33Z",
        reason_code="lease_conflict",
        blocking_holder_id="writer-B",
        blocking_lease_token=token_b,
        expected_lease_version=int(takeover["lease_version"]),
        run_id="phase01-pass1",
    )
    assert conflict_record["status"] == "BLOCKED"
    assert conflict_record["lease_state"] == "BLOCKED_CONFLICT"
    assert conflict_record["ledger_entry"]["event_kind"] == "LEASE_CONFLICT_BLOCKED"

    rows = read_technical_route_run_ledger(ledger_table_path=ledger, route_id="moex_historical_route")
    event_kinds = [str(row["event_kind"]) for row in rows]
    assert event_kinds == [
        "LEASE_ACQUIRE",
        "LEASE_HEARTBEAT",
        "LEASE_TAKEOVER",
        "LEASE_TAKEOVER",
        "LEASE_RELEASE",
        "LEASE_CONFLICT_BLOCKED",
    ]


def test_raw_route_lease_api_responses_match_contracts(tmp_path: Path) -> None:
    acquire_schema = _load_json(SCHEMAS / "technical_route_lease_acquire_response.v1.json")
    heartbeat_schema = _load_json(SCHEMAS / "technical_route_lease_heartbeat_response.v1.json")
    takeover_schema = _load_json(SCHEMAS / "technical_route_lease_takeover_response.v1.json")
    release_schema = _load_json(SCHEMAS / "technical_route_lease_release_response.v1.json")
    conflict_schema = _load_json(SCHEMAS / "technical_route_lease_record_blocked_conflict_response.v1.json")
    ledger_schema = _load_json(SCHEMAS / "technical_route_run_ledger.v1.json")

    ledger = tmp_path / "technical-route-ledger.delta"
    acquire = acquire_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        owner_job="dagster-moex-historical-nightly",
        requested_at_utc="2026-04-01T11:00:00Z",
        ttl_seconds=60,
        run_id="phase01-pass1",
    )
    _assert_schema_valid(acquire_schema, acquire)
    _assert_schema_valid(ledger_schema, acquire["ledger_entry"])

    heartbeat = heartbeat_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        owner_job="dagster-moex-historical-nightly",
        lease_token=str(acquire["lease_token"]),
        requested_at_utc="2026-04-01T11:00:30Z",
        ttl_seconds=60,
        expected_lease_version=int(acquire["lease_version"]),
        run_id="phase01-pass1",
    )
    _assert_schema_valid(heartbeat_schema, heartbeat)
    _assert_schema_valid(ledger_schema, heartbeat["ledger_entry"])

    takeover = takeover_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-B",
        owner_job="dagster-moex-historical-recovery",
        requested_at_utc="2026-04-01T11:01:31Z",
        ttl_seconds=60,
        expected_lease_version=int(heartbeat["lease_version"]),
        previous_lease_token=str(acquire["lease_token"]),
        run_id="phase01-recovery1",
        retry_of_run_id="phase01-pass1",
    )
    _assert_schema_valid(takeover_schema, takeover)
    _assert_schema_valid(ledger_schema, takeover["ledger_entry"])

    release = release_technical_route_lease(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-B",
        owner_job="dagster-moex-historical-recovery",
        lease_token=str(takeover["lease_token"]),
        requested_at_utc="2026-04-01T11:01:32Z",
        run_id="phase01-recovery1",
    )
    _assert_schema_valid(release_schema, release)
    _assert_schema_valid(ledger_schema, release["ledger_entry"])

    conflict_record = record_technical_route_blocked_conflict(
        ledger_table_path=ledger,
        route_id="moex_historical_route",
        holder_id="writer-A",
        owner_job="dagster-moex-historical-nightly",
        requested_at_utc="2026-04-01T11:01:33Z",
        reason_code="lease_conflict",
        blocking_holder_id="writer-B",
        blocking_lease_token=str(takeover["lease_token"]),
        expected_lease_version=int(takeover["lease_version"]) + 1,
        run_id="phase01-pass1",
    )
    _assert_schema_valid(conflict_schema, conflict_record)
    _assert_schema_valid(ledger_schema, conflict_record["ledger_entry"])


def test_raw_route_lease_api_rejects_invalid_changed_windows_hash(tmp_path: Path) -> None:
    ledger = tmp_path / "technical-route-ledger-invalid-hash.delta"

    with pytest.raises(ValueError, match="`changed_windows_hash` must be 64-char lowercase sha256 hex"):
        acquire_technical_route_lease(
            ledger_table_path=ledger,
            route_id="moex_historical_route",
            holder_id="writer-A",
            requested_at_utc="2026-04-01T11:10:00Z",
            ttl_seconds=60,
            changed_windows_hash="not-a-sha256",
        )
