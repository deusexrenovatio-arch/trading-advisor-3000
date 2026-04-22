from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from ..delta_runtime import append_delta_table_rows, has_delta_log, read_delta_table_rows


RAW_INGEST_RUN_REPORT_VERSION = "raw_ingest_run_report.v2"
PARITY_MANIFEST_VERSION = "parity_manifest.v1"
TECHNICAL_ROUTE_RUN_LEDGER_VERSION = "technical_route_run_ledger.v1"

STATUS_PASS = "PASS"
STATUS_PASS_NOOP = "PASS-NOOP"
STATUS_BLOCKED = "BLOCKED"
STATUS_FAILED = "FAILED"

LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE = "authoritative_store_route"
LEASE_BACKEND_DELTA_LEDGER_CAS = "delta-ledger-cas"

LEASE_EVENT_ACQUIRE = "LEASE_ACQUIRE"
LEASE_EVENT_HEARTBEAT = "LEASE_HEARTBEAT"
LEASE_EVENT_TAKEOVER = "LEASE_TAKEOVER"
LEASE_EVENT_RELEASE = "LEASE_RELEASE"
LEASE_EVENT_CONFLICT_BLOCKED = "LEASE_CONFLICT_BLOCKED"

LEASE_STATE_ACQUIRED = "ACQUIRED"
LEASE_STATE_HEARTBEATING = "HEARTBEATING"
LEASE_STATE_RELEASED = "RELEASED"
LEASE_STATE_EXPIRED = "EXPIRED"
LEASE_STATE_TAKEN_OVER = "TAKEN_OVER"
LEASE_STATE_BLOCKED_CONFLICT = "BLOCKED_CONFLICT"

_ACTIVE_LEASE_STATES = {LEASE_STATE_ACQUIRED, LEASE_STATE_HEARTBEATING, LEASE_STATE_TAKEN_OVER}
_PASS_LIKE_STATUSES = {STATUS_PASS, STATUS_PASS_NOOP}

TECHNICAL_ROUTE_LEDGER_COLUMNS: dict[str, str] = {
    "ledger_version": "string",
    "event_sequence": "bigint",
    "route_id": "string",
    "event_kind": "string",
    "status": "string",
    "status_reason": "string",
    "lease_scope": "string",
    "lease_state": "string",
    "lease_backend": "string",
    "lease_version": "bigint",
    "holder_id": "string",
    "owner_job": "string",
    "lease_token": "string",
    "expected_lease_token": "string",
    "requested_at_utc": "string",
    "heartbeat_at_utc": "string",
    "expires_at_utc": "string",
    "released_at_utc": "string",
    "blocking_holder_id": "string",
    "blocking_lease_token": "string",
    "previous_lease_owner": "string",
    "ttl_seconds": "int",
    "lease_timeout_sec": "int",
    "run_id": "string",
    "retry_of_run_id": "string",
    "changed_windows_hash": "string",
    "metadata_json": "json",
}

_SORTED_CHANGED_WINDOW_KEYS = (
    "internal_id",
    "source_timeframe",
    "source_interval",
    "moex_secid",
    "window_start_utc",
    "window_end_utc",
    "incremental_rows",
)
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def _require_non_empty_text(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"`{field_name}` must be non-empty text")
    return text


def _require_non_negative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"`{field_name}` must be integer >= 0")
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"`{field_name}` must be integer >= 0")
    return parsed


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str, *, field_name: str) -> datetime:
    text = _require_non_empty_text(value, field_name)
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:  # pragma: no cover - defensive conversion
        raise ValueError(f"`{field_name}` must be ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"`{field_name}` must include timezone")
    return parsed.astimezone(UTC)


def _to_iso_utc(value: datetime) -> str:
    normalized = value.astimezone(UTC).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _sha256_json(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalize_optional_sha256(value: object, field_name: str) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if not _SHA256_HEX_RE.fullmatch(text):
        raise ValueError(f"`{field_name}` must be 64-char lowercase sha256 hex")
    return text


def normalize_changed_windows(changed_windows: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for index, item in enumerate(changed_windows):
        if not isinstance(item, Mapping):
            raise ValueError(f"`changed_windows[{index}]` must be object")
        row = {
            "internal_id": _require_non_empty_text(item.get("internal_id"), f"changed_windows[{index}].internal_id"),
            "source_timeframe": _require_non_empty_text(
                item.get("source_timeframe"),
                f"changed_windows[{index}].source_timeframe",
            ),
            "source_interval": _require_non_negative_int(
                item.get("source_interval"),
                f"changed_windows[{index}].source_interval",
            ),
            "moex_secid": _require_non_empty_text(item.get("moex_secid"), f"changed_windows[{index}].moex_secid"),
            "window_start_utc": _to_iso_utc(
                _parse_iso_utc(
                    _require_non_empty_text(item.get("window_start_utc"), f"changed_windows[{index}].window_start_utc"),
                    field_name=f"changed_windows[{index}].window_start_utc",
                )
            ),
            "window_end_utc": _to_iso_utc(
                _parse_iso_utc(
                    _require_non_empty_text(item.get("window_end_utc"), f"changed_windows[{index}].window_end_utc"),
                    field_name=f"changed_windows[{index}].window_end_utc",
                )
            ),
            "incremental_rows": _require_non_negative_int(
                item.get("incremental_rows"),
                f"changed_windows[{index}].incremental_rows",
            ),
        }
        if row["incremental_rows"] <= 0:
            raise ValueError(f"`changed_windows[{index}].incremental_rows` must be > 0")
        if row["window_end_utc"] < row["window_start_utc"]:
            raise ValueError(
                f"`changed_windows[{index}]` has invalid window: "
                "window_end_utc must be >= window_start_utc"
            )
        normalized_rows.append(row)

    dedup: dict[tuple[str, str, int, str, str, str], dict[str, Any]] = {}
    for row in normalized_rows:
        key = (
            row["internal_id"],
            row["source_timeframe"],
            row["source_interval"],
            row["moex_secid"],
            row["window_start_utc"],
            row["window_end_utc"],
        )
        existing = dedup.get(key)
        if existing is None:
            dedup[key] = row
            continue
        existing["incremental_rows"] = int(existing["incremental_rows"]) + int(row["incremental_rows"])

    merged = list(dedup.values())
    merged.sort(
        key=lambda item: (
            str(item["internal_id"]),
            str(item["source_timeframe"]),
            str(item["moex_secid"]),
            int(item["source_interval"]),
            str(item["window_start_utc"]),
            str(item["window_end_utc"]),
        )
    )
    return merged


def derive_raw_ingest_status(*, incremental_rows: int) -> str:
    if incremental_rows < 0:
        raise ValueError("`incremental_rows` must be >= 0")
    return STATUS_PASS if incremental_rows > 0 else STATUS_PASS_NOOP


def build_raw_ingest_run_report_v2(
    *,
    run_id: str,
    ingest_till_utc: str,
    source_rows: int,
    incremental_rows: int,
    deduplicated_rows: int,
    stale_rows: int,
    watermark_by_key: Mapping[str, str],
    raw_table_path: str,
    raw_ingest_progress_path: str,
    raw_ingest_error_path: str,
    raw_ingest_error_latest_path: str,
    changed_windows: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    run_id_text = _require_non_empty_text(run_id, "run_id")
    ingest_till = _to_iso_utc(_parse_iso_utc(ingest_till_utc, field_name="ingest_till_utc"))
    generated = _to_iso_utc(
        _parse_iso_utc(generated_at_utc, field_name="generated_at_utc")
        if generated_at_utc
        else datetime.now(tz=UTC)
    )
    normalized_windows = normalize_changed_windows(list(changed_windows))

    source_rows_int = _require_non_negative_int(source_rows, "source_rows")
    incremental_rows_int = _require_non_negative_int(incremental_rows, "incremental_rows")
    deduplicated_rows_int = _require_non_negative_int(deduplicated_rows, "deduplicated_rows")
    stale_rows_int = _require_non_negative_int(stale_rows, "stale_rows")
    if source_rows_int < incremental_rows_int:
        raise ValueError("`source_rows` cannot be lower than `incremental_rows`")

    status = derive_raw_ingest_status(incremental_rows=incremental_rows_int)
    if status == STATUS_PASS_NOOP and normalized_windows:
        raise ValueError("PASS-NOOP requires `changed_windows` to be empty")
    if status == STATUS_PASS and not normalized_windows:
        raise ValueError("PASS requires non-empty `changed_windows`")

    normalized_watermarks = {
        _require_non_empty_text(key, "watermark_by_key key"): _to_iso_utc(
            _parse_iso_utc(value, field_name=f"watermark_by_key[{key}]")
        )
        for key, value in watermark_by_key.items()
    }

    report = {
        "contract_version": RAW_INGEST_RUN_REPORT_VERSION,
        "run_id": run_id_text,
        "generated_at_utc": generated,
        "ingest_till_utc": ingest_till,
        "status": status,
        "status_semantics": {
            "pass_condition": "incremental_rows > 0",
            "pass_noop_condition": "incremental_rows == 0 and changed_windows == []",
            "blocked_condition": "ingest failures must fail closed and are not downgraded to PASS statuses",
            "failed_condition": "terminal ingest runtime failures produce FAILED and block canonical launch",
        },
        "source_rows": source_rows_int,
        "incremental_rows": incremental_rows_int,
        "deduplicated_rows": deduplicated_rows_int,
        "stale_rows": stale_rows_int,
        "changed_windows": [
            {key: row[key] for key in _SORTED_CHANGED_WINDOW_KEYS}
            for row in normalized_windows
        ],
        "changed_windows_hash_sha256": _sha256_json(
            [{key: row[key] for key in _SORTED_CHANGED_WINDOW_KEYS} for row in normalized_windows]
        ),
        "watermark_by_key": normalized_watermarks,
        "raw_table_path": _require_non_empty_text(raw_table_path, "raw_table_path"),
        "raw_ingest_progress_path": _require_non_empty_text(raw_ingest_progress_path, "raw_ingest_progress_path"),
        "raw_ingest_error_path": _require_non_empty_text(raw_ingest_error_path, "raw_ingest_error_path"),
        "raw_ingest_error_latest_path": _require_non_empty_text(raw_ingest_error_latest_path, "raw_ingest_error_latest_path"),
    }
    return report


def build_parity_manifest_v1(
    *,
    run_id: str,
    raw_ingest_run_report: Mapping[str, Any],
    changed_windows: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...] | None = None,
    generated_at_utc: str | None = None,
    window_policy_id: str = "fixed-proof-window.v1",
) -> dict[str, Any]:
    run_id_text = _require_non_empty_text(run_id, "run_id")
    report_contract = _require_non_empty_text(raw_ingest_run_report.get("contract_version"), "raw_ingest_run_report.contract_version")
    report_run_id = _require_non_empty_text(raw_ingest_run_report.get("run_id"), "raw_ingest_run_report.run_id")
    source_windows = changed_windows if changed_windows is not None else raw_ingest_run_report.get("changed_windows", [])
    if not isinstance(source_windows, (list, tuple)):
        raise ValueError("`changed_windows` must be list-like")
    normalized_windows = normalize_changed_windows(list(source_windows))
    window_hash = _sha256_json(
        [{key: row[key] for key in _SORTED_CHANGED_WINDOW_KEYS} for row in normalized_windows]
    )
    report_hash = _normalize_optional_sha256(
        raw_ingest_run_report.get("changed_windows_hash_sha256", ""),
        "raw_ingest_run_report.changed_windows_hash_sha256",
    )
    if report_hash and report_hash != window_hash:
        raise ValueError("`raw_ingest_run_report.changed_windows_hash_sha256` does not match deterministic hash rules")

    status = STATUS_PASS if normalized_windows else STATUS_PASS_NOOP
    generated = _to_iso_utc(
        _parse_iso_utc(generated_at_utc, field_name="generated_at_utc")
        if generated_at_utc
        else datetime.now(tz=UTC)
    )

    manifest = {
        "contract_version": PARITY_MANIFEST_VERSION,
        "run_id": run_id_text,
        "generated_at_utc": generated,
        "status": status,
        "window_policy_id": _require_non_empty_text(window_policy_id, "window_policy_id"),
        "raw_ingest_report_contract": report_contract,
        "raw_ingest_run_id": report_run_id,
        "window_count": len(normalized_windows),
        "changed_windows": [
            {key: row[key] for key in _SORTED_CHANGED_WINDOW_KEYS}
            for row in normalized_windows
        ],
        "changed_windows_hash_sha256": window_hash,
        "deterministic_rules": {
            "sort_order": "internal_id|source_timeframe|moex_secid|source_interval|window_start_utc|window_end_utc",
            "hash_rule": "sha256(canonical-json(changed_windows))",
            "pass_noop_rule": "window_count == 0",
        },
    }
    return manifest


def _load_technical_route_ledger_rows(ledger_table_path: Path) -> list[dict[str, Any]]:
    if not has_delta_log(ledger_table_path):
        return []
    rows = read_delta_table_rows(ledger_table_path)
    return [row for row in rows if isinstance(row, dict)]


def _next_event_sequence(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 1
    return max(int(row.get("event_sequence", 0) or 0) for row in rows) + 1


def _sorted_ledger_rows(
    *,
    rows: list[dict[str, Any]],
    route_id: str | None = None,
    lease_scope: str | None = None,
) -> list[dict[str, Any]]:
    filtered = rows
    if route_id is not None:
        filtered = [row for row in filtered if str(row.get("route_id", "")).strip() == route_id]
    if lease_scope is not None:
        filtered = [row for row in filtered if str(row.get("lease_scope", "")).strip() in {"", lease_scope}]
    return sorted(
        filtered,
        key=lambda row: (
            int(row.get("event_sequence", 0) or 0),
            str(row.get("requested_at_utc", "")),
        ),
    )


def _normalize_lease_scope(value: object) -> str:
    lease_scope = _require_non_empty_text(value, "lease_scope")
    if lease_scope != LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE:
        raise ValueError(
            "`lease_scope` must use canonical backend scope "
            f"`{LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE}`"
        )
    return lease_scope


def _normalize_lease_backend(value: object) -> str:
    lease_backend = _require_non_empty_text(value, "lease_backend")
    if lease_backend != LEASE_BACKEND_DELTA_LEDGER_CAS:
        raise ValueError(
            "`lease_backend` must use canonical CAS backend "
            f"`{LEASE_BACKEND_DELTA_LEDGER_CAS}`"
        )
    return lease_backend


def _derive_lease_state(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("lease_state", "")).strip()
    if explicit:
        return explicit
    event_kind = str(row.get("event_kind", "")).strip()
    if event_kind == LEASE_EVENT_ACQUIRE:
        return LEASE_STATE_ACQUIRED
    if event_kind == LEASE_EVENT_HEARTBEAT:
        return LEASE_STATE_HEARTBEATING
    if event_kind == LEASE_EVENT_TAKEOVER:
        return LEASE_STATE_TAKEN_OVER
    if event_kind == LEASE_EVENT_RELEASE:
        return LEASE_STATE_RELEASED
    if event_kind == LEASE_EVENT_CONFLICT_BLOCKED:
        return LEASE_STATE_BLOCKED_CONFLICT
    return ""


def _latest_successful_row(
    *,
    rows: list[dict[str, Any]],
    route_id: str,
    lease_scope: str,
) -> dict[str, Any] | None:
    successful_rows = [
        row
        for row in _sorted_ledger_rows(rows=rows, route_id=route_id, lease_scope=lease_scope)
        if str(row.get("status", "")).strip() in _PASS_LIKE_STATUSES
    ]
    if not successful_rows:
        return None
    return successful_rows[-1]


def _lease_expires_before_or_at(*, row: Mapping[str, Any], as_of: datetime) -> bool:
    expires_at_raw = str(row.get("expires_at_utc", "")).strip()
    if not expires_at_raw:
        return False
    expires_at = _parse_iso_utc(expires_at_raw, field_name="expires_at_utc")
    return expires_at <= as_of


def _read_lease_version(row: Mapping[str, Any]) -> int:
    version = int(row.get("lease_version", 0) or 0)
    if version <= 0:
        return 1
    return version


def _max_lease_version(*, rows: list[dict[str, Any]], route_id: str, lease_scope: str) -> int:
    route_rows = _sorted_ledger_rows(rows=rows, route_id=route_id, lease_scope=lease_scope)
    if not route_rows:
        return 0
    return max(_read_lease_version(row) for row in route_rows)


def _find_active_lease(
    *,
    rows: list[dict[str, Any]],
    route_id: str,
    lease_scope: str,
    as_of: datetime,
) -> dict[str, Any] | None:
    latest = _latest_successful_row(rows=rows, route_id=route_id, lease_scope=lease_scope)
    if latest is None:
        return None
    lease_state = _derive_lease_state(latest)
    if lease_state not in _ACTIVE_LEASE_STATES:
        return None
    if _lease_expires_before_or_at(row=latest, as_of=as_of):
        return None
    return latest


def _build_ledger_row(
    *,
    event_sequence: int,
    route_id: str,
    event_kind: str,
    status: str,
    status_reason: str,
    lease_scope: str,
    lease_state: str,
    lease_backend: str,
    lease_version: int,
    holder_id: str,
    owner_job: str,
    lease_token: str | None,
    expected_lease_token: str | None,
    requested_at_utc: str,
    heartbeat_at_utc: str | None,
    expires_at_utc: str | None,
    released_at_utc: str | None,
    blocking_holder_id: str | None,
    blocking_lease_token: str | None,
    previous_lease_owner: str | None,
    ttl_seconds: int,
    lease_timeout_sec: int,
    run_id: str | None,
    retry_of_run_id: str | None,
    changed_windows_hash: str | None,
    metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "ledger_version": TECHNICAL_ROUTE_RUN_LEDGER_VERSION,
        "event_sequence": event_sequence,
        "route_id": route_id,
        "event_kind": event_kind,
        "status": status,
        "status_reason": status_reason,
        "lease_scope": lease_scope,
        "lease_state": lease_state,
        "lease_backend": lease_backend,
        "lease_version": max(1, int(lease_version)),
        "holder_id": holder_id,
        "owner_job": owner_job or None,
        "lease_token": lease_token or None,
        "expected_lease_token": expected_lease_token or None,
        "requested_at_utc": requested_at_utc,
        "heartbeat_at_utc": heartbeat_at_utc,
        "expires_at_utc": expires_at_utc,
        "released_at_utc": released_at_utc,
        "blocking_holder_id": blocking_holder_id,
        "blocking_lease_token": blocking_lease_token,
        "previous_lease_owner": previous_lease_owner,
        "ttl_seconds": max(0, int(ttl_seconds)),
        "lease_timeout_sec": max(0, int(lease_timeout_sec)),
        "run_id": run_id or None,
        "retry_of_run_id": retry_of_run_id,
        "changed_windows_hash": _normalize_optional_sha256(changed_windows_hash, "changed_windows_hash"),
        "metadata_json": dict(metadata or {}),
    }


def _build_lease_response(contract_version: str, row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "contract_version": contract_version,
        "status": str(row.get("status", "")),
        "reason_code": str(row.get("status_reason", "")),
        "route_id": str(row.get("route_id", "")),
        "lease_scope": str(row.get("lease_scope", "")),
        "lease_state": str(row.get("lease_state", "")),
        "lease_backend": str(row.get("lease_backend", "")),
        "lease_version": int(row.get("lease_version", 1) or 1),
        "holder_id": str(row.get("holder_id", "")),
        "owner_job": row.get("owner_job"),
        "requested_at_utc": str(row.get("requested_at_utc", "")),
        "heartbeat_at_utc": row.get("heartbeat_at_utc"),
        "lease_token": row.get("lease_token"),
        "expires_at_utc": row.get("expires_at_utc"),
        "released_at_utc": row.get("released_at_utc"),
        "event_sequence": int(row.get("event_sequence", 0) or 0),
        "blocking_holder_id": row.get("blocking_holder_id"),
        "blocking_lease_token": row.get("blocking_lease_token"),
        "previous_lease_owner": row.get("previous_lease_owner"),
        "retry_of_run_id": row.get("retry_of_run_id"),
        "ledger_entry": dict(row),
    }


def _append_technical_route_ledger_row(*, ledger_table_path: Path, row: dict[str, Any]) -> None:
    append_delta_table_rows(
        table_path=ledger_table_path,
        rows=[row],
        columns=TECHNICAL_ROUTE_LEDGER_COLUMNS,
    )


def read_technical_route_run_ledger(
    *,
    ledger_table_path: Path,
    route_id: str | None = None,
) -> list[dict[str, Any]]:
    rows = _load_technical_route_ledger_rows(ledger_table_path)
    filtered = rows
    if route_id is not None:
        route_id_text = _require_non_empty_text(route_id, "route_id")
        filtered = [row for row in rows if str(row.get("route_id", "")).strip() == route_id_text]
    filtered.sort(
        key=lambda row: (
            int(row.get("event_sequence", 0) or 0),
            str(row.get("requested_at_utc", "")),
        )
    )
    normalized: list[dict[str, Any]] = []
    for row in filtered:
        payload = dict(row)
        payload["event_sequence"] = int(payload.get("event_sequence", 0) or 0)
        payload["lease_version"] = _read_lease_version(payload)
        payload["ttl_seconds"] = int(payload.get("ttl_seconds", 0) or 0)
        payload["lease_timeout_sec"] = int(payload.get("lease_timeout_sec", payload["ttl_seconds"]) or 0)
        payload["lease_scope"] = str(payload.get("lease_scope", "")).strip() or LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE
        payload["lease_backend"] = str(payload.get("lease_backend", "")).strip() or LEASE_BACKEND_DELTA_LEDGER_CAS
        payload["lease_state"] = _derive_lease_state(payload)
        metadata_raw = payload.get("metadata_json")
        if isinstance(metadata_raw, str) and metadata_raw.strip():
            try:
                decoded = json.loads(metadata_raw)
            except json.JSONDecodeError:
                decoded = {"raw": metadata_raw}
            if isinstance(decoded, dict):
                payload["metadata_json"] = decoded
        normalized.append(payload)
    return normalized


def acquire_technical_route_lease(
    *,
    ledger_table_path: Path,
    route_id: str,
    holder_id: str,
    requested_at_utc: str,
    ttl_seconds: int,
    expected_lease_token: str | None = None,
    run_id: str = "",
    changed_windows_hash: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    lease_scope: str = LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE,
    owner_job: str = "",
    lease_backend: str = LEASE_BACKEND_DELTA_LEDGER_CAS,
) -> dict[str, Any]:
    route_id_text = _require_non_empty_text(route_id, "route_id")
    holder_id_text = _require_non_empty_text(holder_id, "holder_id")
    request_time = _parse_iso_utc(requested_at_utc, field_name="requested_at_utc")
    ttl_seconds_int = _require_non_negative_int(ttl_seconds, "ttl_seconds")
    if ttl_seconds_int <= 0:
        raise ValueError("`ttl_seconds` must be > 0")
    lease_scope_text = _normalize_lease_scope(lease_scope)
    lease_backend_text = _normalize_lease_backend(lease_backend)
    owner_job_text = str(owner_job).strip() or holder_id_text

    expected_token_text = str(expected_lease_token).strip() if expected_lease_token else None
    rows = _load_technical_route_ledger_rows(ledger_table_path)
    active = _find_active_lease(
        rows=rows,
        route_id=route_id_text,
        lease_scope=lease_scope_text,
        as_of=request_time,
    )
    event_sequence = _next_event_sequence(rows)
    request_iso = _to_iso_utc(request_time)
    expires_iso = _to_iso_utc(request_time + timedelta(seconds=ttl_seconds_int))
    next_lease_version = _max_lease_version(rows=rows, route_id=route_id_text, lease_scope=lease_scope_text) + 1

    status = STATUS_PASS
    status_reason = "lease_acquired"
    lease_state = LEASE_STATE_ACQUIRED
    lease_token = f"lease-{uuid4().hex}"
    lease_version = next_lease_version
    heartbeat_at_utc = request_iso
    blocking_holder_id: str | None = None
    blocking_lease_token: str | None = None
    previous_lease_owner: str | None = None
    retry_of_run_id: str | None = None
    resolved_expires = expires_iso

    if active is not None:
        active_holder = str(active.get("holder_id", "")).strip()
        active_token = str(active.get("lease_token", "")).strip() or None
        active_owner_job = str(active.get("owner_job", "")).strip() or active_holder
        active_expires = str(active.get("expires_at_utc", "")).strip() or None
        active_heartbeat = str(active.get("heartbeat_at_utc", "")).strip() or request_iso
        active_version = _read_lease_version(active)
        if active_holder == holder_id_text:
            if expected_token_text and expected_token_text != active_token:
                status = STATUS_BLOCKED
                status_reason = "lease_token_mismatch"
                lease_state = LEASE_STATE_BLOCKED_CONFLICT
                lease_token = ""
                lease_version = active_version
                heartbeat_at_utc = None
                resolved_expires = ""
                blocking_holder_id = active_holder
                blocking_lease_token = active_token
            else:
                status = STATUS_PASS_NOOP
                status_reason = "lease_already_held"
                lease_state = _derive_lease_state(active) or LEASE_STATE_ACQUIRED
                lease_token = active_token or ""
                lease_version = active_version
                owner_job_text = active_owner_job
                heartbeat_at_utc = active_heartbeat
                resolved_expires = active_expires or expires_iso
        else:
            status = STATUS_BLOCKED
            status_reason = "lease_conflict"
            lease_state = LEASE_STATE_BLOCKED_CONFLICT
            lease_token = ""
            lease_version = active_version
            heartbeat_at_utc = None
            resolved_expires = ""
            blocking_holder_id = active_holder
            blocking_lease_token = active_token

    row = _build_ledger_row(
        event_sequence=event_sequence,
        route_id=route_id_text,
        event_kind=LEASE_EVENT_ACQUIRE,
        status=status,
        status_reason=status_reason,
        lease_scope=lease_scope_text,
        lease_state=lease_state,
        lease_backend=lease_backend_text,
        lease_version=lease_version,
        holder_id=holder_id_text,
        owner_job=owner_job_text,
        lease_token=lease_token or None,
        expected_lease_token=expected_token_text,
        requested_at_utc=request_iso,
        heartbeat_at_utc=heartbeat_at_utc,
        expires_at_utc=resolved_expires or None,
        released_at_utc=None,
        blocking_holder_id=blocking_holder_id,
        blocking_lease_token=blocking_lease_token,
        previous_lease_owner=previous_lease_owner,
        ttl_seconds=ttl_seconds_int,
        lease_timeout_sec=ttl_seconds_int,
        run_id=str(run_id or "").strip() or None,
        retry_of_run_id=retry_of_run_id,
        changed_windows_hash=changed_windows_hash,
        metadata=metadata,
    )
    _append_technical_route_ledger_row(ledger_table_path=ledger_table_path, row=row)

    return _build_lease_response("technical_route_lease_acquire_response.v1", row)


def heartbeat_technical_route_lease(
    *,
    ledger_table_path: Path,
    route_id: str,
    holder_id: str,
    lease_token: str,
    requested_at_utc: str,
    ttl_seconds: int,
    expected_lease_version: int | None = None,
    run_id: str = "",
    changed_windows_hash: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    lease_scope: str = LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE,
    owner_job: str = "",
    lease_backend: str = LEASE_BACKEND_DELTA_LEDGER_CAS,
) -> dict[str, Any]:
    route_id_text = _require_non_empty_text(route_id, "route_id")
    holder_id_text = _require_non_empty_text(holder_id, "holder_id")
    lease_token_text = _require_non_empty_text(lease_token, "lease_token")
    request_time = _parse_iso_utc(requested_at_utc, field_name="requested_at_utc")
    request_iso = _to_iso_utc(request_time)
    ttl_seconds_int = _require_non_negative_int(ttl_seconds, "ttl_seconds")
    if ttl_seconds_int <= 0:
        raise ValueError("`ttl_seconds` must be > 0")
    lease_scope_text = _normalize_lease_scope(lease_scope)
    lease_backend_text = _normalize_lease_backend(lease_backend)
    owner_job_text = str(owner_job).strip() or holder_id_text

    rows = _load_technical_route_ledger_rows(ledger_table_path)
    event_sequence = _next_event_sequence(rows)
    active = _find_active_lease(
        rows=rows,
        route_id=route_id_text,
        lease_scope=lease_scope_text,
        as_of=request_time,
    )
    max_version = _max_lease_version(rows=rows, route_id=route_id_text, lease_scope=lease_scope_text)
    lease_version = max(1, max_version)

    status = STATUS_BLOCKED
    status_reason = "lease_not_active"
    lease_state = LEASE_STATE_BLOCKED_CONFLICT
    expires_at_utc: str | None = None
    heartbeat_at_utc: str | None = None
    blocking_holder_id: str | None = None
    blocking_lease_token: str | None = None
    previous_lease_owner: str | None = None
    retry_of_run_id: str | None = None

    if active is not None:
        active_holder = str(active.get("holder_id", "")).strip()
        active_owner_job = str(active.get("owner_job", "")).strip() or active_holder
        active_token = str(active.get("lease_token", "")).strip()
        active_expires = str(active.get("expires_at_utc", "")).strip() or None
        active_version = _read_lease_version(active)
        lease_version = active_version
        blocking_holder_id = active_holder or None
        blocking_lease_token = active_token or None

        if active_holder != holder_id_text or active_token != lease_token_text:
            status_reason = "lease_conflict"
            owner_job_text = active_owner_job
            expires_at_utc = active_expires
        elif expected_lease_version is not None and int(expected_lease_version) != active_version:
            status_reason = "lease_version_mismatch"
            owner_job_text = active_owner_job
            expires_at_utc = active_expires
        else:
            status = STATUS_PASS
            status_reason = "lease_heartbeated"
            lease_state = LEASE_STATE_HEARTBEATING
            lease_version = active_version + 1
            blocking_holder_id = None
            blocking_lease_token = None
            expires_at_utc = _to_iso_utc(request_time + timedelta(seconds=ttl_seconds_int))
            heartbeat_at_utc = request_iso
            owner_job_text = active_owner_job

    row = _build_ledger_row(
        event_sequence=event_sequence,
        route_id=route_id_text,
        event_kind=LEASE_EVENT_HEARTBEAT,
        status=status,
        status_reason=status_reason,
        lease_scope=lease_scope_text,
        lease_state=lease_state,
        lease_backend=lease_backend_text,
        lease_version=lease_version,
        holder_id=holder_id_text,
        owner_job=owner_job_text,
        lease_token=lease_token_text,
        expected_lease_token=None,
        requested_at_utc=request_iso,
        heartbeat_at_utc=heartbeat_at_utc,
        expires_at_utc=expires_at_utc,
        released_at_utc=None,
        blocking_holder_id=blocking_holder_id,
        blocking_lease_token=blocking_lease_token,
        previous_lease_owner=previous_lease_owner,
        ttl_seconds=ttl_seconds_int,
        lease_timeout_sec=ttl_seconds_int,
        run_id=str(run_id or "").strip() or None,
        retry_of_run_id=retry_of_run_id,
        changed_windows_hash=changed_windows_hash,
        metadata=metadata,
    )
    _append_technical_route_ledger_row(ledger_table_path=ledger_table_path, row=row)

    return _build_lease_response("technical_route_lease_heartbeat_response.v1", row)


def takeover_technical_route_lease(
    *,
    ledger_table_path: Path,
    route_id: str,
    holder_id: str,
    requested_at_utc: str,
    ttl_seconds: int,
    expected_lease_version: int,
    previous_lease_token: str | None = None,
    run_id: str = "",
    retry_of_run_id: str | None = None,
    changed_windows_hash: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    lease_scope: str = LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE,
    owner_job: str = "",
    lease_backend: str = LEASE_BACKEND_DELTA_LEDGER_CAS,
) -> dict[str, Any]:
    route_id_text = _require_non_empty_text(route_id, "route_id")
    holder_id_text = _require_non_empty_text(holder_id, "holder_id")
    request_time = _parse_iso_utc(requested_at_utc, field_name="requested_at_utc")
    request_iso = _to_iso_utc(request_time)
    ttl_seconds_int = _require_non_negative_int(ttl_seconds, "ttl_seconds")
    if ttl_seconds_int <= 0:
        raise ValueError("`ttl_seconds` must be > 0")
    expected_lease_version_int = _require_non_negative_int(expected_lease_version, "expected_lease_version")
    if expected_lease_version_int <= 0:
        raise ValueError("`expected_lease_version` must be > 0")
    lease_scope_text = _normalize_lease_scope(lease_scope)
    lease_backend_text = _normalize_lease_backend(lease_backend)
    owner_job_text = str(owner_job).strip() or holder_id_text
    previous_lease_token_text = str(previous_lease_token or "").strip() or None

    rows = _load_technical_route_ledger_rows(ledger_table_path)
    event_sequence = _next_event_sequence(rows)
    max_version = _max_lease_version(rows=rows, route_id=route_id_text, lease_scope=lease_scope_text)
    latest = _latest_successful_row(rows=rows, route_id=route_id_text, lease_scope=lease_scope_text)

    status = STATUS_BLOCKED
    status_reason = "lease_not_found"
    lease_state = LEASE_STATE_BLOCKED_CONFLICT
    lease_version = max(1, max_version)
    lease_token_value: str | None = None
    heartbeat_at_utc: str | None = None
    expires_at_utc: str | None = None
    released_at_utc: str | None = None
    blocking_holder_id: str | None = None
    blocking_lease_token: str | None = None
    previous_lease_owner: str | None = None
    retry_of_run_id_text: str | None = str(retry_of_run_id or "").strip() or None

    if latest is not None:
        latest_state = _derive_lease_state(latest)
        latest_holder = str(latest.get("holder_id", "")).strip() or None
        latest_token = str(latest.get("lease_token", "")).strip() or None
        latest_version = _read_lease_version(latest)
        latest_run_id = str(latest.get("run_id", "")).strip() or None
        blocking_holder_id = latest_holder
        blocking_lease_token = latest_token
        previous_lease_owner = latest_holder
        lease_version = latest_version

        if latest_state not in _ACTIVE_LEASE_STATES:
            status_reason = "lease_not_active_for_takeover"
        elif not _lease_expires_before_or_at(row=latest, as_of=request_time):
            status_reason = "lease_not_stale"
        elif expected_lease_version_int != latest_version:
            status_reason = "lease_version_mismatch"
        elif previous_lease_token_text and latest_token != previous_lease_token_text:
            status_reason = "lease_token_mismatch"
        else:
            status = STATUS_PASS
            status_reason = "lease_taken_over"
            lease_state = LEASE_STATE_TAKEN_OVER
            lease_version = latest_version + 1
            lease_token_value = f"lease-{uuid4().hex}"
            heartbeat_at_utc = request_iso
            expires_at_utc = _to_iso_utc(request_time + timedelta(seconds=ttl_seconds_int))
            blocking_holder_id = None
            blocking_lease_token = None
            retry_of_run_id_text = retry_of_run_id_text or latest_run_id

    row = _build_ledger_row(
        event_sequence=event_sequence,
        route_id=route_id_text,
        event_kind=LEASE_EVENT_TAKEOVER,
        status=status,
        status_reason=status_reason,
        lease_scope=lease_scope_text,
        lease_state=lease_state,
        lease_backend=lease_backend_text,
        lease_version=lease_version,
        holder_id=holder_id_text,
        owner_job=owner_job_text,
        lease_token=lease_token_value,
        expected_lease_token=previous_lease_token_text,
        requested_at_utc=request_iso,
        heartbeat_at_utc=heartbeat_at_utc,
        expires_at_utc=expires_at_utc,
        released_at_utc=released_at_utc,
        blocking_holder_id=blocking_holder_id,
        blocking_lease_token=blocking_lease_token,
        previous_lease_owner=previous_lease_owner,
        ttl_seconds=ttl_seconds_int,
        lease_timeout_sec=ttl_seconds_int,
        run_id=str(run_id or "").strip() or None,
        retry_of_run_id=retry_of_run_id_text,
        changed_windows_hash=changed_windows_hash,
        metadata=metadata,
    )
    _append_technical_route_ledger_row(ledger_table_path=ledger_table_path, row=row)

    return _build_lease_response("technical_route_lease_takeover_response.v1", row)


def release_technical_route_lease(
    *,
    ledger_table_path: Path,
    route_id: str,
    holder_id: str,
    lease_token: str,
    requested_at_utc: str,
    run_id: str = "",
    changed_windows_hash: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    lease_scope: str = LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE,
    owner_job: str = "",
    lease_backend: str = LEASE_BACKEND_DELTA_LEDGER_CAS,
) -> dict[str, Any]:
    route_id_text = _require_non_empty_text(route_id, "route_id")
    holder_id_text = _require_non_empty_text(holder_id, "holder_id")
    lease_token_text = _require_non_empty_text(lease_token, "lease_token")
    request_time = _parse_iso_utc(requested_at_utc, field_name="requested_at_utc")
    request_iso = _to_iso_utc(request_time)
    lease_scope_text = _normalize_lease_scope(lease_scope)
    lease_backend_text = _normalize_lease_backend(lease_backend)
    owner_job_text = str(owner_job).strip() or holder_id_text

    rows = _load_technical_route_ledger_rows(ledger_table_path)
    active = _find_active_lease(
        rows=rows,
        route_id=route_id_text,
        lease_scope=lease_scope_text,
        as_of=request_time,
    )
    event_sequence = _next_event_sequence(rows)
    max_lease_version = _max_lease_version(rows=rows, route_id=route_id_text, lease_scope=lease_scope_text)
    lease_version = max(1, max_lease_version)

    status = STATUS_PASS_NOOP
    status_reason = "lease_not_active"
    lease_state = LEASE_STATE_EXPIRED
    expires_at_utc: str | None = None
    released_at_utc: str | None = None
    blocking_holder_id: str | None = None
    blocking_lease_token: str | None = None
    previous_lease_owner: str | None = None
    retry_of_run_id: str | None = None
    lease_timeout_sec = 0

    if active is not None:
        active_holder = str(active.get("holder_id", "")).strip()
        active_owner_job = str(active.get("owner_job", "")).strip() or active_holder
        active_token = str(active.get("lease_token", "")).strip()
        expires_at_utc = str(active.get("expires_at_utc", "")).strip() or None
        previous_lease_owner = active_holder or None
        lease_version = _read_lease_version(active)
        lease_timeout_sec = int(active.get("lease_timeout_sec", active.get("ttl_seconds", 0)) or 0)
        owner_job_text = active_owner_job
        if active_holder != holder_id_text or active_token != lease_token_text:
            status = STATUS_BLOCKED
            status_reason = "lease_conflict"
            lease_state = LEASE_STATE_BLOCKED_CONFLICT
            blocking_holder_id = active_holder or None
            blocking_lease_token = active_token or None
        else:
            status = STATUS_PASS
            status_reason = "lease_released"
            lease_state = LEASE_STATE_RELEASED
            lease_version = _read_lease_version(active) + 1
            released_at_utc = request_iso

    row = _build_ledger_row(
        event_sequence=event_sequence,
        route_id=route_id_text,
        event_kind=LEASE_EVENT_RELEASE,
        status=status,
        status_reason=status_reason,
        lease_scope=lease_scope_text,
        lease_state=lease_state,
        lease_backend=lease_backend_text,
        lease_version=lease_version,
        holder_id=holder_id_text,
        owner_job=owner_job_text,
        lease_token=lease_token_text,
        expected_lease_token=None,
        requested_at_utc=request_iso,
        heartbeat_at_utc=None,
        expires_at_utc=expires_at_utc,
        released_at_utc=released_at_utc,
        blocking_holder_id=blocking_holder_id,
        blocking_lease_token=blocking_lease_token,
        previous_lease_owner=previous_lease_owner,
        ttl_seconds=0,
        lease_timeout_sec=lease_timeout_sec,
        run_id=str(run_id or "").strip() or None,
        retry_of_run_id=retry_of_run_id,
        changed_windows_hash=changed_windows_hash,
        metadata=metadata,
    )
    _append_technical_route_ledger_row(ledger_table_path=ledger_table_path, row=row)

    return _build_lease_response("technical_route_lease_release_response.v1", row)


def record_technical_route_blocked_conflict(
    *,
    ledger_table_path: Path,
    route_id: str,
    holder_id: str,
    requested_at_utc: str,
    reason_code: str,
    blocking_holder_id: str | None = None,
    blocking_lease_token: str | None = None,
    expected_lease_version: int | None = None,
    run_id: str = "",
    changed_windows_hash: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    lease_scope: str = LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE,
    owner_job: str = "",
    lease_backend: str = LEASE_BACKEND_DELTA_LEDGER_CAS,
) -> dict[str, Any]:
    route_id_text = _require_non_empty_text(route_id, "route_id")
    holder_id_text = _require_non_empty_text(holder_id, "holder_id")
    reason_code_text = _require_non_empty_text(reason_code, "reason_code")
    request_time = _parse_iso_utc(requested_at_utc, field_name="requested_at_utc")
    request_iso = _to_iso_utc(request_time)
    lease_scope_text = _normalize_lease_scope(lease_scope)
    lease_backend_text = _normalize_lease_backend(lease_backend)
    owner_job_text = str(owner_job).strip() or holder_id_text

    rows = _load_technical_route_ledger_rows(ledger_table_path)
    event_sequence = _next_event_sequence(rows)
    active = _find_active_lease(
        rows=rows,
        route_id=route_id_text,
        lease_scope=lease_scope_text,
        as_of=request_time,
    )
    max_lease_version = _max_lease_version(rows=rows, route_id=route_id_text, lease_scope=lease_scope_text)
    lease_version = max(1, max_lease_version)

    resolved_blocking_holder = str(blocking_holder_id or "").strip() or None
    resolved_blocking_token = str(blocking_lease_token or "").strip() or None
    previous_lease_owner: str | None = None
    expected_lease_version_int: int | None = None
    if expected_lease_version is not None:
        expected_lease_version_int = _require_non_negative_int(expected_lease_version, "expected_lease_version")
        if expected_lease_version_int <= 0:
            raise ValueError("`expected_lease_version` must be > 0")

    if active is not None:
        lease_version = _read_lease_version(active)
        active_holder = str(active.get("holder_id", "")).strip() or None
        active_token = str(active.get("lease_token", "")).strip() or None
        previous_lease_owner = active_holder
        if resolved_blocking_holder is None:
            resolved_blocking_holder = active_holder
        if resolved_blocking_token is None:
            resolved_blocking_token = active_token

    if expected_lease_version_int is not None and expected_lease_version_int != lease_version:
        reason_code_text = "lease_version_mismatch"

    row = _build_ledger_row(
        event_sequence=event_sequence,
        route_id=route_id_text,
        event_kind=LEASE_EVENT_CONFLICT_BLOCKED,
        status=STATUS_BLOCKED,
        status_reason=reason_code_text,
        lease_scope=lease_scope_text,
        lease_state=LEASE_STATE_BLOCKED_CONFLICT,
        lease_backend=lease_backend_text,
        lease_version=lease_version,
        holder_id=holder_id_text,
        owner_job=owner_job_text,
        lease_token=None,
        expected_lease_token=None,
        requested_at_utc=request_iso,
        heartbeat_at_utc=None,
        expires_at_utc=None,
        released_at_utc=None,
        blocking_holder_id=resolved_blocking_holder,
        blocking_lease_token=resolved_blocking_token,
        previous_lease_owner=previous_lease_owner,
        ttl_seconds=0,
        lease_timeout_sec=0,
        run_id=str(run_id or "").strip() or None,
        retry_of_run_id=None,
        changed_windows_hash=changed_windows_hash,
        metadata=metadata,
    )
    _append_technical_route_ledger_row(ledger_table_path=ledger_table_path, row=row)

    return _build_lease_response("technical_route_lease_record_blocked_conflict_response.v1", row)
