from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(payload: object) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def qc_observation(
    *,
    run_id: str,
    check_id: str,
    check_group: str,
    severity: str,
    status: str,
    entity_key: str,
    observed_value: object,
    expected_value: object,
    sample_rows: object = (),
    created_at_utc: str | None = None,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "check_id": check_id,
        "check_group": check_group,
        "severity": severity,
        "status": status,
        "entity_key": entity_key,
        "observed_value": str(observed_value),
        "expected_value": str(expected_value),
        "sample_rows_hash": stable_hash(sample_rows),
        "sample_rows_json": sample_rows,
        "created_at_utc": created_at_utc or utc_now_iso(),
    }
