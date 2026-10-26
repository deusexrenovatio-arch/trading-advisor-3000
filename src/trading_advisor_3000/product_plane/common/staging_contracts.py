"""Shared schema and signature boundary for protected staging control documents.

The JSON schema is the envelope authority. This module validates transport and
plan coherence; the protected collector/gate own actual route-evidence checks.
Signatures cover schema_version, key_id and payload, using canonical UTF-8 JSON.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_PATH = (
    Path(__file__).resolve().parents[1] / "contracts" / "schemas" / "staging_acceptance.v1.json"
)
_KINDS = frozenset(
    {
        "plan",
        "grant",
        "receipt",
        "signed_receipt",
        "budget_approval",
        "signed_budget_approval",
        "reference_authorization",
        "signed_reference_authorization",
    }
)


def canonical_bytes(value: object) -> bytes:
    """The sole encoding used by plan identities and receipt signatures."""

    def check_keys(item: object) -> None:
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise ValueError("staging JSON object keys must be strings")
            for nested in item.values():
                check_keys(nested)
        elif isinstance(item, (list, tuple)):
            for nested in item:
                check_keys(nested)

    check_keys(value)
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("staging document must contain finite JSON values") from exc


def digest(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate staging document key: {key}")
        result[key] = value
    return result


@lru_cache(maxsize=1)
def _schema() -> dict[str, Any]:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    Draft202012Validator.check_schema(schema)
    return schema


def _utc(value: str, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must be a UTC timestamp")
    return parsed.astimezone(UTC)


def _absolute_parts(value: str) -> tuple[str, ...]:
    path = PureWindowsPath(value) if PureWindowsPath(value).drive else PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("staging paths must be absolute and contain no parent traversal")
    return tuple(part.lower() for part in path.parts) if path.drive else path.parts


def _execution_windows(plan: Mapping[str, Any]):
    if "execution_window" in plan and "daily_execution_window_utc" in plan:
        raise ValueError("absolute and daily execution windows are mutually exclusive")
    if "execution_window" in plan:
        window = plan["execution_window"]
        if not isinstance(window, Mapping) or set(window) != {"start_utc", "end_utc"}:
            raise ValueError("execution_window requires start_utc and end_utc")
        if any(not isinstance(value, str) for value in window.values()):
            raise ValueError("execution_window requires UTC timestamps")
        start = _utc(window["start_utc"], field="execution_window.start_utc")
        end = _utc(window["end_utc"], field="execution_window.end_utc")
        if start >= end:
            raise ValueError("execution_window must have increasing bounds")
        return (start, end), None
    if "daily_execution_window_utc" in plan:
        window = plan["daily_execution_window_utc"]
        if not isinstance(window, Mapping) or set(window) != {"start", "end"}:
            raise ValueError("daily_execution_window_utc requires start and end")
        minutes = []
        for key in ("start", "end"):
            value = window[key]
            if not isinstance(value, str) or not re.fullmatch(
                r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value
            ):
                raise ValueError("daily execution window requires exact UTC HH:MM bounds")
            hour, minute = map(int, value.split(":"))
            minutes.append(hour * 60 + minute)
        if minutes[0] == minutes[1]:
            raise ValueError("daily execution window bounds must differ")
        return None, tuple(minutes)
    return None, None


def execution_window_end(plan: Mapping[str, Any], now: datetime) -> datetime | None:
    """Active half-open UTC window end only; runtime/deadline authorization is separate."""
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("execution window observation time must be timezone-aware")
    observed = now.astimezone(UTC)
    absolute, daily = _execution_windows(plan)
    if absolute is not None:
        start, end = absolute
        return end if start <= observed < end else None
    if daily is None:
        return None
    midnight = observed.replace(hour=0, minute=0, second=0, microsecond=0)
    start, end = (midnight + timedelta(minutes=value) for value in daily)
    if start < end:
        return end if start <= observed < end else None
    if observed >= start:
        return end + timedelta(days=1)
    return end if observed < end else None


def _validate_plan(plan: Mapping[str, Any]) -> None:
    root = plan["root_job"]
    jobs = plan["jobs"]
    if root not in jobs or jobs[root]["upstream_job"] is not None:
        raise ValueError("plan root_job must exist and have no upstream_job")
    for name in jobs:
        seen: set[str] = set()
        current = name
        while current != root:
            if current in seen:
                raise ValueError("plan job graph contains a cycle")
            seen.add(current)
            upstream = jobs[current]["upstream_job"]
            if upstream not in jobs:
                raise ValueError("every plan job must descend from root_job")
            current = upstream
    expected_kind = "small" if plan["stage"] == "branch" else "acceptance"
    if plan["attempt_kind"] not in {expected_kind, "rebuild"}:
        raise ValueError("plan stage and attempt_kind disagree")
    if plan["scope"].get("full_history") and plan["attempt_kind"] != "rebuild":
        raise ValueError("full_history requires the explicit rebuild attempt kind")
    start = _utc(plan["scope"]["start_utc"], field="scope.start_utc")
    end = _utc(plan["scope"]["end_utc"], field="scope.end_utc")
    if end <= start:
        raise ValueError("plan scope must have an increasing time interval")
    deadline = _utc(plan["deadline_utc"], field="deadline_utc")
    window, _ = _execution_windows(plan)
    if window is not None:
        if window[1] > deadline:
            raise ValueError(
                "protected execution_window must increase and end within plan deadline"
            )
    data = _absolute_parts(plan["data_root"])
    quota = _absolute_parts(plan["quota_home"])
    if data == quota[: len(data)] or quota == data[: len(quota)]:
        raise ValueError("quota_home must be independent of staging data_root")


def validate_document(kind: str, payload: object) -> dict[str, Any]:
    """Validate against one named definition from the versioned authority."""
    if kind not in _KINDS:
        raise ValueError(f"unsupported staging document kind: {kind}")
    canonical_bytes(payload)
    schema = {**_schema(), "$ref": f"#/$defs/{kind}"}
    schema.pop("oneOf", None)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(
        validator.iter_errors(payload), key=lambda error: str(list(error.absolute_path))
    )
    if errors:
        error = errors[0]
        location = ".".join(str(item) for item in error.absolute_path) or "document"
        raise ValueError(f"invalid staging {kind} at {location}: {error.message}")
    result = deepcopy(dict(payload))
    if kind == "plan":
        _validate_plan(result)
    elif kind == "grant":
        _absolute_parts(result["output_path"])
        _utc(result["deadline_utc"], field="deadline_utc")
    elif kind in {"receipt", "signed_receipt"}:
        receipt = result if kind == "receipt" else result["payload"]
        issued = _utc(receipt["issued_at_utc"], field="issued_at_utc")
        expires = _utc(receipt["expires_at_utc"], field="expires_at_utc")
        if expires <= issued:
            raise ValueError("staging receipt expiry must follow its issue time")
    elif kind in {
        "budget_approval",
        "signed_budget_approval",
        "reference_authorization",
        "signed_reference_authorization",
    }:
        approval = result["payload"] if kind.startswith("signed_") else result
        _utc(approval["issued_at_utc"], field="issued_at_utc")
    return result


def load_plan(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(
            Path(path).read_text(encoding="utf-8"), object_pairs_hook=_unique_object
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"staging plan is unavailable: {path}") from exc
    return validate_document("plan", payload)


def _signing_bytes(envelope: Mapping[str, Any]) -> bytes:
    return canonical_bytes({key: value for key, value in envelope.items() if key != "signature"})


def _sign(payload: object, private_key: bytes, key_id: str, kind: str) -> dict[str, Any]:
    value = validate_document(kind, payload)
    envelope_kind = f"signed_{kind}"
    unsigned = {
        "schema_version": _schema()["$defs"][envelope_kind]["properties"]["schema_version"][
            "const"
        ],
        "key_id": key_id,
        "payload": value,
    }
    validate_document(envelope_kind, {**unsigned, "signature": "pending"})
    try:
        key = Ed25519PrivateKey.from_private_bytes(private_key)
    except (TypeError, ValueError) as exc:
        raise ValueError("staging signing key must be 32 raw Ed25519 private bytes") from exc
    return {
        **unsigned,
        "signature": base64.b64encode(key.sign(_signing_bytes(unsigned))).decode("ascii"),
    }


def _verify(envelope: object, keyring: Mapping[str, str], kind: str) -> dict[str, Any]:
    value = validate_document(f"signed_{kind}", envelope)
    if not isinstance(keyring, Mapping):
        raise ValueError("staging receipt keyring must be a protected key mapping")
    encoded_key = keyring.get(value["key_id"])
    if not isinstance(encoded_key, str):
        raise ValueError("staging receipt signing key is not trusted")
    try:
        public = Ed25519PublicKey.from_public_bytes(base64.b64decode(encoded_key, validate=True))
        signature = base64.b64decode(value["signature"], validate=True)
        public.verify(signature, _signing_bytes(value))
    except (TypeError, ValueError, InvalidSignature) as exc:
        raise ValueError("staging receipt signature is invalid") from exc
    return value["payload"]


def _active_at(payload: Mapping[str, Any], now: datetime | None) -> datetime:
    observed = now if now is not None else datetime.now(UTC)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ValueError("staging receipt verification time must be timezone-aware")
    observed = observed.astimezone(UTC)
    if observed < _utc(payload["issued_at_utc"], field="issued_at_utc"):
        raise ValueError("staging signed document is not active yet")
    return observed


def sign_receipt(payload: object, private_key: bytes, key_id: str) -> dict[str, Any]:
    return _sign(payload, private_key, key_id, "receipt")


def verify_receipt(
    envelope: object, keyring: Mapping[str, str], now: datetime | None = None
) -> dict[str, Any]:
    """Authenticate the envelope and its active window, without trusting route claims."""
    payload = _verify(envelope, keyring, "receipt")
    observed = _active_at(payload, now)
    if observed >= _utc(payload["expires_at_utc"], field="expires_at_utc"):
        raise ValueError("staging receipt has expired")
    return payload


def sign_budget_approval(payload: object, private_key: bytes, key_id: str) -> dict[str, Any]:
    return _sign(payload, private_key, key_id, "budget_approval")


def verify_budget_approval(
    envelope: object, keyring: Mapping[str, str], now: datetime | None = None
) -> dict[str, Any]:
    """Authenticate a durable operator approval; plans retain their own deadlines."""
    payload = _verify(envelope, keyring, "budget_approval")
    _active_at(payload, now)
    return payload


def sign_reference_authorization(
    payload: object, private_key: bytes, key_id: str
) -> dict[str, Any]:
    return _sign(payload, private_key, key_id, "reference_authorization")


def verify_reference_authorization(
    envelope: object, keyring: Mapping[str, str], now: datetime | None = None
) -> dict[str, Any]:
    """Authenticate reference-mode activation; the bound plan controls its deadline."""
    payload = _verify(envelope, keyring, "reference_authorization")
    _active_at(payload, now)
    return payload
