from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast
from urllib import error as urllib_error


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from run_f1e_real_broker_process import (  # noqa: E402
    has_synthetic_marker,
    is_placeholder_secret,
    probe_finam_session_details,
    validate_finam_session_details,
    validate_real_connector_base_url,
    validate_real_connector_health,
    validate_connector_profile,
    validate_miswire_failure,
    validate_rollout_payload,
    validate_smoke_payload,
    verify_hash_manifest,
    write_hash_manifest,
)


def _profile_fixture() -> dict[str, object]:
    payload = json.loads(
        (ROOT / "tests" / "app" / "fixtures" / "contracts" / "broker_staging_connector_profile.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert isinstance(payload, dict)
    return payload


def test_validate_connector_profile_accepts_fixture() -> None:
    profile = validate_connector_profile(_profile_fixture())
    assert profile["environment"] == "staging-real"
    assert profile["proof_class"] == "staging-real"


def test_validate_connector_profile_rejects_missing_required_flag() -> None:
    payload = _profile_fixture()
    required_flags = cast(dict[str, Any], payload["required_feature_flags"])
    required_flags["TA3000_ENABLE_QUIK_CONNECTOR"] = "0"

    try:
        validate_connector_profile(payload)
    except ValueError as exc:
        assert "TA3000_ENABLE_QUIK_CONNECTOR=1" in str(exc)
    else:
        raise AssertionError("expected connector profile validation failure")


def test_validate_connector_profile_rejects_missing_finam_binding_env_var() -> None:
    payload = _profile_fixture()
    payload["finam_session_binding"] = {
        "base_url_env_var": "",
        "jwt_env_var": "TA3000_FINAM_API_TOKEN",
        "session_details_path": "/v1/sessions/details",
        "required_session_fields": [
            "created_at",
            "expires_at",
            "readonly",
        ],
    }

    try:
        validate_connector_profile(payload)
    except ValueError as exc:
        assert "finam_session_binding.base_url_env_var" in str(exc)
    else:
        raise AssertionError("expected connector profile validation failure")


def test_validate_smoke_payload_rejects_missing_kill_switch_restore_marker() -> None:
    payload = {
        "status": "ok",
        "smoke": {
            "submit_ack": {"accepted": True},
            "replace_ack": {"state": "replaced"},
            "cancel_ack": {"state": "canceled"},
            "updates_count": 4,
            "readiness": {"ready": True},
            "connector_session": {
                "connector_session_id": "SESSION-1",
                "connector_binding_source": "staging-broker-contour",
                "connector_last_heartbeat": "2026-03-31T12:00:00Z",
            },
            "kill_switch": {
                "ready_under_kill_switch": {"ready": False, "reason": "kill_switch_active"},
                "blocked_submit": {"error_code": "kill_switch_active", "status_code": 503},
                "submit_after_restore": {"accepted": True},
            },
            "metrics": {
                "while_kill_switch_enabled": "ta3000_sidecar_gateway_kill_switch 1",
                "after_kill_switch_restore": "no marker",
            },
        },
    }
    try:
        validate_smoke_payload(payload)
    except ValueError as exc:
        assert "restored marker" in str(exc)
    else:
        raise AssertionError("expected smoke validation failure")


def test_validate_rollout_payload_rejects_reconciliation_incidents() -> None:
    payload = {
        "status": "ok",
        "stages": [
            {"stage": "connectivity", "status": "ok", "details": {}},
            {"stage": "canary", "status": "ok", "details": {}},
            {
                "stage": "batch",
                "status": "ok",
                "details": {"reconciliation_incidents": 1, "sync_incidents_total": 0},
            },
        ],
    }
    try:
        validate_rollout_payload(payload)
    except ValueError as exc:
        assert "reconciliation_incidents" in str(exc)
    else:
        raise AssertionError("expected rollout validation failure")


def test_validate_miswire_failure_requires_connectivity_failed() -> None:
    payload = {
        "status": "failed",
        "stages": [
            {
                "stage": "connectivity",
                "status": "ok",
                "details": {"degraded_transports": ["transport-1"]},
            }
        ],
    }
    try:
        validate_miswire_failure(payload)
    except ValueError as exc:
        assert "must be failed" in str(exc)
    else:
        raise AssertionError("expected miswire validation failure")


def test_placeholder_secret_detection_blocks_demo_values() -> None:
    assert is_placeholder_secret("CHANGE_ME_STOCKSHARP_KEY")
    assert is_placeholder_secret("sample-token-001")
    assert not is_placeholder_secret("2f9ddf68ef18477f9a7f3e0c5e73a0ff")


def test_validate_real_connector_health_requires_real_markers() -> None:
    payload = {
        "connector_mode": "staging-real",
        "connector_backend": "stocksharp-quik-finam",
        "connector_ready": True,
        "connector_session_id": "SESSION-123",
        "connector_binding_source": "staging-broker-contour",
        "connector_last_heartbeat": "2026-03-31T12:00:00Z",
    }
    validated = validate_real_connector_health(payload)
    assert validated["connector_mode"] == "staging-real"
    assert validated["connector_session_id"] == "SESSION-123"

    try:
        validate_real_connector_health(
            {
                "connector_mode": "simulated",
                "connector_backend": "stub",
                "connector_ready": False,
                "connector_session_id": "",
                "connector_binding_source": "",
                "connector_last_heartbeat": "",
            }
        )
    except RuntimeError as exc:
        assert "non-real connector backend" in str(exc) or "connector_mode" in str(exc)
    else:
        raise AssertionError("expected connector-health validation failure")


def test_validate_real_connector_health_rejects_stub_shaped_binding_source() -> None:
    for value in ("local-remediation-stub", "integration-test-stub", "sandbox-mock-route"):
        try:
            validate_real_connector_health(
                {
                    "connector_mode": "staging-real",
                    "connector_backend": "stocksharp-quik-finam",
                    "connector_ready": True,
                    "connector_session_id": "SESSION-123",
                    "connector_binding_source": value,
                    "connector_last_heartbeat": "2026-03-31T12:00:00Z",
                }
            )
        except RuntimeError as exc:
            assert "non-synthetic connector_binding_source" in str(exc)
        else:
            raise AssertionError(f"expected connector-binding rejection for {value}")


def test_validate_real_connector_health_rejects_stub_shaped_service_identity() -> None:
    try:
        validate_real_connector_health(
            {
                "service": "stocksharp-sidecar-gateway-stub",
                "connector_mode": "staging-real",
                "connector_backend": "stocksharp-quik-finam",
                "connector_ready": True,
                "connector_session_id": "SESSION-123",
                "connector_binding_source": "stocksharp-quik-finam-gateway",
                "connector_last_heartbeat": "2026-03-31T12:00:00Z",
            }
        )
    except RuntimeError as exc:
        assert "service identity" in str(exc)
    else:
        raise AssertionError("expected stub-service rejection")


def test_has_synthetic_marker_detects_tokenized_stub_variants() -> None:
    assert has_synthetic_marker("local-remediation-stub")
    assert has_synthetic_marker("in_memory")
    assert has_synthetic_marker("mock")
    assert not has_synthetic_marker("staging-broker-contour")


def test_validate_real_connector_base_url_requires_external_host() -> None:
    assert (
        validate_real_connector_base_url("https://staging-broker.example.net:8443/connector")
        == "https://staging-broker.example.net:8443/connector"
    )
    for url in ("http://localhost:18081", "http://127.0.0.1:18081", "http://0.0.0.0:18081"):
        try:
            validate_real_connector_base_url(url)
        except RuntimeError as exc:
            assert "external host" in str(exc) or "loopback" in str(exc)
        else:
            raise AssertionError(f"expected external-host rejection for {url}")


def test_probe_finam_session_details_accepts_valid_payload(monkeypatch: Any) -> None:
    class _Response:
        status = 200

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "created_at": "2026-03-31T12:00:00Z",
                    "expires_at": "2026-04-01T12:00:00Z",
                    "account_ids": ["1899011"],
                    "readonly": False,
                    "md_permissions": [],
                }
            ).encode("utf-8")

    def _urlopen(request: object, timeout: float) -> _Response:
        return _Response()

    monkeypatch.setattr("run_f1e_real_broker_process.urllib_request.urlopen", _urlopen)
    payload = probe_finam_session_details(
        base_url="https://staging-broker.example.net",
        jwt_token="real-token-value",
        session_details_path="/v1/sessions/details",
        timeout_seconds=2.0,
    )
    assert payload["status_code"] == 200
    assert payload["details_url"] == "https://staging-broker.example.net/v1/sessions/details"
    session_details = cast(dict[str, Any], payload["session_details"])
    assert session_details["readonly"] is False
    assert session_details["account_ids"] == ["1899011"]


def test_probe_finam_session_details_rejects_http_error(monkeypatch: Any) -> None:
    def _urlopen(request: object, timeout: float) -> object:
        raise urllib_error.HTTPError(
            url="https://api.finam.ru/v1/sessions/details",
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=io.BytesIO(b'{"error":"not_found"}'),
        )

    monkeypatch.setattr("run_f1e_real_broker_process.urllib_request.urlopen", _urlopen)
    try:
        probe_finam_session_details(
            base_url="https://api.finam.ru",
            jwt_token="real-token-value",
            session_details_path="/v1/sessions/details",
            timeout_seconds=2.0,
        )
    except RuntimeError as exc:
        message = str(exc)
        assert "Finam session preflight failed" in message
        assert "TA3000_FINAM_API_BASE_URL" in message
    else:
        raise AssertionError("expected preflight failure for invalid connector endpoint")


def test_validate_finam_session_details_requires_contract_fields() -> None:
    payload = {
        "created_at": "2026-03-31T12:00:00Z",
        "expires_at": "2026-04-01T12:00:00Z",
        "account_ids": [],
        "readonly": True,
        "md_permissions": [],
    }
    validated = validate_finam_session_details(payload)
    assert validated["readonly"] is True
    assert validated["account_ids"] == []


def test_hash_manifest_round_trip(tmp_path: Path) -> None:
    build_artifact = tmp_path / "build.json"
    build_artifact.write_text('{"step":"build"}\n', encoding="utf-8")
    smoke_artifact = tmp_path / "smoke.json"
    smoke_artifact.write_text('{"status":"ok"}\n', encoding="utf-8")

    entries = write_hash_manifest(
        base_dir=tmp_path,
        targets=[build_artifact, smoke_artifact],
        output_json=tmp_path / "hashes.json",
        output_sha256=tmp_path / "hashes.sha256",
    )
    verify_hash_manifest(base_dir=tmp_path, entries=entries)


def test_script_writes_failure_artifact_when_required_secrets_missing(tmp_path: Path) -> None:
    output_root = tmp_path / "phase05-run"
    run_id = "missing-secrets"
    env = dict(os.environ)
    env.pop("TA3000_STOCKSHARP_API_KEY", None)
    env.pop("TA3000_FINAM_API_TOKEN", None)
    env.pop("TA3000_FINAM_API_BASE_URL", None)

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "run_f1e_real_broker_process.py"),
            "--output-root",
            str(output_root),
            "--run-id",
            run_id,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    assert completed.returncode == 1

    failure_artifact = output_root / run_id / "failure.json"
    assert failure_artifact.exists(), "expected failure artifact for fail-closed missing-secrets path"
    payload = json.loads(failure_artifact.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["failure_stage"] == "secret_validation"
    assert "TA3000_STOCKSHARP_API_KEY" in payload["error"]
    assert payload["route_signal"] == "worker:phase-only"
