from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.app.execution import (
    evaluate_phase9_sidecar_preflight,
    load_phase9_sidecar_delivery_manifest,
)


ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = ROOT / "deployment" / "stocksharp-sidecar" / "phase9-sidecar-delivery-manifest.json"


def _base_env() -> dict[str, str]:
    return {
        "TA3000_ENABLE_LIVE_EXECUTION": "1",
        "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
        "TA3000_ENABLE_QUIK_CONNECTOR": "1",
        "TA3000_ENABLE_FINAM_TRANSPORT": "1",
        "TA3000_SIDECAR_TRANSPORT": "http",
        "TA3000_SIDECAR_BASE_URL": "http://127.0.0.1:18081",
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-secret-001",
    }


def test_phase9_sidecar_manifest_freezes_gateway_bundle_contract() -> None:
    spec = load_phase9_sidecar_delivery_manifest(MANIFEST_PATH)

    assert spec.delivery_mode == "pinned-staging-gateway-bundle"
    assert spec.wire_api_version == "v1"
    assert spec.sidecar_route == "stocksharp->quik->finam"
    assert spec.gateway_profile_dir == "deployment/docker/staging-gateway"
    assert spec.readiness_endpoints == ("/health", "/ready", "/metrics")


def test_phase9_sidecar_preflight_is_fail_closed_for_wrong_transport_and_missing_secret() -> None:
    spec = load_phase9_sidecar_delivery_manifest(MANIFEST_PATH)
    env = _base_env()
    env["TA3000_SIDECAR_TRANSPORT"] = "stub"
    del env["TA3000_FINAM_API_TOKEN"]

    report = evaluate_phase9_sidecar_preflight(
        env=env,
        delivery_spec=spec,
        include_rollout_dry_run=False,
    )

    assert report.is_ready is False
    assert "TA3000_SIDECAR_TRANSPORT must be http for the frozen sidecar delivery mode" in report.invalid_env
    assert report.secrets_policy["missing_secret_names"] == ["TA3000_FINAM_API_TOKEN"]


def test_phase9_sidecar_preflight_warns_when_kill_switch_env_is_active() -> None:
    spec = load_phase9_sidecar_delivery_manifest(MANIFEST_PATH)
    env = _base_env()
    env["TA3000_GATEWAY_KILL_SWITCH"] = "1"

    report = evaluate_phase9_sidecar_preflight(
        env=env,
        delivery_spec=spec,
        include_rollout_dry_run=False,
    )

    assert any("TA3000_GATEWAY_KILL_SWITCH is set" in item for item in report.warnings)
