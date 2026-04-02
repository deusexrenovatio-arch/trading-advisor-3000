from __future__ import annotations

from trading_advisor_3000.product_plane.runtime.ops import (
    build_live_bridge_from_env,
    build_runtime_operational_snapshot,
    render_runtime_operational_metrics,
)


def _base_env() -> dict[str, str]:
    return {
        "TA3000_ENVIRONMENT": "staging-live-sim",
        "TA3000_ENABLE_LIVE_EXECUTION": "1",
        "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
        "TA3000_ENABLE_QUIK_CONNECTOR": "1",
        "TA3000_ENABLE_FINAM_TRANSPORT": "1",
        "TA3000_ENFORCE_LIVE_SECRETS": "1",
        "TA3000_RETRY_MAX_ATTEMPTS": "4",
        "TA3000_RETRY_BACKOFF_SECONDS": "0.1",
    }


def test_phase6_profile_is_fail_closed_when_live_flags_are_not_explicitly_set() -> None:
    env = {
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-token-001",
    }

    snapshot = build_runtime_operational_snapshot(env)
    preflight_errors = set(snapshot["bridge"]["preflight_errors"])

    assert snapshot["ready"] is False
    assert snapshot["bridge"]["status"] == "degraded"
    assert "live_execution_disabled" in preflight_errors
    assert "stocksharp_bridge_disabled" in preflight_errors
    assert "quik_connector_disabled" in preflight_errors
    assert "finam_transport_disabled" in preflight_errors


def test_phase6_runtime_snapshot_is_not_ready_when_required_secrets_are_missing() -> None:
    env = {
        **_base_env(),
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-only",
    }

    snapshot = build_runtime_operational_snapshot(env)

    assert snapshot["ready"] is False
    bridge = snapshot["bridge"]
    assert bridge["status"] == "degraded"
    assert bridge["secrets_policy"]["is_ready"] is False
    assert bridge["secrets_policy"]["missing_secret_names"] == ["TA3000_FINAM_API_TOKEN"]


def test_phase6_runtime_snapshot_and_metrics_are_ready_when_secrets_exist() -> None:
    env = {
        **_base_env(),
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-token-001",
    }

    snapshot = build_runtime_operational_snapshot(env)
    metrics = render_runtime_operational_metrics(snapshot)

    assert snapshot["ready"] is True
    assert "ta3000_live_bridge_ready 1" in metrics
    assert "ta3000_live_bridge_missing_secrets_total 0" in metrics
    assert "ta3000_live_bridge_retry_max_attempts 4" in metrics


def test_phase6_bridge_builder_respects_retry_policy_from_environment() -> None:
    env = {
        **_base_env(),
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-token-001",
    }
    bridge = build_live_bridge_from_env(env)
    health = bridge.health()

    assert health["retry_policy"]["max_attempts"] == 4
    assert health["retry_policy"]["backoff_seconds"] == 0.1
    assert health["secrets_policy"]["is_ready"] is True
