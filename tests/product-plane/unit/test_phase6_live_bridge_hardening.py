from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.contracts import Mode, OrderIntent
from trading_advisor_3000.product_plane.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionDisabledError,
    LiveExecutionFeatureFlags,
    LiveExecutionRetryExhaustedError,
    LiveExecutionRetryPolicy,
    StockSharpSidecarStub,
)


def _live_intent(intent_id: str) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id=f"SIG-{intent_id}",
        mode=Mode.LIVE,
        broker_adapter="stocksharp-sidecar-stub",
        action="buy",
        contract_id="BR-6.26",
        qty=1,
        price=82.5,
        stop_price=81.8,
        created_at="2026-03-18T10:00:00Z",
    )


def _full_live_flags(*, enforce_live_secrets: bool = True) -> LiveExecutionFeatureFlags:
    return LiveExecutionFeatureFlags(
        enable_live_execution=True,
        enable_stocksharp_bridge=True,
        enable_quik_connector=True,
        enable_finam_transport=True,
        enforce_live_secrets=enforce_live_secrets,
        environment="staging-live-sim",
    )


def _env_with_secrets() -> dict[str, str]:
    return {
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-token-001",
    }


def test_phase6_submit_retries_and_succeeds_before_budget_exhaustion() -> None:
    sidecar = StockSharpSidecarStub()
    sidecar.inject_transient_failures(operation="submit_order_intent", failures=2)
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=_full_live_flags(),
        retry_policy=LiveExecutionRetryPolicy(max_attempts=3, backoff_seconds=0.0),
        env=_env_with_secrets(),
    )

    ack = bridge.submit_order_intent(
        _live_intent("INT-PHASE6-RETRY-OK"),
        accepted_at="2026-03-18T10:00:01Z",
    )

    assert ack["accepted"] is True
    assert ack["state"] == "submitted"
    assert sidecar.health()["queued_intents"] == 1


def test_phase6_submit_raises_retry_exhausted_when_budget_is_exceeded() -> None:
    sidecar = StockSharpSidecarStub()
    sidecar.inject_transient_failures(operation="submit_order_intent", failures=5)
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=_full_live_flags(),
        retry_policy=LiveExecutionRetryPolicy(max_attempts=2, backoff_seconds=0.0),
        env=_env_with_secrets(),
    )

    with pytest.raises(LiveExecutionRetryExhaustedError) as exc_info:
        bridge.submit_order_intent(
            _live_intent("INT-PHASE6-RETRY-FAIL"),
            accepted_at="2026-03-18T10:01:00Z",
        )

    assert exc_info.value.operation == "submit_order_intent"
    assert exc_info.value.attempts == 2
    assert sidecar.health()["queued_intents"] == 0


def test_phase6_live_submit_is_blocked_when_secrets_policy_is_not_satisfied() -> None:
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=_full_live_flags(enforce_live_secrets=True),
        retry_policy=LiveExecutionRetryPolicy(max_attempts=2, backoff_seconds=0.0),
        env={"TA3000_STOCKSHARP_API_KEY": "only-one-secret"},
    )

    with pytest.raises(LiveExecutionDisabledError):
        bridge.submit_order_intent(
            _live_intent("INT-PHASE6-MISSING-SECRET"),
            accepted_at="2026-03-18T10:02:00Z",
        )

    health = bridge.health()
    assert health["status"] == "degraded"
    assert health["secrets_policy"]["is_ready"] is False
    assert "TA3000_FINAM_API_TOKEN" in health["secrets_policy"]["missing_secret_names"]


def test_phase6_bridge_health_redacts_secret_values() -> None:
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=_full_live_flags(enforce_live_secrets=True),
        retry_policy=LiveExecutionRetryPolicy(max_attempts=2, backoff_seconds=0.0),
        env=_env_with_secrets(),
    )

    health = bridge.health()
    probes = {item["name"]: item for item in health["secrets_policy"]["probes"]}
    assert probes["TA3000_STOCKSHARP_API_KEY"]["present"] is True
    assert probes["TA3000_FINAM_API_TOKEN"]["present"] is True
    assert probes["TA3000_STOCKSHARP_API_KEY"]["redacted_value"] != _env_with_secrets()["TA3000_STOCKSHARP_API_KEY"]
    assert probes["TA3000_FINAM_API_TOKEN"]["redacted_value"] != _env_with_secrets()["TA3000_FINAM_API_TOKEN"]
