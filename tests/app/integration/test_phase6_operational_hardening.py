from __future__ import annotations

import pytest

from trading_advisor_3000.app.contracts import Mode, OrderIntent, PositionSnapshot
from trading_advisor_3000.app.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionFeatureFlags,
    LiveExecutionRetryExhaustedError,
    LiveExecutionRetryPolicy,
    StockSharpSidecarStub,
)
from trading_advisor_3000.app.execution.broker_sync import BrokerSyncEngine, ControlledLiveExecutionEngine


def _live_intent(*, intent_id: str, qty: int = 1) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id=f"SIG-{intent_id}",
        mode=Mode.LIVE,
        broker_adapter="stocksharp-sidecar-stub",
        action="buy",
        contract_id="BR-6.26",
        qty=qty,
        price=82.5,
        stop_price=81.8,
        created_at="2026-03-18T12:00:00Z",
    )


def _expected_position(*, qty: int, avg_price: float) -> PositionSnapshot:
    return PositionSnapshot(
        position_key="LIVE-ACC:BR-6.26:live",
        account_id="LIVE-ACC",
        contract_id="BR-6.26",
        mode=Mode.LIVE,
        qty=qty,
        avg_price=avg_price,
        as_of_ts="2026-03-18T12:00:10Z",
    )


def _build_engine(
    *,
    sidecar: StockSharpSidecarStub,
    retry_max_attempts: int = 3,
) -> tuple[BrokerSyncEngine, ControlledLiveExecutionEngine]:
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=LiveExecutionFeatureFlags(
            enable_live_execution=True,
            enable_stocksharp_bridge=True,
            enable_quik_connector=True,
            enable_finam_transport=True,
            enforce_live_secrets=True,
            environment="staging-live-sim",
        ),
        retry_policy=LiveExecutionRetryPolicy(max_attempts=retry_max_attempts, backoff_seconds=0.0),
        env={
            "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
            "TA3000_FINAM_API_TOKEN": "finam-token-001",
        },
    )
    sync = BrokerSyncEngine(account_id="LIVE-ACC")
    return sync, ControlledLiveExecutionEngine(bridge=bridge, sync_engine=sync)


def test_phase6_retry_exhaustion_does_not_leave_ghost_intent_state() -> None:
    sidecar = StockSharpSidecarStub()
    sidecar.inject_transient_failures(operation="submit_order_intent", failures=3)
    sync, engine = _build_engine(sidecar=sidecar, retry_max_attempts=2)

    with pytest.raises(LiveExecutionRetryExhaustedError):
        engine.submit_intents(
            [_live_intent(intent_id="INT-PHASE6-FAILURE-1")],
            submitted_at="2026-03-18T12:00:01Z",
        )

    assert sync.list_registered_intents() == []
    assert sync.list_broker_orders() == []
    assert sync.to_dict()["stats"]["registered_intents"] == 0


def test_phase6_hardened_cycle_surfaces_recovery_plan_for_non_happy_state() -> None:
    sidecar = StockSharpSidecarStub()
    _, engine = _build_engine(sidecar=sidecar, retry_max_attempts=3)
    report = engine.run_hardened_cycle(
        intents=[_live_intent(intent_id="INT-PHASE6-HARDENED-1")],
        submitted_at="2026-03-18T12:10:01Z",
        expected_positions=[_expected_position(qty=1, avg_price=82.5)],
    )

    action_reasons = {item.trigger_reason for item in report.recovery_plan.actions}
    assert report.cycle.reconciliation.is_clean is False
    assert report.recovery_plan.incidents_total >= 1
    assert report.recovery_plan.escalation_required is True
    assert "missing_position" in action_reasons


def test_phase6_controlled_live_replay_reuses_submission_ack_idempotently() -> None:
    sidecar = StockSharpSidecarStub()
    sync, engine = _build_engine(sidecar=sidecar, retry_max_attempts=3)
    intent = _live_intent(intent_id="INT-PHASE6-IDEMP-2")

    first = engine.submit_intents([intent], submitted_at="2026-03-18T12:20:01Z")
    second = engine.submit_intents([intent], submitted_at="2026-03-18T12:20:02Z")

    assert first[0]["idempotent_reuse"] is False
    assert second[0]["idempotent_reuse"] is True
    assert sidecar.health()["queued_intents"] == 1
    assert sync.to_dict()["stats"]["registered_intents"] == 1
