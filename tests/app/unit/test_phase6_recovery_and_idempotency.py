from __future__ import annotations

from trading_advisor_3000.app.contracts import Mode, OrderIntent, PositionSnapshot
from trading_advisor_3000.app.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionFeatureFlags,
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
        created_at="2026-03-18T11:00:00Z",
    )


def _expected_position(*, qty: int, avg_price: float) -> PositionSnapshot:
    return PositionSnapshot(
        position_key="LIVE-ACC:BR-6.26:live",
        account_id="LIVE-ACC",
        contract_id="BR-6.26",
        mode=Mode.LIVE,
        qty=qty,
        avg_price=avg_price,
        as_of_ts="2026-03-18T11:00:10Z",
    )


def _build_engine() -> tuple[StockSharpSidecarStub, BrokerSyncEngine, ControlledLiveExecutionEngine]:
    sidecar = StockSharpSidecarStub()
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
        retry_policy=LiveExecutionRetryPolicy(max_attempts=3, backoff_seconds=0.0),
        env={
            "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
            "TA3000_FINAM_API_TOKEN": "finam-token-001",
        },
    )
    sync = BrokerSyncEngine(account_id="LIVE-ACC")
    return sidecar, sync, ControlledLiveExecutionEngine(bridge=bridge, sync_engine=sync)


def test_phase6_submit_reuses_existing_ack_without_duplicate_side_effects() -> None:
    sidecar, sync, engine = _build_engine()
    intent = _live_intent(intent_id="INT-PHASE6-IDEMP-1")

    first = engine.submit_intents([intent], submitted_at="2026-03-18T11:00:01Z")
    second = engine.submit_intents([intent], submitted_at="2026-03-18T11:00:02Z")

    assert first[0]["idempotent_reuse"] is False
    assert second[0]["idempotent_reuse"] is True
    assert first[0]["external_order_id"] == second[0]["external_order_id"]
    assert sidecar.health()["queued_intents"] == 1
    assert sync.to_dict()["stats"]["registered_intents"] == 1


def test_phase6_recovery_plan_contains_actions_for_quantity_mismatch() -> None:
    sidecar, _, engine = _build_engine()
    intent = _live_intent(intent_id="INT-PHASE6-RECOVERY-1", qty=1)
    acks = engine.submit_intents([intent], submitted_at="2026-03-18T11:10:01Z")
    external_order_id = str(acks[0]["external_order_id"])
    sidecar.push_broker_update(
        external_order_id=external_order_id,
        state="filled",
        event_ts="2026-03-18T11:10:02Z",
    )
    sidecar.push_broker_fill(
        external_order_id=external_order_id,
        fill_id="FILL-PHASE6-RECOVERY-1",
        qty=1,
        price=82.55,
        fill_ts="2026-03-18T11:10:03Z",
    )
    engine.poll_broker_sync()

    plan = engine.build_recovery_plan(expected_positions=[_expected_position(qty=2, avg_price=82.55)])
    actions = [item.to_dict() for item in plan.actions]
    trigger_reasons = {str(item["trigger_reason"]) for item in actions}

    assert plan.incidents_total >= 1
    assert plan.escalation_required is True
    assert "quantity_mismatch" in trigger_reasons
    assert actions[0]["trigger_reason"] == "high_severity_incident"


def test_phase6_recovery_plan_maps_sync_incidents_without_falling_back_to_uncovered() -> None:
    sidecar, _, engine = _build_engine()
    intent = _live_intent(intent_id="INT-PHASE6-RECOVERY-2")
    acks = engine.submit_intents([intent], submitted_at="2026-03-18T11:20:01Z")
    external_order_id = str(acks[0]["external_order_id"])
    sidecar.push_broker_update(
        external_order_id=external_order_id,
        state="unknown_broker_state",
        event_ts="2026-03-18T11:20:02Z",
    )
    engine.poll_broker_sync()

    plan = engine.build_recovery_plan(expected_positions=[])
    action_reasons = {item.trigger_reason for item in plan.actions}

    assert "broker_update_unsupported_state" in action_reasons
    assert "broker_update_unsupported_state" not in plan.uncovered_reasons
