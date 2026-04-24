from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.contracts import Mode, OrderIntent, PositionSnapshot
from trading_advisor_3000.product_plane.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionDisabledError,
    LiveExecutionFeatureFlags,
    StockSharpSidecarStub,
)
from trading_advisor_3000.product_plane.execution.broker_sync import BrokerSyncEngine, ControlledLiveExecutionEngine


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
        created_at="2026-03-17T13:00:00Z",
    )


def _expected_position(*, qty: int, avg_price: float) -> PositionSnapshot:
    return PositionSnapshot(
        position_key="LIVE-ACC:BR-6.26:live",
        account_id="LIVE-ACC",
        contract_id="BR-6.26",
        mode=Mode.LIVE,
        qty=qty,
        avg_price=avg_price,
        as_of_ts="2026-03-17T13:00:10Z",
    )


def test_controlled_live_execution_controlled_live_cycle_syncs_and_reconciles_cleanly() -> None:
    sidecar = StockSharpSidecarStub()
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=LiveExecutionFeatureFlags(
            enable_live_execution=True,
            enable_stocksharp_bridge=True,
            enable_quik_connector=True,
            enable_finam_transport=True,
            environment="staging-live-sim",
        ),
    )
    sync = BrokerSyncEngine(account_id="LIVE-ACC")
    engine = ControlledLiveExecutionEngine(bridge=bridge, sync_engine=sync)

    intent = _live_intent(intent_id="INT-LIVE-CYCLE-1")
    acks = engine.submit_intents([intent], submitted_at="2026-03-17T13:00:01Z")
    external_order_id = str(acks[0]["external_order_id"])
    sidecar.push_broker_update(
        external_order_id=external_order_id,
        state="filled",
        event_ts="2026-03-17T13:00:02Z",
    )
    sidecar.push_broker_fill(
        external_order_id=external_order_id,
        fill_id="FILL-CYCLE-1",
        qty=1,
        price=82.55,
        fill_ts="2026-03-17T13:00:03Z",
    )

    sync_stats = engine.poll_broker_sync()
    report = engine.reconcile(expected_positions=[_expected_position(qty=1, avg_price=82.55)])

    assert sync_stats == {"updates": 1, "fills": 1}
    assert report.is_clean is True
    assert report.orders_matched == 1
    assert report.fills_matched == 1
    assert report.position_report.matched == 1


def test_controlled_live_execution_controlled_live_cycle_rejects_live_when_flags_disabled() -> None:
    sidecar = StockSharpSidecarStub()
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=LiveExecutionFeatureFlags(),
    )
    sync = BrokerSyncEngine(account_id="LIVE-ACC")
    engine = ControlledLiveExecutionEngine(bridge=bridge, sync_engine=sync)

    with pytest.raises(LiveExecutionDisabledError):
        engine.submit_intents(
            [_live_intent(intent_id="INT-LIVE-CYCLE-2")],
            submitted_at="2026-03-17T13:05:01Z",
        )
    assert sync.list_registered_intents() == []
    assert sync.to_dict()["stats"]["registered_intents"] == 0
    assert sync.list_incidents() == []


def test_controlled_live_execution_replace_flow_is_supported_end_to_end() -> None:
    sidecar = StockSharpSidecarStub()
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=LiveExecutionFeatureFlags(
            enable_live_execution=True,
            enable_stocksharp_bridge=True,
            enable_quik_connector=True,
            enable_finam_transport=True,
            environment="staging-live-sim",
        ),
    )
    sync = BrokerSyncEngine(account_id="LIVE-ACC")
    engine = ControlledLiveExecutionEngine(bridge=bridge, sync_engine=sync)

    intent = _live_intent(intent_id="INT-LIVE-CYCLE-3", qty=1)
    acks = engine.submit_intents([intent], submitted_at="2026-03-17T13:10:00Z")
    assert acks[0]["state"] == "submitted"

    replace_ack = engine.replace_intent(
        intent_id=intent.intent_id,
        new_qty=2,
        new_price=82.8,
        replaced_at="2026-03-17T13:10:01Z",
    )
    assert replace_ack["state"] == "replaced"

    sync_stats = engine.poll_broker_sync()
    assert sync_stats == {"updates": 1, "fills": 0}

    orders = sync.list_broker_orders()
    assert len(orders) == 1
    assert orders[0].state == "replaced"
    assert sync.list_incidents() == []
    assert sync.list_registered_intents()[0].qty == 2
