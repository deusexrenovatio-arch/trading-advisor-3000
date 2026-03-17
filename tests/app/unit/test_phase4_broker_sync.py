from __future__ import annotations

from trading_advisor_3000.app.contracts import Mode, OrderIntent
from trading_advisor_3000.app.execution.broker_sync import BrokerSyncEngine


def _intent(*, intent_id: str, action: str = "buy") -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id="SIG-20260317-2001",
        mode=Mode.LIVE,
        broker_adapter="stocksharp-sidecar-stub",
        action=action,
        contract_id="BR-6.26",
        qty=2,
        price=82.5,
        stop_price=81.8,
        created_at="2026-03-17T11:00:00Z",
    )


def test_broker_sync_builds_order_fill_position_state() -> None:
    engine = BrokerSyncEngine(account_id="LIVE-01")
    intent = _intent(intent_id="INT-SYNC-1")
    engine.register_intent(intent)
    order = engine.record_submission_ack(
        intent_id=intent.intent_id,
        ack={
            "accepted": True,
            "state": "submitted",
            "external_order_id": "EXT-1001",
        },
        acknowledged_at="2026-03-17T11:00:01Z",
    )

    assert order is not None
    engine.ingest_broker_updates(
        [
            {
                "external_order_id": "EXT-1001",
                "state": "partially_filled",
                "event_ts": "2026-03-17T11:00:02Z",
                "payload": {"source": "quik"},
            }
        ]
    )
    engine.ingest_broker_fills(
        [
            {
                "external_order_id": "EXT-1001",
                "fill_id": "FILL-1001",
                "qty": 1,
                "price": 82.55,
                "fill_ts": "2026-03-17T11:00:03Z",
            },
            {
                "external_order_id": "EXT-1001",
                "fill_id": "FILL-1002",
                "qty": 1,
                "price": 82.60,
                "fill_ts": "2026-03-17T11:00:04Z",
            },
        ]
    )

    orders = engine.list_broker_orders()
    fills = engine.list_broker_fills()
    positions = engine.list_positions()

    assert len(orders) == 1
    assert orders[0].state == "filled"
    assert len(fills) == 2
    assert len(positions) == 1
    assert positions[0].qty == 2
    assert round(positions[0].avg_price, 4) == 82.575
    assert engine.to_dict()["stats"]["incidents"] == 0


def test_broker_sync_is_idempotent_for_duplicate_fill_ids() -> None:
    engine = BrokerSyncEngine(account_id="LIVE-01")
    intent = _intent(intent_id="INT-SYNC-2")
    engine.register_intent(intent)
    engine.record_submission_ack(
        intent_id=intent.intent_id,
        ack={
            "accepted": True,
            "state": "submitted",
            "external_order_id": "EXT-2001",
        },
        acknowledged_at="2026-03-17T11:10:01Z",
    )
    duplicate_fill = {
        "external_order_id": "EXT-2001",
        "fill_id": "FILL-DUP-1",
        "qty": 1,
        "price": 83.0,
        "fill_ts": "2026-03-17T11:10:02Z",
    }
    engine.ingest_broker_fills([duplicate_fill, duplicate_fill])

    assert len(engine.list_broker_fills()) == 1
    assert engine.to_dict()["stats"]["incidents"] == 0


def test_broker_sync_surfaces_incident_for_unmapped_fill() -> None:
    engine = BrokerSyncEngine(account_id="LIVE-01")
    engine.ingest_broker_fills(
        [
            {
                "external_order_id": "EXT-UNKNOWN",
                "fill_id": "FILL-UNKNOWN-1",
                "qty": 1,
                "price": 82.0,
                "fill_ts": "2026-03-17T11:20:00Z",
            }
        ]
    )
    incidents = engine.list_incidents()
    assert len(incidents) == 1
    assert incidents[0].reason == "broker_fill_for_unknown_order"


def test_broker_sync_surfaces_incident_for_invalid_fill_payload() -> None:
    engine = BrokerSyncEngine(account_id="LIVE-01")
    intent = _intent(intent_id="INT-SYNC-3")
    engine.register_intent(intent)
    engine.record_submission_ack(
        intent_id=intent.intent_id,
        ack={
            "accepted": True,
            "state": "submitted",
            "external_order_id": "EXT-3001",
        },
        acknowledged_at="2026-03-17T11:30:01Z",
    )
    engine.ingest_broker_fills(
        [
            {
                "external_order_id": "EXT-3001",
                "fill_id": "FILL-BAD-1",
                "qty": "not-a-number",
                "price": 82.0,
                "fill_ts": "2026-03-17T11:30:02Z",
            }
        ]
    )
    incidents = engine.list_incidents()
    assert len(incidents) == 1
    assert incidents[0].reason == "invalid_broker_fill_payload"


def test_broker_sync_supports_replace_update_and_updates_expected_intent_qty() -> None:
    engine = BrokerSyncEngine(account_id="LIVE-01")
    intent = _intent(intent_id="INT-SYNC-4")
    engine.register_intent(intent)
    engine.record_submission_ack(
        intent_id=intent.intent_id,
        ack={
            "accepted": True,
            "state": "submitted",
            "external_order_id": "EXT-4001",
        },
        acknowledged_at="2026-03-17T11:40:01Z",
    )
    engine.ingest_broker_updates(
        [
            {
                "external_order_id": "EXT-4001",
                "state": "replaced",
                "event_ts": "2026-03-17T11:40:02Z",
                "payload": {"intent_id": intent.intent_id, "new_qty": 3, "new_price": 82.8},
            }
        ]
    )
    engine.ingest_broker_fills(
        [
            {
                "external_order_id": "EXT-4001",
                "fill_id": "FILL-4001",
                "qty": 3,
                "price": 82.8,
                "fill_ts": "2026-03-17T11:40:03Z",
            }
        ]
    )

    assert engine.list_broker_orders()[0].state == "filled"
    assert engine.list_registered_intents()[0].qty == 3
    assert engine.list_registered_intents()[0].price == 82.8
    assert engine.list_incidents() == []
