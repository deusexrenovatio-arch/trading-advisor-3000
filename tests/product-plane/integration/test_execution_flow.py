from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts import Mode, OrderIntent
from trading_advisor_3000.product_plane.execution.adapters import StockSharpSidecarStub
from trading_advisor_3000.product_plane.execution.intents import PaperBrokerEngine


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "product-plane" / "fixtures" / "contracts"


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_execution_flow_paper_mode_runs_from_intent_to_position_snapshot() -> None:
    intent = OrderIntent.from_dict(_load_json(FIXTURES / "order_intent.v1.json"))
    broker = PaperBrokerEngine(account_id="PAPER-01")

    result = broker.execute_intent(
        intent,
        fill_price=82.50,
        fill_ts="2026-03-16T10:17:02Z",
        fee=0.04,
    )
    assert result.position_snapshot.contract_id == intent.contract_id
    assert result.position_snapshot.mode == Mode.PAPER
    assert result.position_snapshot.qty == 1
    assert result.position_snapshot.avg_price == 82.50

    events_before = len(broker.list_broker_events())
    replay = broker.execute_intent(intent, fill_price=90.0, fill_ts="2026-03-16T10:17:30Z")
    events_after = len(broker.list_broker_events())
    assert replay.broker_order.broker_order_id == result.broker_order.broker_order_id
    assert events_before == events_after

    event_types = {event.event_type for event in broker.list_broker_events()}
    assert {"intent_received", "order_submitted", "order_filled", "position_updated"} <= event_types

    sidecar = StockSharpSidecarStub()
    ack = sidecar.submit_order_intent(intent)
    assert ack["accepted"] is True
    assert ack["intent_id"] == intent.intent_id
    assert ack["state"] == "submitted"

    replaced = sidecar.replace_order_intent(
        intent_id=intent.intent_id,
        new_qty=2,
        new_price=82.60,
        replaced_at="2026-03-16T10:17:05Z",
    )
    assert replaced["state"] == "replaced"
    canceled = sidecar.cancel_order_intent(
        intent_id=intent.intent_id,
        canceled_at="2026-03-16T10:17:10Z",
    )
    assert canceled["state"] == "canceled"

    sidecar.push_broker_update(
        external_order_id=str(ack["external_order_id"]),
        state="partially_filled",
        event_ts="2026-03-16T10:17:06Z",
    )
    sidecar.push_broker_fill(
        external_order_id=str(ack["external_order_id"]),
        fill_id="FILL-1001",
        qty=1,
        price=82.55,
        fill_ts="2026-03-16T10:17:07Z",
        fee=0.01,
    )
    assert len(sidecar.list_broker_updates()) >= 2
    assert len(sidecar.list_broker_fills()) == 1


def test_execution_flow_rejects_unsupported_order_action() -> None:
    payload = _load_json(FIXTURES / "order_intent.v1.json")
    payload["action"] = "hold"
    with pytest.raises(ValueError):
        OrderIntent.from_dict(payload)
