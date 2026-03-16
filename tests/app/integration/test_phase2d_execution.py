from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.app.contracts import Mode, OrderIntent
from trading_advisor_3000.app.execution.adapters import StockSharpSidecarStub
from trading_advisor_3000.app.execution.intents import PaperBrokerEngine


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "app" / "fixtures" / "contracts"


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_phase2d_paper_mode_runs_from_intent_to_position_snapshot() -> None:
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
    assert result.position_snapshot.quantity == 1
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


def test_phase2d_rejects_flat_order_intent() -> None:
    payload = _load_json(FIXTURES / "order_intent.v1.json")
    payload["side"] = "flat"
    with pytest.raises(ValueError):
        OrderIntent.from_dict(payload)
