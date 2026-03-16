from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.app.contracts import BrokerEvent, BrokerFill, BrokerOrder, RiskSnapshot


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "app" / "fixtures" / "contracts"
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "app" / "contracts" / "schemas"


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_broker_order_round_trip() -> None:
    payload = _load_json(FIXTURES / "broker_order.v1.json")
    contract = BrokerOrder.from_dict(payload)
    assert contract.to_dict() == payload


def test_broker_fill_round_trip() -> None:
    payload = _load_json(FIXTURES / "broker_fill.v1.json")
    contract = BrokerFill.from_dict(payload)
    assert contract.to_dict() == payload


def test_risk_snapshot_round_trip() -> None:
    payload = _load_json(FIXTURES / "risk_snapshot.v1.json")
    contract = RiskSnapshot.from_dict(payload)
    assert contract.to_dict() == payload


def test_broker_event_round_trip() -> None:
    payload = _load_json(FIXTURES / "broker_event.v1.json")
    contract = BrokerEvent.from_dict(payload)
    assert contract.to_dict() == payload


def test_phase2d_schema_snapshots_exist() -> None:
    expected = {
        "broker_order.v1.json": "contracts/broker_order.v1.json",
        "broker_fill.v1.json": "contracts/broker_fill.v1.json",
        "risk_snapshot.v1.json": "contracts/risk_snapshot.v1.json",
        "broker_event.v1.json": "contracts/broker_event.v1.json",
    }
    for file_name, schema_id in expected.items():
        payload = _load_json(SCHEMAS / file_name)
        assert payload["$id"] == schema_id
