from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.app.contracts import (
    CanonicalBar,
    DecisionCandidate,
    DecisionPublication,
    OrderIntent,
    PositionSnapshot,
    RuntimeSignal,
    SignalEvent,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "app" / "fixtures" / "contracts"
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "app" / "contracts" / "schemas"


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_canonical_bar_fixture_round_trip() -> None:
    payload = _load_json(FIXTURES / "canonical_bar.v1.json")
    contract = CanonicalBar.from_dict(payload)
    assert contract.to_dict() == payload


def test_signal_candidate_fixture_round_trip() -> None:
    payload = _load_json(FIXTURES / "signal_candidate.v1.json")
    contract = DecisionCandidate.from_dict(payload)
    assert contract.to_dict() == payload


def test_order_intent_fixture_round_trip() -> None:
    payload = _load_json(FIXTURES / "order_intent.v1.json")
    contract = OrderIntent.from_dict(payload)
    assert contract.to_dict() == payload


def test_decision_publication_fixture_round_trip() -> None:
    payload = _load_json(FIXTURES / "decision_publication.v1.json")
    contract = DecisionPublication.from_dict(payload)
    assert contract.to_dict() == payload


def test_position_snapshot_fixture_round_trip() -> None:
    payload = _load_json(FIXTURES / "position_snapshot.v1.json")
    contract = PositionSnapshot.from_dict(payload)
    assert contract.to_dict() == payload


def test_runtime_signal_fixture_round_trip() -> None:
    payload = _load_json(FIXTURES / "runtime_signal.v1.json")
    contract = RuntimeSignal.from_dict(payload)
    assert contract.to_dict() == payload


def test_signal_event_fixture_round_trip() -> None:
    payload = _load_json(FIXTURES / "signal_event.v1.json")
    contract = SignalEvent.from_dict(payload)
    assert contract.to_dict() == payload


def test_signal_candidate_rejects_unsupported_mode() -> None:
    payload = _load_json(FIXTURES / "signal_candidate.v1.json")
    payload["mode"] = "advisory"
    with pytest.raises(ValueError):
        DecisionCandidate.from_dict(payload)


def test_signal_candidate_rejects_flat_side() -> None:
    payload = _load_json(FIXTURES / "signal_candidate.v1.json")
    payload["side"] = "flat"
    with pytest.raises(ValueError):
        DecisionCandidate.from_dict(payload)


def test_order_intent_rejects_non_positive_quantity() -> None:
    payload = _load_json(FIXTURES / "order_intent.v1.json")
    payload["qty"] = 0
    with pytest.raises(ValueError):
        OrderIntent.from_dict(payload)


def test_order_intent_rejects_unsupported_action() -> None:
    payload = _load_json(FIXTURES / "order_intent.v1.json")
    payload["action"] = "hold"
    with pytest.raises(ValueError):
        OrderIntent.from_dict(payload)


def test_order_intent_rejects_quantity_string() -> None:
    payload = _load_json(FIXTURES / "order_intent.v1.json")
    payload["qty"] = "1"
    with pytest.raises(ValueError):
        OrderIntent.from_dict(payload)


def test_signal_candidate_rejects_extra_field() -> None:
    payload = _load_json(FIXTURES / "signal_candidate.v1.json")
    payload["unexpected"] = "value"
    with pytest.raises(ValueError):
        DecisionCandidate.from_dict(payload)


def test_canonical_bar_rejects_extra_field() -> None:
    payload = _load_json(FIXTURES / "canonical_bar.v1.json")
    payload["unexpected"] = "value"
    with pytest.raises(ValueError):
        CanonicalBar.from_dict(payload)


def test_canonical_bar_rejects_string_volume() -> None:
    payload = _load_json(FIXTURES / "canonical_bar.v1.json")
    payload["volume"] = "1500"
    with pytest.raises(ValueError):
        CanonicalBar.from_dict(payload)


def test_position_snapshot_rejects_extra_field() -> None:
    payload = _load_json(FIXTURES / "position_snapshot.v1.json")
    payload["unexpected"] = "value"
    with pytest.raises(ValueError):
        PositionSnapshot.from_dict(payload)


def test_contract_schema_snapshots_exist() -> None:
    expected = {
        "canonical_bar.v1.json": "contracts/canonical_bar.v1.json",
        "signal_candidate.v1.json": "contracts/signal_candidate.v1.json",
        "order_intent.v1.json": "contracts/order_intent.v1.json",
        "decision_publication.v1.json": "contracts/decision_publication.v1.json",
        "position_snapshot.v1.json": "contracts/position_snapshot.v1.json",
        "runtime_signal.v1.json": "contracts/runtime_signal.v1.json",
        "signal_event.v1.json": "contracts/signal_event.v1.json",
    }
    for file_name, schema_id in expected.items():
        payload = _load_json(SCHEMAS / file_name)
        assert payload["$id"] == schema_id


def test_order_intent_schema_action_enum_snapshot() -> None:
    schema = _load_json(SCHEMAS / "order_intent.v1.json")
    assert schema["properties"]["action"]["enum"] == ["buy", "sell"]


def test_publication_schema_contains_traceability_fields() -> None:
    schema = _load_json(SCHEMAS / "decision_publication.v1.json")
    required = set(schema["required"])
    assert {"publication_id", "publication_type", "signal_id"} <= required


def test_signal_event_rejects_non_object_payload() -> None:
    payload = _load_json(FIXTURES / "signal_event.v1.json")
    payload["payload_json"] = "bad-payload"
    with pytest.raises(ValueError):
        SignalEvent.from_dict(payload)
