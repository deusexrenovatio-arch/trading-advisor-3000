from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.app.contracts import CanonicalBar, DecisionCandidate, OrderIntent


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


def test_signal_candidate_rejects_unsupported_mode() -> None:
    payload = _load_json(FIXTURES / "signal_candidate.v1.json")
    payload["mode"] = "advisory"
    with pytest.raises(ValueError):
        DecisionCandidate.from_dict(payload)


def test_order_intent_rejects_non_positive_quantity() -> None:
    payload = _load_json(FIXTURES / "order_intent.v1.json")
    payload["quantity"] = 0
    with pytest.raises(ValueError):
        OrderIntent.from_dict(payload)


def test_contract_schema_snapshots_exist() -> None:
    expected = {
        "canonical_bar.v1.json": "contracts/canonical_bar.v1.json",
        "signal_candidate.v1.json": "contracts/signal_candidate.v1.json",
        "order_intent.v1.json": "contracts/order_intent.v1.json",
    }
    for file_name, schema_id in expected.items():
        payload = _load_json(SCHEMAS / file_name)
        assert payload["$id"] == schema_id
