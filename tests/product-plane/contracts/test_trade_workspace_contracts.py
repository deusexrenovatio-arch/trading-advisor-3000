from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from trading_advisor_3000.product_plane.contracts.trade_workspace import (
    ChangeTrade,
    CreateTrade,
    Entry,
    EntryInput,
    UnknownDecimal,
    journal_json_schema,
)

ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src/trading_advisor_3000/product_plane/contracts/schemas"
FIXTURES = ROOT / "tests/product-plane/fixtures/contracts"


@pytest.mark.parametrize("model,name", [(CreateTrade, "create"), (ChangeTrade, "change")])
def test_frozen_schema_and_fixture(model, name):
    family = json.loads((SCHEMAS / "journal.v1.json").read_text(encoding="utf-8"))
    assert family == journal_json_schema()
    schema = {"$ref": f"#/$defs/{model.__name__}", "$defs": family["$defs"]}
    fixture = json.loads((FIXTURES / f"journal_{name}.v1.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(fixture)
    assert model.model_validate(fixture).schema_version == 1


def test_schema_rejects_unknown_without_reason_and_negative_price():
    family = json.loads((SCHEMAS / "journal.v1.json").read_text(encoding="utf-8"))
    schema = {"$ref": "#/$defs/UnknownDecimal", "$defs": family["$defs"]}
    validator = Draft202012Validator(schema)
    assert not validator.is_valid({"value": None, "reason": None})
    assert not validator.is_valid({"value": "-1", "reason": None})


@pytest.mark.parametrize("value", [4170, 4170.0, "NaN", "Infinity", "-1", "1e4", "1.12345678901"])
def test_decimal_rejects_float_nonfinite_negative_and_excess_precision(value):
    with pytest.raises(ValidationError):
        UnknownDecimal(value=value, reason=None)


def test_unknown_requires_reason_and_numbers_canonicalize():
    with pytest.raises(ValidationError):
        UnknownDecimal(value=None, reason=None)
    with pytest.raises(ValidationError):
        UnknownDecimal(value="1", reason="unknown")
    assert UnknownDecimal(value="04170.00", reason=None).value == "4170"


def test_unknown_fields_and_naive_time_rejected():
    fixture = json.loads((FIXTURES / "journal_create.v1.json").read_text(encoding="utf-8"))
    fixture["unexpected"] = True
    with pytest.raises(ValidationError):
        CreateTrade.model_validate(fixture)
    del fixture["unexpected"]
    fixture["event_at"] = "2026-10-12T12:00:00"
    with pytest.raises(ValidationError):
        CreateTrade.model_validate(fixture)


def assert_schema_dto_agree(model, payload, accepted):
    family = json.loads((SCHEMAS / "journal.v1.json").read_text(encoding="utf-8"))
    schema = {"$ref": f"#/$defs/{model.__name__}", "$defs": family["$defs"]}
    assert Draft202012Validator(schema).is_valid(payload) is accepted
    if accepted:
        model.model_validate(payload)
    else:
        with pytest.raises(ValidationError):
            model.model_validate(payload)


@pytest.mark.parametrize(
    "links,accepted",
    [
        ({}, True),
        ({"related_trade_id": None, "relation": None}, True),
        ({"related_trade_id": "prior", "relation": "reentry"}, True),
        ({"related_trade_id": "prior", "relation": "roll"}, True),
        ({"related_trade_id": "prior"}, False),
        ({"related_trade_id": "prior", "relation": None}, False),
        ({"relation": "reentry"}, False),
        ({"related_trade_id": None, "relation": "roll"}, False),
    ],
)
def test_linked_episode_schema_and_dto_parity(links, accepted):
    payload = json.loads((FIXTURES / "journal_create.v1.json").read_text(encoding="utf-8"))
    for field in ("related_trade_id", "relation"):
        payload["card"].pop(field, None)
    payload["card"].update(links)
    assert_schema_dto_agree(CreateTrade, payload, accepted)


@pytest.mark.parametrize("model", [EntryInput, Entry])
@pytest.mark.parametrize(
    "kind,execution_state,accepted",
    [
        ("note", "omitted", True),
        ("note", "null", True),
        ("note", "provided", False),
        ("execution", "omitted", False),
        ("execution", "null", False),
        ("execution", "provided", True),
    ],
)
def test_execution_kind_schema_and_dto_parity(model, kind, execution_state, accepted):
    unknown = {"value": None, "reason": "not_provided"}
    payload = {
        "kind": kind,
        "text": "synthetic entry",
        "event_at": "2026-10-18T09:00:00Z",
        "source": {"source_type": "manual", "original_text": "synthetic entry"},
    }
    if execution_state != "omitted":
        payload["execution"] = (
            None
            if execution_state == "null"
            else {
                "venue": "MOEX",
                "contract_code": "TEST-GOLD-DO-NOT-TRADE",
                "account": unknown,
                "action": "increase",
                "contracts": 1,
                "price": unknown,
                "fee": unknown,
                "currency": unknown,
            }
        )
    if model is Entry:
        payload.update(
            entry_id="entry",
            trade_id="trade",
            revision=1,
            sequence=1,
            recorded_at="2026-10-18T09:00:00Z",
        )
    assert_schema_dto_agree(model, payload, accepted)
