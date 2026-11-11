"""Versioned manual Journal DTOs. No market lookup or execution inference."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic.json_schema import models_json_schema

Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=10000)]
ShortText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
Identifier = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")]
Status = Literal["draft", "planned", "active", "closed", "cancelled", "invalidated"]
AVAILABILITY_SCHEMA: dict[str, Any] = {
    "oneOf": [
        {"properties": {"value": {"type": "string"}, "reason": {"type": "null"}}},
        {"properties": {"value": {"type": "null"}, "reason": {"type": "string"}}},
    ]
}


class DTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @field_validator("schema_version", check_fields=False, mode="before")
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError("schema_version must be an integer")
        return value

    @field_validator("event_at", "recorded_at", "created_at", "updated_at", check_fields=False)
    @classmethod
    def utc_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp requires timezone")
        return value.astimezone(timezone.utc)

    @field_validator(
        "event_at", "recorded_at", "created_at", "updated_at", check_fields=False, mode="before"
    )
    @classmethod
    def timestamp_type(cls, value):
        if not isinstance(value, (str, datetime)):
            raise ValueError("timestamp requires ISO 8601 with timezone")
        return value


class UnknownText(DTO):
    model_config = ConfigDict(extra="forbid", json_schema_extra=AVAILABILITY_SCHEMA)
    value: ShortText | None
    reason: ShortText | None

    @model_validator(mode="after")
    def availability(self):
        if (self.value is None) == (self.reason is None):
            raise ValueError("provide value or an unknown reason, exclusively")
        return self


class UnknownDecimal(DTO):
    model_config = ConfigDict(extra="forbid", json_schema_extra=AVAILABILITY_SCHEMA)
    value: Annotated[str, StringConstraints(pattern=r"^\d{1,20}(\.\d{1,10})?$")] | None
    reason: ShortText | None

    @field_validator("value", mode="before")
    @classmethod
    def decimal_string(cls, value):
        if value is None:
            return None
        if not isinstance(value, str) or not re.fullmatch(r"\d{1,20}(\.\d{1,10})?", value):
            raise ValueError("expected nonnegative decimal string, max 20+10 digits")
        normalized = format(Decimal(value), "f")
        return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized

    @model_validator(mode="after")
    def availability(self):
        if (self.value is None) == (self.reason is None):
            raise ValueError("provide value or an unknown reason, exclusively")
        return self


class Source(DTO):
    source_type: Literal["manual", "import"]
    original_text: Annotated[str, StringConstraints(min_length=1, max_length=65536)]
    reference: ShortText | None = None


class Analysis(DTO):
    symbol: ShortText
    asset: ShortText
    market_type: Literal["spot", "index", "futures", "other"]
    price_unit: ShortText
    timeframe: UnknownText
    data_provider: UnknownText


class Level(DTO):
    price: UnknownDecimal
    approximate: bool = False
    rationale: Text


class TechnicalItem(DTO):
    kind: Literal["pattern", "indicator", "zone", "fibonacci", "correlation", "other"]
    title: ShortText
    description: Text
    timeframe: UnknownText
    lower: UnknownDecimal
    upper: UnknownDecimal

    @model_validator(mode="after")
    def zone_order(self):
        if self.lower.value is not None and self.upper.value is not None:
            if Decimal(self.lower.value) > Decimal(self.upper.value):
                raise ValueError("lower exceeds upper")
        return self


class SharedThesis(DTO):
    thesis_id: Identifier
    title: ShortText
    description: Text
    source: Source


class Card(DTO):
    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {
                    "properties": {
                        "related_trade_id": {"type": "null"},
                        "relation": {"type": "null"},
                    }
                },
                {
                    "required": ["related_trade_id", "relation"],
                    "properties": {
                        "related_trade_id": {"type": "string"},
                        "relation": {"type": "string"},
                    },
                },
            ]
        }
    )
    title: ShortText
    introduction: Text
    analysis: Analysis
    direction: Literal["long", "short"]
    entry: Level
    invalidation: Level
    targets: list[Level] = Field(max_length=100)
    technical_items: list[TechnicalItem] = Field(max_length=100)
    shared_thesis: SharedThesis | None = None
    is_test: bool = False
    related_trade_id: Identifier | None = None
    relation: Literal["reentry", "roll"] | None = None

    @model_validator(mode="after")
    def linked_episode(self):
        if (self.related_trade_id is None) != (self.relation is None):
            raise ValueError("related_trade_id and relation must be supplied together")
        return self


class Execution(DTO):
    venue: Literal["MOEX"]
    contract_code: ShortText
    account: UnknownText
    action: Literal["increase", "reduce"]
    contracts: int = Field(strict=True, ge=1, le=1000000000)
    price: UnknownDecimal
    fee: UnknownDecimal
    currency: UnknownText


class EntryInput(DTO):
    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {"properties": {"kind": {"const": "note"}, "execution": {"type": "null"}}},
                {
                    "required": ["execution"],
                    "properties": {"kind": {"const": "execution"}, "execution": {"type": "object"}},
                },
            ]
        }
    )
    kind: Literal["note", "execution"]
    text: Text
    event_at: datetime
    source: Source
    execution: Execution | None = None
    supersedes_entry_id: Identifier | None = None

    @model_validator(mode="after")
    def payload_kind(self):
        if (self.kind == "execution") != (self.execution is not None):
            raise ValueError("execution payload is required exclusively for execution entries")
        return self


class CreateTrade(DTO):
    schema_version: Literal[1]
    command_id: Identifier
    trade_id: Identifier
    card: Card
    status: Literal["draft", "planned"] = "draft"
    event_at: datetime
    reason: Text
    source: Source


class ReplaceCard(DTO):
    kind: Literal["replace_card"]
    card: Card


class AddEntry(DTO):
    kind: Literal["add_entry"]
    entry: EntryInput


class SetStatus(DTO):
    kind: Literal["set_status", "correct_status"]
    status: Status


Operation = Annotated[ReplaceCard | AddEntry | SetStatus, Field(discriminator="kind")]


class ChangeTrade(DTO):
    schema_version: Literal[1]
    command_id: Identifier
    expected_revision: int = Field(strict=True, ge=1)
    event_at: datetime
    reason: Text
    source: Source
    operations: list[Operation] = Field(min_length=1, max_length=20)


class Entry(EntryInput):
    entry_id: Identifier
    trade_id: Identifier
    revision: int
    sequence: int
    recorded_at: datetime


class Position(DTO):
    contract_code: ShortText
    contracts: int = Field(strict=True, ge=0)


class ExecutionSummary(DTO):
    status: Literal["not_recorded", "recorded"]
    total_contracts: int | None
    reason: ShortText | None


class Snapshot(DTO):
    schema_version: Literal[1] = 1
    trade_id: Identifier
    revision: int
    card: Card
    status: Status
    entry_ids: list[Identifier]
    positions: list[Position]
    execution_summary: ExecutionSummary
    created_at: datetime
    updated_at: datetime
    event_at: datetime
    reason: Text
    source: Source


class Receipt(DTO):
    schema_version: Literal[1] = 1
    command_id: Identifier
    trade_id: Identifier
    revision: int
    entry_ids: list[Identifier]
    recorded_at: datetime


def journal_json_schema() -> dict:
    models: list[type[BaseModel]] = [CreateTrade, ChangeTrade, Snapshot, Receipt, Entry]
    roots, schema = models_json_schema(
        [(model, "validation") for model in models], title="Manual Journal v1"
    )
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["oneOf"] = [roots[(model, "validation")] for model in models]
    return schema
