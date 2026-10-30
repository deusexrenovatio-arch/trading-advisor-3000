"""Validation for isolated source-native quote products; legacy bars stay unchanged."""

from __future__ import annotations

import json
import math
import re
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from .schema_validation import SchemaValidationError, load_schema, validate_schema


@lru_cache(maxsize=2)
def _schema(name: str) -> dict:
    return load_schema(Path(__file__).parent / "schemas" / f"market_quote_{name}.v1.json")


def _utc_timestamp(value: str, *, field: str) -> datetime:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", value):
        raise SchemaValidationError(f"{field}: UTC timestamp ending in Z required")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SchemaValidationError(f"{field}: invalid timestamp") from exc


def _source_timestamp(value: str, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SchemaValidationError(f"{field}: invalid source timestamp") from exc
    if parsed.tzinfo is not None or " " not in value:
        raise SchemaValidationError(f"{field}: naive MOEX local timestamp required")
    return parsed.replace(tzinfo=ZoneInfo("Europe/Moscow")).astimezone(UTC)


def validate_quote_series(payload: dict[str, object]) -> None:
    """Validate schema and descriptor meaning; metadata_asof is retrieval time only."""
    validate_schema(_schema("series"), payload)
    for field in ("series_id", "security_id", "engine", "market", "board", "secid"):
        if not str(payload[field]).strip():
            raise SchemaValidationError(f"{field}: nonblank value required")
    expected = f"MOEX:{payload['engine']}:{payload['market']}:{payload['secid']}"
    if payload["security_id"] != expected:
        raise SchemaValidationError("security_id must retain the full MOEX security identity")
    expected_series = f"{payload['security_id']}:{payload['board']}:{payload['price_basis']}"
    if payload["series_id"] != expected_series:
        raise SchemaValidationError("series_id must equal security_id:board:price_basis")
    underlying = payload["underlying_security_id"]
    if payload["asset_class"] == "futures":
        if (
            not isinstance(underlying, str)
            or not re.fullmatch(r"MOEX:[^:\s]+:[^:\s]+:[^:\s]+", underlying)
            or underlying == payload["security_id"]
        ):
            raise SchemaValidationError(
                "underlying_security_id requires a distinct neutral MOEX security identity"
            )
    elif underlying is not None:
        raise SchemaValidationError("equity/index underlying_security_id must be null")
    _utc_timestamp(str(payload["metadata_asof"]), field="metadata_asof")
    try:
        metadata = json.loads(str(payload["metadata_json"]))
        aliases = json.loads(str(payload["aliases_json"]))
    except (ValueError, TypeError) as exc:
        raise SchemaValidationError("metadata_json/aliases_json must contain valid JSON") from exc
    if not isinstance(metadata, dict):
        raise SchemaValidationError("metadata_json must encode an object")
    if not isinstance(aliases, list) or any(
        not isinstance(alias, str) or not alias.strip() for alias in aliases
    ):
        raise SchemaValidationError("aliases_json must encode an array of nonempty strings")
    is_index = payload["asset_class"] == "index"
    if is_index:
        if payload["tradable"] or payload["price_unit"] != "index_points":
            raise SchemaValidationError("index series must be nontradable index points")
        if (
            payload["volume_unit"] != "not_applicable"
            or payload["turnover_unit"] != "not_applicable"
        ):
            raise SchemaValidationError("index trading volume/turnover units are not applicable")
    elif (
        not payload["tradable"]
        or payload["price_unit"] != "source_native"
        or payload["volume_unit"] != "source_native"
        or payload["turnover_unit"] != "source_native"
    ):
        raise SchemaValidationError("equity/futures series must preserve native trading units")


def validate_quote_bar(payload: dict[str, object]) -> None:
    """Validate a native daily candle without fabricating volume or open interest."""
    validate_schema(_schema("bar"), payload)
    for field in ("open", "high", "low", "close", "volume", "turnover"):
        value = payload[field]
        if value is not None and not math.isfinite(value):
            raise SchemaValidationError(f"{field}: finite number required")
    if payload["high"] < max(payload["open"], payload["close"]):
        raise SchemaValidationError("high must cover open and close")
    if payload["low"] > min(payload["open"], payload["close"]):
        raise SchemaValidationError("low must cover open and close")
    start = _utc_timestamp(str(payload["ts_open"]), field="ts_open")
    end = _utc_timestamp(str(payload["ts_close"]), field="ts_close")
    _utc_timestamp(str(payload["fetched_at"]), field="fetched_at")
    if start > end:
        raise SchemaValidationError("ts_open must not follow inclusive ts_close")
    if _source_timestamp(str(payload["source_begin"]), field="source_begin") != start:
        raise SchemaValidationError("ts_open must preserve source_begin converted to UTC")
    if _source_timestamp(str(payload["source_end"]), field="source_end") != end:
        raise SchemaValidationError("ts_close must preserve inclusive source_end converted to UTC")
    if (payload["volume_status"] == "reported") != (payload["volume"] is not None):
        raise SchemaValidationError(
            "reported volume requires a number; missing/not_applicable requires null"
        )
