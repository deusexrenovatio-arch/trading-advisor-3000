from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from trading_advisor_3000.product_plane.data_plane.schemas.delta import (
    historical_data_delta_schema_manifest,
)

MOEX_CONTRACT_ECONOMICS_MODEL_VERSION = "moex_contract_economics_v1"
MOEX_MARGIN_BUFFER_POLICY_VERSION = "moex_margin_buffer_policy_v1"
MOEX_DEFAULT_RADIUS_PCT = 15.0

MOEX_ECONOMICS_RAW_TABLES = (
    "raw_moex_contract_securities",
    "raw_moex_indicative_fx_rates",
    "raw_moex_rms_limits",
    "raw_moex_rms_staticparams",
)
MOEX_ECONOMICS_CANONICAL_TABLES = (
    "canonical_fx_rates",
    "canonical_asset_risk_parameters",
    "canonical_contract_economics",
)

MOEX_USD_LINKED_ASSETS = frozenset(
    {
        "BR",
        "BRM",
        "GOLD",
        "GOLDM",
        "GL",
        "SILV",
        "SILVM",
        "NG",
        "NGM",
        "RTS",
        "RTSM",
        "SPYF",
        "NASD",
    }
)
MOEX_FX_LINKED_ASSETS = frozenset(
    {
        "SI",
        "USDRUBF",
        "ED",
        "EU",
        "EURRUBF",
        "CNY",
        "UCNY",
        "TRY",
        "GBPU",
        "AUDU",
        "UJPY",
    }
)
MOEX_FX_OR_USD_LINKED_ASSETS = frozenset(sorted(MOEX_USD_LINKED_ASSETS | MOEX_FX_LINKED_ASSETS))


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _as_float(value: object, *, field_name: str, required: bool = True) -> float | None:
    if value is None or value == "":
        if required:
            raise ValueError(f"{field_name} is required for MOEX contract economics")
        return None
    try:
        resolved = float(Decimal(str(value).replace(",", ".")))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric for MOEX contract economics") from exc
    if not math.isfinite(resolved):
        raise ValueError(f"{field_name} must be finite for MOEX contract economics")
    return resolved


def _as_positive_float(value: object, *, field_name: str) -> float:
    resolved = _as_float(value, field_name=field_name, required=True)
    assert resolved is not None
    if resolved <= 0:
        raise ValueError(f"{field_name} must be > 0 for MOEX contract economics")
    return resolved


def _normalized_assetcode(instrument_id: str, assetcode: str | None = None) -> str:
    if assetcode:
        return assetcode.strip().upper()
    normalized = instrument_id.strip().upper()
    if normalized.startswith("FUT_"):
        normalized = normalized[4:]
    return normalized


def margin_buffer_pct(
    *,
    instrument_id: str,
    quote_currency: str | None,
    maturity_rank: int | None,
    days_to_expiry: int | None,
    assetcode: str | None = None,
) -> float:
    if maturity_rank is not None and maturity_rank >= 3:
        return 0.30
    if days_to_expiry is not None and days_to_expiry > 120:
        return 0.30

    currency = (quote_currency or "RUB").strip().upper()
    resolved_assetcode = _normalized_assetcode(instrument_id, assetcode)
    if currency != "RUB":
        return 0.05
    if resolved_assetcode in MOEX_FX_OR_USD_LINKED_ASSETS:
        return 0.05
    return 0.01


def compute_contract_economics(
    *,
    contract_id: str,
    instrument_id: str,
    economics_session_date: str | date,
    min_step: object,
    lot_volume: object,
    fx_rate_to_rub: object,
    last_settle_price: object,
    mr1: object,
    official_initial_margin: object | None,
    radius_pct: object | None,
    maturity_rank: int | None,
    days_to_expiry: int | None,
    quote_currency: str | None = "RUB",
    clearing_type: str = "mc",
    assetcode: str | None = None,
    moex_secid: str | None = None,
    official_step_price: object | None = None,
    expiration_date: str | date | None = None,
    effective_session_date: str | date | None = None,
    effective_from_ts: str | None = None,
    effective_to_ts: str | None = None,
    source_flags: Mapping[str, object] | None = None,
    source_document_hashes: Mapping[str, object] | None = None,
) -> dict[str, object]:
    resolved_contract_id = contract_id.strip()
    resolved_instrument_id = instrument_id.strip()
    if not resolved_contract_id:
        raise ValueError("contract_id is required for MOEX contract economics")
    if not resolved_instrument_id:
        raise ValueError("instrument_id is required for MOEX contract economics")

    min_step_value = _as_positive_float(min_step, field_name="MINSTEP")
    lot_volume_value = _as_positive_float(lot_volume, field_name="LOTVOLUME")
    fx_rate_value = _as_positive_float(fx_rate_to_rub, field_name="FX rate")
    settle_value = _as_positive_float(last_settle_price, field_name="LASTSETTLEPRICE")
    mr1_value = _as_positive_float(mr1, field_name="MR1")
    official_margin = _as_float(official_initial_margin, field_name="INITIALMARGIN", required=False)
    official_margin = 0.0 if official_margin is None else max(0.0, official_margin)
    official_step = _as_float(official_step_price, field_name="STEPPRICE", required=False)
    if radius_pct is None or radius_pct == "":
        radius_value = MOEX_DEFAULT_RADIUS_PCT
        radius_source = "policy_default"
    else:
        radius_value = _as_float(radius_pct, field_name="RADIUS", required=True)
        assert radius_value is not None
        if radius_value < 0:
            raise ValueError("RADIUS must be >= 0 for MOEX contract economics")
        radius_source = "source"

    tick_value_currency = min_step_value * lot_volume_value
    step_price_rub = tick_value_currency * fx_rate_value
    margin_formula_base = settle_value * (step_price_rub / min_step_value) * mr1_value
    margin_radius_adjusted = margin_formula_base * (1.0 + radius_value / 100.0)
    margin_required_no_buffer = max(official_margin, margin_radius_adjusted)
    buffer_pct = margin_buffer_pct(
        instrument_id=resolved_instrument_id,
        quote_currency=quote_currency,
        maturity_rank=maturity_rank,
        days_to_expiry=days_to_expiry,
        assetcode=assetcode,
    )
    session_date_text = (
        economics_session_date.isoformat()
        if isinstance(economics_session_date, date)
        else str(economics_session_date)
    )
    effective_session_text = (
        effective_session_date.isoformat()
        if isinstance(effective_session_date, date)
        else str(effective_session_date or session_date_text)
    )
    expiration_date_text = (
        expiration_date.isoformat()
        if isinstance(expiration_date, date)
        else (str(expiration_date) if expiration_date else None)
    )
    effective_from = effective_from_ts or f"{session_date_text}T19:00:00Z"
    resolved_assetcode = _normalized_assetcode(resolved_instrument_id, assetcode)
    resolved_source_flags = dict(source_flags or {})
    resolved_source_flags.setdefault("radius_source", radius_source)
    return {
        "contract_id": resolved_contract_id,
        "instrument_id": resolved_instrument_id,
        "moex_secid": (moex_secid or resolved_contract_id).strip(),
        "assetcode": resolved_assetcode,
        "economics_session_date": session_date_text,
        "effective_session_date": effective_session_text,
        "clearing_type": clearing_type.strip().lower() or "mc",
        "effective_from_ts": effective_from,
        "effective_to_ts": effective_to_ts,
        "min_step": min_step_value,
        "lot_volume": lot_volume_value,
        "quote_currency": (quote_currency or "RUB").strip().upper() or "RUB",
        "fx_rate_to_rub": fx_rate_value,
        "tick_value_currency": tick_value_currency,
        "step_price_rub": step_price_rub,
        "official_step_price": official_step,
        "official_initial_margin": official_margin,
        "last_settle_price": settle_value,
        "mr1": mr1_value,
        "radius_pct": radius_value,
        "radius_source": radius_source,
        "margin_formula_base": margin_formula_base,
        "margin_radius_adjusted": margin_radius_adjusted,
        "margin_required_no_buffer": margin_required_no_buffer,
        "margin_buffer_pct": buffer_pct,
        "margin_required_estimate": margin_required_no_buffer * (1.0 + buffer_pct),
        "maturity_rank": maturity_rank,
        "days_to_expiry": days_to_expiry,
        "expiration_date": expiration_date_text,
        "model_version": MOEX_CONTRACT_ECONOMICS_MODEL_VERSION,
        "buffer_policy_version": MOEX_MARGIN_BUFFER_POLICY_VERSION,
        "model_quality": "estimated",
        "source_flags_json": resolved_source_flags,
        "source_document_hashes_json": dict(source_document_hashes or {}),
        "created_at": _utc_now_iso(),
    }


def moex_economics_store_contract() -> dict[str, dict[str, Any]]:
    manifest = historical_data_delta_schema_manifest()
    names = (*MOEX_ECONOMICS_RAW_TABLES, *MOEX_ECONOMICS_CANONICAL_TABLES)
    return {name: dict(manifest[name]) for name in names}
