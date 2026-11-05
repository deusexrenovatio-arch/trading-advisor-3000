from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from trading_advisor_3000.product_plane.contracts.market_quotes import (
    validate_quote_bar,
    validate_quote_series,
)

ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src/trading_advisor_3000/product_plane/contracts/schemas"
FIXTURES = ROOT / "tests/product-plane/fixtures/contracts"


def _fixture(name):
    return json.loads((FIXTURES / f"market_quote_{name}.v1.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "name,validate", [("series", validate_quote_series), ("bar", validate_quote_bar)]
)
def test_quote_fixture_obeys_schema_and_owner_semantics(name, validate):
    schema = json.loads((SCHEMAS / f"market_quote_{name}.v1.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    payload = _fixture(name)
    Draft202012Validator(schema).validate(payload)
    validate(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("asset_class", "bond"),
        ("price_basis", "adjusted"),
        ("source", "inferred"),
        ("source_timezone", "UTC"),
        ("tradable", "true"),
        ("security_id", "SBER"),
        ("metadata_asof", "2026-10-26T12:00:00"),
        ("metadata_json", "[]"),
        ("aliases_json", "{}"),
        ("underlying_security_id", ""),
    ],
)
def test_quote_series_rejects_contract_mutations(field, value):
    payload = _fixture("series")
    payload[field] = value
    with pytest.raises(ValueError):
        validate_quote_series(payload)


def test_index_series_requires_index_units_and_nontradable_capability():
    payload = _fixture("series")
    payload.update(
        asset_class="index",
        price_unit="index_points",
        volume_unit="not_applicable",
        turnover_unit="not_applicable",
        tradable=False,
    )
    validate_quote_series(payload)
    for field, value in [
        ("tradable", True),
        ("volume_unit", "source_native"),
        ("turnover_unit", "source_native"),
        ("price_unit", "source_native"),
    ]:
        invalid = {**payload, field: value}
        with pytest.raises(ValueError):
            validate_quote_series(invalid)


@pytest.mark.parametrize(
    "series_id",
    [
        "MOEX:stock:shares:GAZP:TQBR:source_native",
        "arbitrary:TQBR:source_native",
        "MOEX:stock:shares:TQBR:SBER:source_native",
    ],
)
def test_quote_series_identity_must_bind_its_exact_security(series_id):
    with pytest.raises(ValueError, match="series_id"):
        validate_quote_series({**_fixture("series"), "series_id": series_id})


@pytest.mark.parametrize("asset_class", ["equity", "index"])
def test_nonderivative_series_rejects_invented_underlying(asset_class):
    payload = _fixture("series")
    if asset_class == "index":
        payload.update(
            asset_class="index",
            price_unit="index_points",
            volume_unit="not_applicable",
            turnover_unit="not_applicable",
            tradable=False,
        )
    payload["underlying_security_id"] = "MOEX:stock:shares:SBER"
    with pytest.raises(ValueError, match="underlying_security_id"):
        validate_quote_series(payload)


@pytest.mark.parametrize(
    "underlying",
    [
        None,
        "",
        "SBER",
        "MOEX:stock:shares:",
        "MOEX:stock:shares:SBER:extra",
        "MOEX:futures:forts:SRZ6",
    ],
)
def test_futures_series_requires_a_distinct_neutral_underlying_identity(underlying):
    payload = _fixture("series")
    payload.update(
        asset_class="futures",
        engine="futures",
        market="forts",
        board="RFUD",
        secid="SRZ6",
        security_id="MOEX:futures:forts:SRZ6",
        series_id="MOEX:futures:forts:SRZ6:RFUD:source_native",
        underlying_security_id=underlying,
    )
    with pytest.raises(ValueError, match="underlying_security_id"):
        validate_quote_series(payload)


def test_futures_underlying_identity_does_not_hardcode_sber():
    payload = _fixture("series")
    payload.update(
        asset_class="futures",
        engine="futures",
        market="forts",
        board="RFUD",
        secid="GZZ6",
        security_id="MOEX:futures:forts:GZZ6",
        series_id="MOEX:futures:forts:GZZ6:RFUD:source_native",
        underlying_security_id="MOEX:stock:shares:GAZP",
    )
    validate_quote_series(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "canonical_bar.v2"),
        ("timeframe", "1h"),
        ("volume", True),
        ("volume", -1),
        ("volume", float("nan")),
        ("turnover", float("inf")),
        ("open_interest", 0),
        ("volume_status", "imputed"),
        ("ts_open", "2026-10-26T10:00:00"),
        ("ts_open", "2026-10-26T10:00:00+03:00"),
        ("ts_close", "2026-10-25T07:00:00Z"),
        ("high", 1),
        ("low", 1000),
        ("close", float("nan")),
        ("source_begin", "invalid"),
    ],
)
def test_quote_bars_reject_mutations(field, value):
    payload = _fixture("bar")
    payload[field] = value
    with pytest.raises(ValueError):
        validate_quote_bar(payload)


def test_quote_volume_preserves_reported_zero_fraction_and_missing_semantics():
    payload = _fixture("bar")
    for volume in [0, 1.25]:
        validate_quote_bar({**payload, "volume": volume})
    for status in ["missing", "not_applicable"]:
        validate_quote_bar({**payload, "volume": None, "volume_status": status})
        with pytest.raises(ValueError):
            validate_quote_bar({**payload, "volume": 0, "volume_status": status})
    with pytest.raises(ValueError):
        validate_quote_bar({**payload, "volume": None})


@pytest.mark.parametrize(
    "name,validate", [("series", validate_quote_series), ("bar", validate_quote_bar)]
)
def test_quote_contract_rejects_unsupported_and_missing_fields(name, validate):
    payload = _fixture(name)
    with pytest.raises(ValueError):
        validate({**payload, "internal_debug": True})
    payload.pop("series_id")
    with pytest.raises(ValueError):
        validate(payload)
