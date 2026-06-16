from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    MOEX_CONTRACT_ECONOMICS_MODEL_VERSION,
    MOEX_MARGIN_BUFFER_POLICY_VERSION,
    compute_contract_economics,
    margin_buffer_pct,
    moex_economics_store_contract,
)


def test_br_step_price_and_margin_formula_use_fx_indicative_rate() -> None:
    economics = compute_contract_economics(
        contract_id="BRN6",
        instrument_id="FUT_BR",
        economics_session_date="2026-06-12",
        min_step=0.01,
        lot_volume=10,
        fx_rate_to_rub=71.9077,
        last_settle_price=93.99,
        mr1=0.12,
        official_initial_margin=17_721.61,
        radius_pct=None,
        maturity_rank=1,
        days_to_expiry=30,
        quote_currency="USD",
    )

    assert economics["tick_value_currency"] == pytest.approx(0.1)
    assert economics["step_price_rub"] == pytest.approx(7.19077)
    assert economics["margin_formula_base"] == pytest.approx(8110.3256676)
    assert economics["margin_required_no_buffer"] == pytest.approx(17721.61)
    assert economics["margin_buffer_pct"] == pytest.approx(0.05)
    assert economics["margin_required_estimate"] == pytest.approx(18607.6905)
    assert economics["radius_source"] == "policy_default"
    assert economics["radius_pct"] == pytest.approx(15.0)
    assert economics["model_version"] == MOEX_CONTRACT_ECONOMICS_MODEL_VERSION
    assert economics["buffer_policy_version"] == MOEX_MARGIN_BUFFER_POLICY_VERSION


def test_margin_formula_can_dominate_official_initial_margin() -> None:
    economics = compute_contract_economics(
        contract_id="BRN6",
        instrument_id="FUT_BR",
        economics_session_date="2026-06-12",
        min_step=0.01,
        lot_volume=10,
        fx_rate_to_rub=71.9077,
        last_settle_price=93.99,
        mr1=0.12,
        official_initial_margin=1_000.0,
        radius_pct=25.0,
        maturity_rank=1,
        days_to_expiry=30,
        quote_currency="USD",
    )

    assert economics["margin_formula_base"] == pytest.approx(8110.3256676)
    assert economics["margin_radius_adjusted"] == pytest.approx(10137.9070845)
    assert economics["margin_required_no_buffer"] == pytest.approx(10137.9070845)


def test_buffer_policy_tiers_are_explicit() -> None:
    assert margin_buffer_pct(
        instrument_id="FUT_SBER",
        quote_currency="RUB",
        maturity_rank=1,
        days_to_expiry=30,
    ) == pytest.approx(0.01)
    assert margin_buffer_pct(
        instrument_id="FUT_BR",
        quote_currency="USD",
        maturity_rank=1,
        days_to_expiry=30,
    ) == pytest.approx(0.05)
    assert margin_buffer_pct(
        instrument_id="FUT_SI",
        quote_currency="RUB",
        maturity_rank=1,
        days_to_expiry=30,
    ) == pytest.approx(0.05)
    assert margin_buffer_pct(
        instrument_id="FUT_BR",
        quote_currency="USD",
        maturity_rank=3,
        days_to_expiry=90,
    ) == pytest.approx(0.30)
    assert margin_buffer_pct(
        instrument_id="FUT_SBER",
        quote_currency="RUB",
        maturity_rank=1,
        days_to_expiry=121,
    ) == pytest.approx(0.30)


@pytest.mark.parametrize(
    ("field", "kwargs"),
    [
        ("MINSTEP", {"min_step": None}),
        ("LOTVOLUME", {"lot_volume": None}),
        ("FX", {"fx_rate_to_rub": None}),
        ("MR1", {"mr1": None}),
    ],
)
def test_economics_fail_closed_on_missing_required_inputs(
    field: str, kwargs: dict[str, object]
) -> None:
    base = {
        "contract_id": "BRN6",
        "instrument_id": "FUT_BR",
        "economics_session_date": "2026-06-12",
        "min_step": 0.01,
        "lot_volume": 10,
        "fx_rate_to_rub": 71.9077,
        "last_settle_price": 93.99,
        "mr1": 0.12,
        "official_initial_margin": 17_721.61,
        "radius_pct": 15.0,
        "maturity_rank": 1,
        "days_to_expiry": 30,
        "quote_currency": "USD",
    }
    base.update(kwargs)

    with pytest.raises(ValueError, match=field):
        compute_contract_economics(**base)


def test_economics_store_contract_declares_raw_and_canonical_side_tables() -> None:
    contract = moex_economics_store_contract()

    assert {
        "raw_moex_contract_securities",
        "raw_moex_indicative_fx_rates",
        "raw_moex_rms_limits",
        "raw_moex_rms_staticparams",
        "canonical_fx_rates",
        "canonical_asset_risk_parameters",
        "canonical_contract_economics",
    }.issubset(contract)
    assert contract["canonical_contract_economics"]["constraints"] == [
        "unique(contract_id, economics_session_date, clearing_type)"
    ]
    assert "execution_step_price_rub" not in contract["canonical_contract_economics"]["columns"]
