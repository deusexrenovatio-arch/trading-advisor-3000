from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    MOEX_CONTRACT_ECONOMICS_MODEL_VERSION,
    MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES,
    MOEX_MARGIN_BUFFER_POLICY_VERSION,
    MOEX_USD_LINKED_ASSETS,
    compute_contract_economics,
    margin_buffer_pct,
    moex_economics_store_contract,
)


def test_historical_contract_parameter_regimes_keep_canonical_moex_parameters() -> None:
    expected = {
        "BR": (0.01, 10.0, "USD", 0.1),
        "GOLD": (0.1, 1.0, "USD", 0.1),
        "SILV": (0.01, 10.0, "USD", 0.1),
        "PLD": (0.01, 1.0, "USD", 0.01),
        "PLT": (0.1, 1.0, "USD", 0.1),
        "NG": (0.001, 100.0, "USD", 0.1),
        "SPYF": (0.01, 1.0, "USD", 0.01),
        "NASD": (1.0, 41.0, "USD", 0.01),
        "RTS": (10.0, 1.0, "USD", 0.2),
        "MIX": (25.0, 1.0, "RUB", 25.0),
        "MXI": (0.05, 1.0, "RUB", 0.5),
        "RGBI": (1.0, 1.0, "RUB", 1.0),
        "WHEAT": (10.0, 1.0, "RUB", 10.0),
    }
    actual = {
        rule.assetcode: (
            rule.min_step,
            rule.lot_volume,
            rule.quote_currency,
            rule.tick_value_quote,
        )
        for rule in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES
    }

    assert actual == expected
    assert {"PLD", "PLT"}.issubset(MOEX_USD_LINKED_ASSETS)
    legacy = [
        r for r in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES if r.rule_id.endswith("2026-v1")
    ]
    assert len(legacy) == 13
    assert all(r.effective_from == "2022-06-17" and r.effective_to == "2026-06-16" for r in legacy)


def test_pre2020_regimes_require_dated_evidence_and_keep_later_regimes() -> None:
    early = [
        r for r in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES if r.rule_id.endswith("pre2020-v1")
    ]
    assert {r.assetcode: r.effective_from for r in early} == {
        "BR": "2018-06-26",
        "GOLD": "2019-03-07",
        "SILV": "2019-03-07",
        "PLD": "2019-06-07",
        "PLT": "2019-06-07",
        "RTS": "2018-01-10",
        "MIX": "2019-03-07",
        "MXI": "2019-03-07",
    }
    assert all(r.effective_to == "2020-01-26" for r in early)
    assert all("ta3000-early-audit-17ab91c2" in r.source_document_id for r in early)


def test_early_parameter_regimes_are_bounded_and_do_not_overlap_existing_history() -> None:
    early = [
        r for r in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES if r.rule_id.endswith("pre2022-v1")
    ]
    assert len(early) == 11
    starts = {r.assetcode: r.effective_from for r in early}
    assert starts == {
        "BR": "2020-01-27",
        "GOLD": "2020-01-27",
        "SILV": "2020-01-27",
        "PLD": "2020-01-27",
        "PLT": "2020-01-27",
        "NG": "2020-02-03",
        "SPYF": "2021-05-25",
        "RTS": "2020-01-27",
        "MIX": "2020-01-27",
        "MXI": "2020-01-27",
        "RGBI": "2022-02-28",
    }
    assert all(r.effective_to == "2022-06-16" for r in early)
    assert all("ta3000-parameter-audit-20260929" in r.source_document_id for r in early)
    for asset in starts:
        regimes = sorted(
            (r for r in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES if r.assetcode == asset),
            key=lambda r: r.effective_from,
        )
        assert all(a.effective_to < b.effective_from for a, b in zip(regimes, regimes[1:]))


def test_br_step_price_and_margin_formula_use_moex_stepprice() -> None:
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
        official_step_price=7.19077,
    )

    assert economics["tick_value_currency"] == pytest.approx(0.1)
    assert economics["step_price_rub"] == pytest.approx(7.19077)
    assert economics["margin_formula_base"] == pytest.approx(8110.3256676)
    assert economics["margin_required_no_buffer"] == pytest.approx(17721.61)
    assert economics["margin_buffer_pct"] == pytest.approx(0.0)
    assert economics["margin_required_estimate"] == pytest.approx(17721.61)
    assert economics["radius_source"] == "policy_default"
    assert economics["radius_pct"] == pytest.approx(15.0)
    assert economics["model_version"] == MOEX_CONTRACT_ECONOMICS_MODEL_VERSION
    assert economics["buffer_policy_version"] == MOEX_MARGIN_BUFFER_POLICY_VERSION


def test_official_initial_margin_is_canonical_without_buffer() -> None:
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
        official_step_price=7.19077,
    )

    assert economics["margin_formula_base"] == pytest.approx(8110.3256676)
    assert economics["margin_radius_adjusted"] == pytest.approx(10137.9070845)
    assert economics["margin_required_no_buffer"] == pytest.approx(1_000.0)
    assert economics["margin_buffer_pct"] == pytest.approx(0.0)
    assert economics["margin_required_estimate"] == pytest.approx(1_000.0)
    assert economics["model_quality"] == "official_initial_margin"


def test_historical_margin_uses_asset_rank_calibration_without_buffer() -> None:
    economics = compute_contract_economics(
        contract_id="BRN4",
        instrument_id="FUT_BR",
        economics_session_date="2024-06-14",
        min_step=0.01,
        lot_volume=10.0,
        fx_rate_to_rub=89.0658,
        last_settle_price=82.84,
        mr1=0.12,
        official_initial_margin=None,
        radius_pct=15.0,
        maturity_rank=3,
        days_to_expiry=184,
        quote_currency="USD",
        assetcode="BR",
        official_step_price=8.90658,
        margin_calibration_factor=1.2,
        margin_calibration_as_of_date="2026-07-09",
        margin_calibration_rank=2,
        margin_calibration_source="latest_official_nearest_rank",
    )

    expected = economics["margin_radius_adjusted"] * 1.2
    assert economics["margin_calibration_factor"] == pytest.approx(1.2)
    assert economics["margin_calibration_as_of_date"] == "2026-07-09"
    assert economics["margin_calibration_rank"] == 2
    assert economics["margin_calibration_source"] == "latest_official_nearest_rank"
    assert economics["margin_required_no_buffer"] == pytest.approx(expected)
    assert economics["margin_buffer_pct"] == pytest.approx(0.0)
    assert economics["margin_required_estimate"] == pytest.approx(expected)
    assert economics["model_quality"] == "calibrated_nearest_rank"


def test_si_step_price_does_not_multiply_lot_volume_or_fx() -> None:
    economics = compute_contract_economics(
        contract_id="SiU6",
        instrument_id="FUT_SI",
        economics_session_date="2026-07-09",
        min_step=1.0,
        lot_volume=1_000.0,
        fx_rate_to_rub=76.4026,
        last_settle_price=77_426.0,
        mr1=0.15,
        official_initial_margin=12_316.83,
        official_step_price=1.0,
        radius_pct=15.0,
        maturity_rank=1,
        days_to_expiry=70,
        quote_currency="RUB",
        assetcode="SI",
    )

    assert economics["step_price_rub"] == pytest.approx(1.0)
    assert economics["tick_value_currency"] == pytest.approx(1.0)
    assert economics["margin_formula_base"] == pytest.approx(11_613.9)
    assert economics["margin_radius_adjusted"] == pytest.approx(13_355.985)
    assert economics["margin_required_estimate"] == pytest.approx(12_316.83)


def test_pure_rub_contract_does_not_apply_currency_radius() -> None:
    economics = compute_contract_economics(
        contract_id="SBER-9.26",
        instrument_id="FUT_SBER",
        economics_session_date="2026-07-09",
        min_step=1.0,
        lot_volume=1.0,
        fx_rate_to_rub=1.0,
        last_settle_price=100.0,
        mr1=0.10,
        official_initial_margin=0.0,
        official_step_price=1.0,
        radius_pct=25.0,
        maturity_rank=1,
        days_to_expiry=70,
        quote_currency="RUB",
        assetcode="SBER",
        margin_calibration_factor=1.0,
        margin_calibration_as_of_date="2026-07-09",
        margin_calibration_rank=1,
        margin_calibration_source="latest_official_asset_rank",
    )

    assert economics["radius_pct"] == pytest.approx(0.0)
    assert economics["radius_source"] == "not_applicable_pure_rub"
    assert economics["margin_formula_base"] == pytest.approx(10.0)
    assert economics["margin_radius_adjusted"] == pytest.approx(10.0)
    assert economics["margin_required_estimate"] == pytest.approx(10.0)


def test_canonical_margin_buffer_policy_is_zero() -> None:
    assert margin_buffer_pct(
        instrument_id="FUT_SBER",
        quote_currency="RUB",
        maturity_rank=1,
        days_to_expiry=30,
    ) == pytest.approx(0.0)
    assert margin_buffer_pct(
        instrument_id="FUT_BR",
        quote_currency="USD",
        maturity_rank=1,
        days_to_expiry=30,
    ) == pytest.approx(0.0)
    assert margin_buffer_pct(
        instrument_id="FUT_SI",
        quote_currency="RUB",
        maturity_rank=1,
        days_to_expiry=30,
    ) == pytest.approx(0.0)
    assert margin_buffer_pct(
        instrument_id="FUT_BR",
        quote_currency="USD",
        maturity_rank=3,
        days_to_expiry=90,
    ) == pytest.approx(0.0)
    assert margin_buffer_pct(
        instrument_id="FUT_SBER",
        quote_currency="RUB",
        maturity_rank=1,
        days_to_expiry=121,
    ) == pytest.approx(0.0)


@pytest.mark.parametrize(
    ("field", "kwargs"),
    [
        ("MINSTEP", {"min_step": None}),
        ("LOTVOLUME", {"lot_volume": None}),
        ("FX", {"fx_rate_to_rub": None}),
        ("MR1", {"mr1": None}),
        ("STEPPRICE", {"official_step_price": None}),
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
        "official_step_price": 7.19077,
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
    assert {
        "margin_calibration_factor",
        "margin_calibration_as_of_date",
        "margin_calibration_rank",
        "margin_calibration_source",
    }.issubset(contract["canonical_contract_economics"]["columns"])
    assert "execution_step_price_rub" not in contract["canonical_contract_economics"]["columns"]
