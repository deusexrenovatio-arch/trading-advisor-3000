from __future__ import annotations

import pandas as pd
import pytest

from trading_advisor_3000.product_plane.research.backtests.money import (
    MONEY_DATA_CONNECTORS,
    MONEY_SOURCE,
    MoneyModelError,
    MoneySizingConfig,
    execution_economics_from_row,
    prepare_money_surface,
    trade_ledger_from_vectorbt_records,
)


def test_money_model_declares_data_connectors() -> None:
    assert MONEY_DATA_CONNECTORS == {
        "execution_bar_view": "research_bar_views.delta",
        "contract_economics": "canonical_contract_economics.delta",
    }


def _column() -> pd.MultiIndex:
    return pd.MultiIndex.from_tuples(
        [("trend", "surface", "template", "PARAM1", "FUT_BR")],
        names=("family_key", "surface_key", "template_key", "param_hash", "instrument_id"),
    )


def _metadata(rows: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "execution_step_price_rub": [10.0] * rows,
            "execution_lot_volume": [10.0] * rows,
            "execution_tick_value_currency": [1.0] * rows,
            "execution_margin_required_estimate": [15_000.0] * rows,
            "economics_model_version": ["unit-economics-v1"] * rows,
        }
    )


def test_execution_economics_resolves_tick_contract_from_research_connector() -> None:
    economics = execution_economics_from_row(
        _metadata(1).iloc[0].to_dict(),
        entry_price=100.0,
        commission_per_contract=2.0,
        slippage_ticks=1.0,
    )

    assert economics.tick_size == pytest.approx(0.1)
    assert economics.tick_value == pytest.approx(10.0)
    assert economics.notional_value == pytest.approx(10_000.0)
    assert economics.money_source == MONEY_SOURCE


def test_risk_sizing_uses_stop_distance_and_margin_cap() -> None:
    index = pd.date_range("2026-03-16T09:00:00Z", periods=3, freq="15min")
    columns = _column()
    entries = pd.DataFrame([True, False, False], index=index, columns=columns)
    shorts = pd.DataFrame(False, index=index, columns=columns)
    close = pd.DataFrame([100.0, 101.0, 102.0], index=index, columns=columns)
    sl_stop = pd.DataFrame([0.02, 0.02, 0.02], index=index, columns=columns)

    prepared = prepare_money_surface(
        entries=entries,
        short_entries=shorts,
        close=close,
        sl_stop=sl_stop,
        metadata_by_instrument={"FUT_BR": _metadata()},
        initial_cash=100_000.0,
        sizing_config=MoneySizingConfig(risk_per_trade_pct=0.01, max_margin_fraction=0.5),
        commission_per_contract=2.0,
        slippage_ticks=1.0,
        default_size=1.0,
    )

    assert prepared.size.iloc[0, 0] == 3.0
    assert prepared.risk_cash.iloc[0, 0] == pytest.approx(1_000.0)
    assert prepared.risk_per_contract.iloc[0, 0] == pytest.approx(200.0)
    assert prepared.margin_required.iloc[0, 0] == pytest.approx(45_000.0)


def test_missing_execution_economics_rejects_money_entry_fail_closed() -> None:
    index = pd.date_range("2026-03-16T09:00:00Z", periods=1, freq="15min")
    columns = _column()

    prepared = prepare_money_surface(
        entries=pd.DataFrame([True], index=index, columns=columns),
        short_entries=pd.DataFrame(False, index=index, columns=columns),
        close=pd.DataFrame([100.0], index=index, columns=columns),
        sl_stop=pd.DataFrame([0.02], index=index, columns=columns),
        metadata_by_instrument={"FUT_BR": pd.DataFrame(index=index)},
        initial_cash=100_000.0,
        sizing_config=MoneySizingConfig(),
        commission_per_contract=0.0,
        slippage_ticks=0.0,
        default_size=1.0,
    )

    assert not bool(prepared.entries.iloc[0, 0])
    assert prepared.rejection_events[0]["failure_code"] == "MONEY_ECONOMICS_MISSING"


def test_trade_ledger_reprices_long_and_short_by_tick_economics() -> None:
    run_row = {"backtest_run_id": "RUN1", "initial_cash": 100_000.0}
    records = [
        {
            "id": 1,
            "entry_idx": 0,
            "exit_idx": 1,
            "direction": 0,
            "status": 1,
            "entry_price": 100.0,
            "exit_price": 102.0,
            "size": 3.0,
        },
        {
            "id": 2,
            "entry_idx": 1,
            "exit_idx": 2,
            "direction": 1,
            "status": 1,
            "entry_price": 102.0,
            "exit_price": 100.0,
            "size": 2.0,
        },
    ]
    ledger = trade_ledger_from_vectorbt_records(
        records=records,
        run_row=run_row,
        index=pd.date_range("2026-03-16T09:00:00Z", periods=3, freq="15min"),
        metadata_frame=_metadata(),
        risk_cash_frame=pd.Series([1_000.0, 1_000.0, 1_000.0]),
        risk_per_contract_frame=pd.Series([200.0, 200.0, 200.0]),
        margin_required_frame=pd.Series([45_000.0, 30_000.0, 0.0]),
        commission_per_contract=2.0,
        slippage_ticks=1.0,
    )

    assert [row["gross_pnl"] for row in ledger.trade_rows] == pytest.approx([600.0, 400.0])
    assert [row["net_pnl"] for row in ledger.trade_rows] == pytest.approx([528.0, 352.0])
    assert ledger.aggregate_metrics["net_pnl"] == pytest.approx(880.0)
    assert ledger.aggregate_metrics["fees_paid"] == pytest.approx(20.0)
    assert ledger.aggregate_metrics["slippage_paid"] == pytest.approx(100.0)


def test_execution_economics_missing_required_fields_raises_failure_code() -> None:
    with pytest.raises(MoneyModelError) as error:
        execution_economics_from_row(
            {"execution_step_price_rub": 10.0},
            entry_price=100.0,
            commission_per_contract=0.0,
            slippage_ticks=0.0,
        )

    assert error.value.failure_code == "MONEY_ECONOMICS_MISSING"
