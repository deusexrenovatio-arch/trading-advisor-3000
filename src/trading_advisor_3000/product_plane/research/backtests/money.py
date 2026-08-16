from __future__ import annotations

from dataclasses import dataclass, field
from math import floor, isfinite
from typing import Mapping, Sequence

import pandas as pd

MONEY_MODEL_VERSION = "futures_trade_ledger.v1"
MONEY_SOURCE = "research_bar_views.execution_economics<-canonical_contract_economics.delta"
MONEY_DATA_CONNECTORS: dict[str, str] = {
    "execution_bar_view": "research_bar_views.delta",
    "contract_economics": "canonical_contract_economics.delta",
}
MONEY_EQUITY_MODE = "realized_trade_equity"
SIZING_MODE_RISK_PER_TRADE = "risk_per_trade"


class MoneyModelError(ValueError):
    def __init__(self, failure_code: str, message: str) -> None:
        super().__init__(message)
        self.failure_code = failure_code


@dataclass(frozen=True)
class ExecutionEconomics:
    tick_size: float
    tick_value: float
    lot_size: float
    initial_margin: float
    notional_value: float
    currency: str = "RUB"
    commission_per_contract: float = 0.0
    slippage_ticks: float = 0.0
    money_model_version: str = MONEY_MODEL_VERSION
    money_source: str = MONEY_SOURCE
    source_model_version: str = ""


@dataclass(frozen=True)
class MoneySizingConfig:
    risk_per_trade_pct: float = 0.01
    max_contracts: int | None = None
    max_margin_fraction: float | None = 1.0

    def __post_init__(self) -> None:
        if self.risk_per_trade_pct <= 0:
            raise ValueError("risk_per_trade_pct must be positive")
        if self.max_contracts is not None and self.max_contracts <= 0:
            raise ValueError("max_contracts must be positive when provided")
        if self.max_margin_fraction is not None and self.max_margin_fraction <= 0:
            raise ValueError("max_margin_fraction must be positive when provided")


@dataclass(frozen=True)
class MoneyPreparedSurface:
    entries: pd.DataFrame
    short_entries: pd.DataFrame
    size: pd.DataFrame
    risk_cash: pd.DataFrame
    risk_per_contract: pd.DataFrame
    margin_required: pd.DataFrame
    rejection_events: tuple[dict[str, object], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class MoneyTradeLedger:
    trade_rows: tuple[dict[str, object], ...]
    aggregate_metrics: dict[str, float | int | str]


def execution_economics_from_row(
    row: Mapping[str, object],
    *,
    entry_price: float,
    commission_per_contract: float,
    slippage_ticks: float,
) -> ExecutionEconomics:
    step_price_rub = _positive(row.get("execution_step_price_rub"), "execution_step_price_rub")
    lot_size = _positive(row.get("execution_lot_volume"), "execution_lot_volume")
    tick_value_currency = _positive(
        row.get("execution_tick_value_currency"), "execution_tick_value_currency"
    )
    initial_margin = _positive(
        row.get("execution_margin_required_estimate"),
        "execution_margin_required_estimate",
    )
    tick_size = tick_value_currency / lot_size
    if tick_size <= 0 or not isfinite(tick_size):
        raise MoneyModelError("MONEY_INVALID_TICK_SIZE", "resolved tick_size must be positive")
    notional_value = float(entry_price) * (step_price_rub / tick_size)
    source_model_version = row.get("economics_model_version")
    return ExecutionEconomics(
        tick_size=tick_size,
        tick_value=step_price_rub,
        lot_size=lot_size,
        initial_margin=initial_margin,
        notional_value=notional_value,
        commission_per_contract=float(commission_per_contract),
        slippage_ticks=float(slippage_ticks),
        source_model_version="" if source_model_version is None else str(source_model_version),
    )


def prepare_money_surface(
    *,
    entries: pd.DataFrame,
    short_entries: pd.DataFrame,
    close: pd.DataFrame,
    sl_stop: pd.DataFrame,
    metadata_by_instrument: Mapping[str, pd.DataFrame],
    initial_cash: float,
    sizing_config: MoneySizingConfig,
    commission_per_contract: float,
    slippage_ticks: float,
    default_size: float,
) -> MoneyPreparedSurface:
    cleaned_entries = entries.copy()
    cleaned_short_entries = short_entries.copy()
    size = pd.DataFrame(default_size, index=entries.index, columns=entries.columns, dtype=float)
    risk_cash_frame = pd.DataFrame(0.0, index=entries.index, columns=entries.columns, dtype=float)
    risk_per_contract_frame = pd.DataFrame(
        0.0, index=entries.index, columns=entries.columns, dtype=float
    )
    margin_required_frame = pd.DataFrame(
        0.0, index=entries.index, columns=entries.columns, dtype=float
    )
    rejection_events: list[dict[str, object]] = []
    risk_cash = float(initial_cash) * sizing_config.risk_per_trade_pct

    for column in entries.columns:
        instrument_id = _column_instrument_id(column)
        metadata = metadata_by_instrument.get(instrument_id)
        for bar_index, is_entry in enumerate(
            (entries[column].astype(bool) | short_entries[column].astype(bool)).to_numpy()
        ):
            if not bool(is_entry):
                continue
            ts = entries.index[bar_index]
            try:
                entry_price = _positive(close[column].iloc[bar_index], "entry_price")
                stop_pct = _positive(sl_stop[column].iloc[bar_index], "sl_stop")
                economics = execution_economics_from_row(
                    _metadata_at(metadata, bar_index),
                    entry_price=entry_price,
                    commission_per_contract=commission_per_contract,
                    slippage_ticks=slippage_ticks,
                )
                risk_per_contract = (
                    entry_price * stop_pct / economics.tick_size
                ) * economics.tick_value
                if risk_per_contract <= 0 or not isfinite(risk_per_contract):
                    raise MoneyModelError(
                        "MONEY_INVALID_RISK_PER_CONTRACT",
                        "risk_per_contract must be positive",
                    )
                qty = floor(risk_cash / risk_per_contract)
                if sizing_config.max_contracts is not None:
                    qty = min(qty, sizing_config.max_contracts)
                if sizing_config.max_margin_fraction is not None:
                    margin_cap = float(initial_cash) * sizing_config.max_margin_fraction
                    qty = min(qty, floor(margin_cap / economics.initial_margin))
                if qty < 1:
                    raise MoneyModelError("MONEY_SIZE_REJECTED", "resolved qty is below 1")
            except MoneyModelError as exc:
                cleaned_entries.at[ts, column] = False
                cleaned_short_entries.at[ts, column] = False
                rejection_events.append(
                    _rejection_event(column, ts=ts, failure_code=exc.failure_code, reason=str(exc))
                )
                continue

            size.at[ts, column] = float(qty)
            risk_cash_frame.at[ts, column] = risk_cash
            risk_per_contract_frame.at[ts, column] = risk_per_contract
            margin_required_frame.at[ts, column] = float(qty) * economics.initial_margin

    return MoneyPreparedSurface(
        entries=cleaned_entries.astype(bool),
        short_entries=cleaned_short_entries.astype(bool),
        size=size,
        risk_cash=risk_cash_frame,
        risk_per_contract=risk_per_contract_frame,
        margin_required=margin_required_frame,
        rejection_events=tuple(rejection_events),
    )


def trade_ledger_from_vectorbt_records(
    *,
    records: Sequence[dict[str, object]],
    run_row: Mapping[str, object],
    index: Sequence[object],
    metadata_frame: pd.DataFrame | None,
    risk_cash_frame: pd.Series | None,
    risk_per_contract_frame: pd.Series | None,
    margin_required_frame: pd.Series | None,
    commission_per_contract: float,
    slippage_ticks: float,
) -> MoneyTradeLedger:
    rows: list[dict[str, object]] = []
    equity_points: list[tuple[int, float]] = []
    cumulative = 0.0
    for record in records:
        entry_idx = int(record.get("entry_idx", 0))
        exit_idx = int(record.get("exit_idx", entry_idx))
        direction = "long" if int(record.get("direction", 0)) == 0 else "short"
        status = "closed" if int(record.get("status", 1)) == 1 else "open"
        qty = abs(float(record.get("size", 0.0) or 0.0))
        entry_price = float(record.get("entry_price", 0.0) or 0.0)
        exit_price = float(record.get("exit_price", entry_price) or entry_price)
        economics = execution_economics_from_row(
            _metadata_at(metadata_frame, entry_idx),
            entry_price=entry_price,
            commission_per_contract=commission_per_contract,
            slippage_ticks=slippage_ticks,
        )
        price_delta = exit_price - entry_price if direction == "long" else entry_price - exit_price
        gross_pnl = (price_delta / economics.tick_size) * economics.tick_value * qty
        side_count = 2.0 if status == "closed" else 1.0
        commission = economics.commission_per_contract * qty * side_count
        slippage = economics.slippage_ticks * economics.tick_value * qty * side_count
        net_pnl = gross_pnl - commission - slippage
        margin_required = _series_value(margin_required_frame, entry_idx)
        if margin_required <= 0:
            margin_required = qty * economics.initial_margin
        risk_cash = _series_value(risk_cash_frame, entry_idx)
        risk_per_contract = _series_value(risk_per_contract_frame, entry_idx)
        return_on_margin = net_pnl / margin_required if margin_required > 0 else 0.0
        cumulative += net_pnl
        equity_points.append((exit_idx, cumulative))
        rows.append(
            {
                "trade_id": f"{run_row['backtest_run_id']}-TRD-{int(record.get('id', 0)):04d}",
                "side": direction,
                "status": status,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": qty,
                "gross_pnl": gross_pnl,
                "net_pnl": net_pnl,
                "commission": commission,
                "slippage": slippage,
                "risk_cash": risk_cash,
                "risk_per_contract": risk_per_contract,
                "risk_per_trade_pct": risk_cash / float(run_row["initial_cash"])
                if float(run_row["initial_cash"]) > 0
                else 0.0,
                "margin_required": margin_required,
                "return_on_margin": return_on_margin,
                "tick_size": economics.tick_size,
                "tick_value": economics.tick_value,
                "lot_size": economics.lot_size,
                "initial_margin": economics.initial_margin,
                "notional_value": economics.notional_value,
                "currency": economics.currency,
                "money_model_version": economics.money_model_version,
                "money_source": economics.money_source,
                "sizing_mode": SIZING_MODE_RISK_PER_TRADE,
                "equity_mode": MONEY_EQUITY_MODE,
                "source_model_version": economics.source_model_version,
            }
        )
    return MoneyTradeLedger(
        trade_rows=tuple(rows),
        aggregate_metrics=money_aggregate_metrics(
            rows,
            initial_cash=float(run_row["initial_cash"]),
            equity_points=equity_points,
        ),
    )


def money_aggregate_metrics(
    trade_rows: Sequence[Mapping[str, object]],
    *,
    initial_cash: float,
    equity_points: Sequence[tuple[int, float]] = (),
) -> dict[str, float | int | str]:
    net_values = [float(row.get("net_pnl", 0.0) or 0.0) for row in trade_rows]
    positive = [value for value in net_values if value > 0]
    negative = [value for value in net_values if value < 0]
    fees = sum(float(row.get("commission", 0.0) or 0.0) for row in trade_rows)
    slippage = sum(float(row.get("slippage", 0.0) or 0.0) for row in trade_rows)
    net_pnl = sum(net_values)
    loss_abs = abs(sum(negative))
    profit_factor = (
        sum(positive) / loss_abs if loss_abs > 0 else (sum(positive) if positive else 0.0)
    )
    trade_count = len(trade_rows)
    max_drawdown_cash = realized_drawdown_cash(equity_points)
    return {
        "net_pnl": net_pnl,
        "fees_paid": fees,
        "slippage_paid": slippage,
        "profit_factor": profit_factor,
        "win_rate": len(positive) / trade_count if trade_count else 0.0,
        "avg_trade": net_pnl / trade_count if trade_count else 0.0,
        "total_return": net_pnl / initial_cash if initial_cash > 0 else 0.0,
        "max_drawdown": max_drawdown_cash / initial_cash if initial_cash > 0 else 0.0,
        "max_drawdown_cash": max_drawdown_cash,
        "trade_count": trade_count,
        "money_model_version": MONEY_MODEL_VERSION,
        "money_source": MONEY_SOURCE,
        "sizing_mode": SIZING_MODE_RISK_PER_TRADE,
        "equity_mode": MONEY_EQUITY_MODE,
    }


def realized_drawdown_cash(equity_points: Sequence[tuple[int, float]]) -> float:
    peak = 0.0
    max_drawdown = 0.0
    for _, equity in sorted(equity_points, key=lambda item: item[0]):
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _positive(value: object, field_name: str) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise MoneyModelError("MONEY_ECONOMICS_MISSING", f"{field_name} is required") from exc
    if number <= 0 or not isfinite(number):
        raise MoneyModelError("MONEY_ECONOMICS_MISSING", f"{field_name} must be positive")
    return number


def _metadata_at(metadata_frame: pd.DataFrame | None, bar_index: int) -> Mapping[str, object]:
    if metadata_frame is None or metadata_frame.empty:
        return {}
    safe_index = min(max(bar_index, 0), len(metadata_frame) - 1)
    return metadata_frame.iloc[safe_index].to_dict()


def _series_value(series: pd.Series | None, bar_index: int) -> float:
    if series is None or series.empty:
        return 0.0
    safe_index = min(max(bar_index, 0), len(series) - 1)
    value = series.iloc[safe_index]
    return 0.0 if pd.isna(value) else float(value)


def _column_instrument_id(column: object) -> str:
    if isinstance(column, tuple) and len(column) >= 5:
        return str(column[4])
    return str(column)


def _rejection_event(
    column: object, *, ts: object, failure_code: str, reason: str
) -> dict[str, object]:
    param_hash = str(column[3]) if isinstance(column, tuple) and len(column) >= 4 else ""
    instrument_id = _column_instrument_id(column)
    return {
        "ts": pd.Timestamp(ts).isoformat().replace("+00:00", "Z"),
        "param_hash": param_hash,
        "instrument_id": instrument_id,
        "failure_code": failure_code,
        "failure_reason": reason,
        "money_source": MONEY_SOURCE,
    }
