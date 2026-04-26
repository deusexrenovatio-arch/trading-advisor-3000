from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from numba import njit
import vectorbt as vbt
from vectorbt.portfolio import enums, nb

from trading_advisor_3000.product_plane.research.io.loaders import ResearchSeriesFrame
from trading_advisor_3000.product_plane.research.strategies import StrategySpec

if TYPE_CHECKING:
    from .batch_runner import BacktestStrategyInstance


@dataclass(frozen=True)
class BacktestEngineConfig:
    engine_name: str = "vectorbt"
    initial_cash: float = 100_000.0
    position_size: float = 1.0
    fees_bps: float = 0.0
    slippage_bps: float = 0.0
    allow_short: bool = True
    session_hours_utc: tuple[int, int] | None = None
    window_count: int = 1

    def __post_init__(self) -> None:
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        if self.position_size <= 0:
            raise ValueError("position_size must be positive")
        if self.fees_bps < 0:
            raise ValueError("fees_bps must be non-negative")
        if self.slippage_bps < 0:
            raise ValueError("slippage_bps must be non-negative")
        if self.window_count <= 0:
            raise ValueError("window_count must be positive")


@njit
def _directional_order_func_nb(c, direction_signal, exit_signal, size, fees, slippage):
    signal = direction_signal[c.i, c.col]
    price = c.close[c.i, c.col]
    if c.position_now == 0:
        if signal == 1:
            return nb.order_nb(
                size=size,
                price=price,
                size_type=enums.SizeType.Amount,
                direction=enums.Direction.LongOnly,
                fees=fees,
                slippage=slippage,
            )
        if signal == -1:
            return nb.order_nb(
                size=size,
                price=price,
                size_type=enums.SizeType.Amount,
                direction=enums.Direction.ShortOnly,
                fees=fees,
                slippage=slippage,
            )
    elif exit_signal[c.i, c.col]:
        return nb.close_position_nb(
            price=price,
            fees=fees,
            slippage=slippage,
        )
    return nb.no_order_func_nb(c)


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _timeframe_freq(timeframe: str) -> str:
    if timeframe.endswith("m"):
        return f"{int(timeframe[:-1])}min"
    if timeframe.endswith("h"):
        return f"{int(timeframe[:-1])}h"
    if timeframe.endswith("d"):
        return f"{int(timeframe[:-1])}D"
    if timeframe.endswith("w"):
        return f"{int(timeframe[:-1])}W"
    raise ValueError(f"unsupported timeframe token: {timeframe}")


def _bars_per_year(timeframe: str) -> float:
    if timeframe.endswith("m"):
        return (365.0 * 24.0 * 60.0) / float(int(timeframe[:-1]))
    if timeframe.endswith("h"):
        return (365.0 * 24.0) / float(int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return 365.0 / float(int(timeframe[:-1]))
    if timeframe.endswith("w"):
        return 52.0 / float(int(timeframe[:-1]))
    return 0.0


def _annualized_return(total_return: float, *, periods: int, timeframe: str) -> float:
    if periods <= 0:
        return 0.0
    bars_year = _bars_per_year(timeframe)
    if bars_year <= 0.0:
        return 0.0
    return (1.0 + total_return) ** (bars_year / periods) - 1.0


def _scalar(value: object) -> float:
    if isinstance(value, pd.DataFrame):
        numeric = pd.to_numeric(pd.Series(value.to_numpy().reshape(-1)), errors="coerce")
        return float(numeric.mean()) if not numeric.empty else 0.0
    if isinstance(value, pd.Series):
        numeric = pd.to_numeric(value, errors="coerce")
        return float(numeric.mean()) if not numeric.empty else 0.0
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _window_frames(
    frame: pd.DataFrame,
    *,
    window_count: int,
    split_windows: tuple[dict[str, object], ...] | None = None,
) -> tuple[tuple[str, pd.DataFrame], ...]:
    if split_windows:
        resolved: list[tuple[str, pd.DataFrame]] = []
        for index, window in enumerate(split_windows, start=1):
            window_id = str(window.get("window_id", f"wf-{index:02d}"))
            subset = pd.DataFrame()
            test_start = window.get("test_start")
            test_stop = window.get("test_stop")
            if isinstance(test_start, int) and isinstance(test_stop, int):
                start = max(0, min(len(frame), test_start))
                stop = max(start, min(len(frame), test_stop))
                subset = frame.iloc[start:stop].copy()
            if subset.empty and window.get("analysis_start_ts") and window.get("analysis_end_ts"):
                start_ts = str(window["analysis_start_ts"])
                end_ts = str(window["analysis_end_ts"])
                subset = frame[(frame["ts"] >= start_ts) & (frame["ts"] <= end_ts)].copy()
            if len(subset) >= 2:
                resolved.append((window_id, subset))
        if resolved:
            return tuple(resolved)

    if window_count <= 1 or len(frame) < 4:
        return (("wf-01", frame.copy()),)
    base_size = max(2, len(frame) // window_count)
    windows: list[tuple[str, pd.DataFrame]] = []
    start = 0
    for index in range(window_count):
        stop = len(frame) if index == window_count - 1 else min(len(frame), start + base_size)
        if stop - start < 2:
            continue
        windows.append((f"wf-{index + 1:02d}", frame.iloc[start:stop].copy()))
        start = stop
    return tuple(windows) or (("wf-01", frame.copy()),)


def _session_entry_mask(index: pd.Index, session_hours_utc: tuple[int, int] | None) -> pd.Series:
    if session_hours_utc is None:
        return pd.Series(True, index=index)
    timestamps = pd.to_datetime(index, utc=True)
    hours = timestamps.hour
    start_hour, end_hour = session_hours_utc
    if start_hour <= end_hour:
        mask = (hours >= start_hour) & (hours < end_hour)
    else:
        mask = (hours >= start_hour) | (hours < end_hour)
    return pd.Series(mask, index=index)


def _require_columns(frame: pd.DataFrame, spec: StrategySpec) -> None:
    missing = [column for column in spec.required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"strategy `{spec.version}` missing required columns: {', '.join(missing)}")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def _ma_column(frame: pd.DataFrame, window: int) -> pd.Series:
    column_name = f"ema_{window}"
    if column_name not in frame.columns:
        raise ValueError(f"missing required moving-average column: {column_name}")
    return _numeric(frame, column_name)


def _risk_multipliers(spec: StrategySpec, params: dict[str, object]) -> tuple[float, float]:
    stop_multiple = float(params.get("stop_atr_multiple", spec.risk_policy.stop_atr_multiple))
    target_multiple = float(params.get("atr_target_multiple", spec.risk_policy.target_atr_multiple))
    return stop_multiple, target_multiple


def _atr_relative(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object]) -> tuple[pd.Series, pd.Series]:
    stop_multiple, target_multiple = _risk_multipliers(spec, params)
    atr = _numeric(frame, "atr_14")
    close = _numeric(frame, "close").replace({0.0: pd.NA})
    return (atr * stop_multiple / close), (atr * target_multiple / close)


def _ma_cross_signals(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object], config: BacktestEngineConfig) -> dict[str, pd.Series]:
    fast = _ma_column(frame, int(params["fast_window"]))
    slow = _ma_column(frame, int(params["slow_window"]))
    ema_10 = _numeric(frame, "ema_10")
    ema_20 = _numeric(frame, "ema_20")
    ema_50 = _numeric(frame, "ema_50")
    session_mask = _session_entry_mask(frame.index, config.session_hours_utc)

    long_state = (fast > slow) & (ema_10 >= ema_20) & (ema_20 >= ema_50)
    short_state = config.allow_short & (fast < slow) & (ema_10 <= ema_20) & (ema_20 <= ema_50)
    entries = long_state & ~long_state.shift(1, fill_value=False) & session_mask
    exits = (~long_state) & long_state.shift(1, fill_value=False)
    short_entries = short_state & ~short_state.shift(1, fill_value=False) & session_mask
    short_exits = (~short_state) & short_state.shift(1, fill_value=False)
    sl_stop, tp_stop = _atr_relative(frame, spec, params)
    return {
        "entries": entries.fillna(False),
        "exits": exits.fillna(False),
        "short_entries": short_entries.fillna(False),
        "short_exits": short_exits.fillna(False),
        "sl_stop": sl_stop,
        "tp_stop": tp_stop,
    }


def _breakout_signals(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object], config: BacktestEngineConfig) -> dict[str, pd.Series]:
    close = _numeric(frame, "close")
    breakout_window = int(params["breakout_window"])
    rolling_high = _numeric(frame, "high").rolling(window=breakout_window, min_periods=breakout_window).max().shift(1)
    rolling_low = _numeric(frame, "low").rolling(window=breakout_window, min_periods=breakout_window).min().shift(1)
    adx = _numeric(frame, "adx_14")
    session_mask = _session_entry_mask(frame.index, config.session_hours_utc)
    buffer = _numeric(frame, "atr_14").fillna(0.0) * float(params["entry_buffer_atr"])

    long_state = (adx >= float(params["min_adx"])) & (close >= (rolling_high + buffer))
    short_state = config.allow_short & (adx >= float(params["min_adx"])) & (close <= (rolling_low - buffer))
    entries = long_state & ~long_state.shift(1, fill_value=False) & session_mask
    exits = (close < rolling_high) & long_state.shift(1, fill_value=False)
    short_entries = short_state & ~short_state.shift(1, fill_value=False) & session_mask
    short_exits = (close > rolling_low) & short_state.shift(1, fill_value=False)
    sl_stop, tp_stop = _atr_relative(frame, spec, params)
    return {
        "entries": entries.fillna(False),
        "exits": exits.fillna(False),
        "short_entries": short_entries.fillna(False),
        "short_exits": short_exits.fillna(False),
        "sl_stop": sl_stop,
        "tp_stop": tp_stop,
    }


def _mean_reversion_signals(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object], config: BacktestEngineConfig) -> dict[str, pd.Series]:
    rsi = _numeric(frame, "rsi_14")
    distance = _numeric(frame, "distance_to_session_vwap")
    session_mask = _session_entry_mask(frame.index, config.session_hours_utc)
    entry_distance = float(params["entry_distance_atr"])

    long_state = (rsi <= float(params["entry_rsi"])) & (distance <= -entry_distance)
    short_state = config.allow_short & (rsi >= (100.0 - float(params["entry_rsi"]))) & (distance >= entry_distance)
    entries = long_state & ~long_state.shift(1, fill_value=False) & session_mask
    exits = ((rsi >= float(params["exit_rsi"])) | (distance >= 0.0)) & long_state.shift(1, fill_value=False)
    short_entries = short_state & ~short_state.shift(1, fill_value=False) & session_mask
    short_exits = ((rsi <= (100.0 - float(params["exit_rsi"]))) | (distance <= 0.0)) & short_state.shift(1, fill_value=False)
    sl_stop, tp_stop = _atr_relative(frame, spec, params)
    return {
        "entries": entries.fillna(False),
        "exits": exits.fillna(False),
        "short_entries": short_entries.fillna(False),
        "short_exits": short_exits.fillna(False),
        "sl_stop": sl_stop,
        "tp_stop": tp_stop,
    }


def _mtf_pullback_signals(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object], config: BacktestEngineConfig) -> dict[str, pd.Series]:
    distance = _numeric(frame, "distance_to_session_vwap")
    local_ema_20 = _numeric(frame, "ema_20")
    local_ema_50 = _numeric(frame, "ema_50")
    htf_ema_20 = _numeric(frame, "mtf_1h_to_15m_ema_20")
    htf_ema_50 = _numeric(frame, "mtf_1h_to_15m_ema_50")
    htf_rsi = _numeric(frame, "mtf_1h_to_15m_rsi_14")
    confirmation = int(params["confirmation_bars"])
    session_mask = _session_entry_mask(frame.index, config.session_hours_utc)
    local_up = local_ema_20 > local_ema_50
    local_down = local_ema_20 < local_ema_50
    htf_up = htf_ema_20 > htf_ema_50
    htf_down = htf_ema_20 < htf_ema_50

    base_long = (
        local_up
        & htf_up
        & (htf_rsi >= float(params["min_htf_rsi"]))
        & (distance <= -float(params["pullback_depth"]))
    )
    base_short = config.allow_short & (
        local_down
        & htf_down
        & (htf_rsi <= (100.0 - float(params["min_htf_rsi"])))
        & (distance >= float(params["pullback_depth"]))
    )
    long_state = base_long.rolling(window=confirmation, min_periods=confirmation).sum() == confirmation
    short_state = base_short.rolling(window=confirmation, min_periods=confirmation).sum() == confirmation
    entries = long_state & ~long_state.shift(1, fill_value=False) & session_mask
    exits = ((~local_up) | (distance >= 0.0)) & long_state.shift(1, fill_value=False)
    short_entries = short_state & ~short_state.shift(1, fill_value=False) & session_mask
    short_exits = ((~local_down) | (distance <= 0.0)) & short_state.shift(1, fill_value=False)
    sl_stop, tp_stop = _atr_relative(frame, spec, params)
    return {
        "entries": entries.fillna(False),
        "exits": exits.fillna(False),
        "short_entries": short_entries.fillna(False),
        "short_exits": short_exits.fillna(False),
        "sl_stop": sl_stop,
        "tp_stop": tp_stop,
    }


def _squeeze_release_signals(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object], config: BacktestEngineConfig) -> dict[str, pd.Series]:
    close = _numeric(frame, "close")
    atr = _numeric(frame, "atr_14")
    bb_position = _numeric(frame, "bb_position_20_2")
    kc_position = _numeric(frame, "kc_position_20_1_5")
    high_cross = _numeric(frame, "cross_close_rolling_high_20_code")
    low_cross = _numeric(frame, "cross_close_rolling_low_20_code")
    ema_20 = _numeric(frame, "ema_20")
    ema_50 = _numeric(frame, "ema_50")
    session_mask = _session_entry_mask(frame.index, config.session_hours_utc)
    confirmation = int(params["release_confirmation"])
    min_squeeze_bars = int(params["min_squeeze_bars"])
    stop_multiple, target_multiple = _risk_multipliers(spec, params)

    channel_mid = bb_position.between(0.35, 0.65) & kc_position.between(0.35, 0.65)
    setup_count = channel_mid.astype(int).rolling(window=min_squeeze_bars, min_periods=min_squeeze_bars).sum()
    setup_ready = setup_count.shift(1, fill_value=0) >= min_squeeze_bars
    trend_up = ema_20 >= ema_50
    trend_down = ema_20 <= ema_50
    release_up = (high_cross > 0) | ((bb_position >= 0.8) & (kc_position >= 0.65))
    release_down = (low_cross < 0) | ((bb_position <= 0.2) & (kc_position <= 0.35))
    long_state = setup_ready & release_up & trend_up
    short_state = config.allow_short & setup_ready & release_down & trend_down
    if confirmation > 1:
        long_state = long_state.rolling(window=confirmation, min_periods=confirmation).sum() == confirmation
        short_state = short_state.rolling(window=confirmation, min_periods=confirmation).sum() == confirmation

    direction_signal = pd.Series(0, index=frame.index, dtype="int64")
    exit_signal = pd.Series(False, index=frame.index, dtype="bool")
    active_direction = 0
    entry_price = 0.0
    entry_atr = 0.0
    for idx in range(len(frame)):
        if active_direction == 0:
            if bool(long_state.iloc[idx]) and bool(session_mask.iloc[idx]) and not pd.isna(close.iloc[idx]) and not pd.isna(atr.iloc[idx]):
                direction_signal.iloc[idx] = 1
                active_direction = 1
                entry_price = float(close.iloc[idx])
                entry_atr = float(atr.iloc[idx])
            elif bool(short_state.iloc[idx]) and bool(session_mask.iloc[idx]) and not pd.isna(close.iloc[idx]) and not pd.isna(atr.iloc[idx]):
                direction_signal.iloc[idx] = -1
                active_direction = -1
                entry_price = float(close.iloc[idx])
                entry_atr = float(atr.iloc[idx])
            continue

        current_close = float(close.iloc[idx]) if not pd.isna(close.iloc[idx]) else entry_price
        if active_direction > 0:
            stop_price = entry_price - (entry_atr * stop_multiple)
            target_price = entry_price + (entry_atr * target_multiple)
            lost_context = bool((not trend_up.iloc[idx]) or low_cross.iloc[idx] < 0 or bb_position.iloc[idx] < 0.25)
            should_exit = current_close <= stop_price or current_close >= target_price or lost_context
        else:
            stop_price = entry_price + (entry_atr * stop_multiple)
            target_price = entry_price - (entry_atr * target_multiple)
            lost_context = bool((not trend_down.iloc[idx]) or high_cross.iloc[idx] > 0 or bb_position.iloc[idx] > 0.75)
            should_exit = current_close >= stop_price or current_close <= target_price or lost_context
        if should_exit:
            exit_signal.iloc[idx] = True
            active_direction = 0
            entry_price = 0.0
            entry_atr = 0.0
    return {
        "direction_signal": direction_signal.fillna(0),
        "exit_signal": exit_signal.fillna(False),
    }


def _apply_direction_mode(spec: StrategySpec, config: BacktestEngineConfig, signals: dict[str, pd.Series]) -> dict[str, pd.Series]:
    mode = spec.direction_mode
    normalized = dict(signals)
    if mode == "long_only" or not config.allow_short:
        if "short_entries" in normalized:
            normalized["short_entries"] = normalized["short_entries"] & False
        if "short_exits" in normalized:
            normalized["short_exits"] = normalized["short_exits"] & False
        if "direction_signal" in normalized:
            normalized["direction_signal"] = normalized["direction_signal"].mask(normalized["direction_signal"] < 0, 0)
        return normalized
    if mode == "short_only":
        if "entries" in normalized:
            normalized["entries"] = normalized["entries"] & False
        if "exits" in normalized:
            normalized["exits"] = normalized["exits"] & False
        if "direction_signal" in normalized:
            normalized["direction_signal"] = normalized["direction_signal"].mask(normalized["direction_signal"] > 0, 0)
        return normalized
    return normalized


def _strategy_signals(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object], config: BacktestEngineConfig) -> dict[str, pd.Series]:
    _require_columns(frame, spec)
    if spec.signal_builder_key == "ma_cross":
        return _apply_direction_mode(spec, config, _ma_cross_signals(frame, spec, params, config))
    if spec.signal_builder_key == "breakout":
        return _apply_direction_mode(spec, config, _breakout_signals(frame, spec, params, config))
    if spec.signal_builder_key == "mean_reversion":
        return _apply_direction_mode(spec, config, _mean_reversion_signals(frame, spec, params, config))
    if spec.signal_builder_key == "mtf_pullback":
        return _apply_direction_mode(spec, config, _mtf_pullback_signals(frame, spec, params, config))
    if spec.signal_builder_key == "squeeze_release":
        return _apply_direction_mode(spec, config, _squeeze_release_signals(frame, spec, params, config))
    raise ValueError(f"unsupported signal builder key: {spec.signal_builder_key}")


def _run_portfolio(frame: pd.DataFrame, spec: StrategySpec, params: dict[str, object], config: BacktestEngineConfig) -> vbt.Portfolio:
    signals = _strategy_signals(frame, spec, params, config)
    fees = config.fees_bps / 10_000.0
    slippage = config.slippage_bps / 10_000.0
    close = _numeric(frame, "close")
    freq = _timeframe_freq(str(frame["timeframe"].iloc[0]))

    if spec.execution_mode == "order_func":
        close_frame = close.to_frame(name="close")
        direction_signal = np.asarray(signals["direction_signal"], dtype=np.int64).reshape(-1, 1)
        exit_signal = np.asarray(signals["exit_signal"], dtype=np.bool_).reshape(-1, 1)
        return vbt.Portfolio.from_order_func(
            close_frame,
            _directional_order_func_nb,
            direction_signal,
            exit_signal,
            float(config.position_size),
            fees,
            slippage,
            init_cash=config.initial_cash,
            freq=freq,
        )

    kwargs = {
        "size": config.position_size,
        "size_type": enums.SizeType.Amount,
        "fees": fees,
        "slippage": slippage,
        "sl_stop": signals["sl_stop"],
        "tp_stop": signals["tp_stop"],
        "init_cash": config.initial_cash,
        "freq": freq,
    }
    if spec.direction_mode == "long_short" and config.allow_short:
        return vbt.Portfolio.from_signals(
            close,
            signals["entries"],
            signals["exits"],
            short_entries=signals["short_entries"],
            short_exits=signals["short_exits"],
            **kwargs,
        )
    if spec.direction_mode == "short_only" and config.allow_short:
        return vbt.Portfolio.from_signals(
            close,
            signals["short_entries"],
            signals["short_exits"],
            direction=enums.Direction.ShortOnly,
            **kwargs,
        )
    return vbt.Portfolio.from_signals(
        close,
        signals["entries"],
        signals["exits"],
        direction=enums.Direction.LongOnly,
        **kwargs,
    )


def _param_hash(params: dict[str, object]) -> str:
    return _stable_hash(json.dumps(params, ensure_ascii=False, sort_keys=True))


def _run_id(
    *,
    batch_id: str,
    strategy_instance_id: str,
    contract_id: str,
    timeframe: str,
    window_id: str,
) -> str:
    return "BTRUN-" + _stable_hash(f"{batch_id}|{strategy_instance_id}|{contract_id}|{timeframe}|{window_id}")


def _trade_rows(
    *,
    portfolio: vbt.Portfolio,
    run_row: dict[str, object],
    series: ResearchSeriesFrame,
) -> list[dict[str, object]]:
    records = portfolio.trades.records
    if records.empty:
        return []
    rows: list[dict[str, object]] = []
    index = list(series.frame.index)
    for _, trade in records.iterrows():
        entry_idx = int(trade["entry_idx"])
        exit_idx = int(trade["exit_idx"])
        entry_ts = pd.Timestamp(index[entry_idx]).isoformat().replace("+00:00", "Z")
        if 0 <= exit_idx < len(index):
            exit_ts = pd.Timestamp(index[exit_idx]).isoformat().replace("+00:00", "Z")
            duration_bars = max(0, exit_idx - entry_idx)
        else:
            exit_ts = entry_ts
            duration_bars = 0
        direction = "long" if int(trade["direction"]) == 0 else "short"
        status = "closed" if int(trade["status"]) == 1 else "open"
        rows.append(
            {
                "backtest_run_id": run_row["backtest_run_id"],
                "campaign_run_id": run_row["campaign_run_id"],
                "strategy_instance_id": run_row["strategy_instance_id"],
                "strategy_template_id": run_row["strategy_template_id"],
                "family_id": run_row["family_id"],
                "family_key": run_row["family_key"],
                "contract_id": run_row["contract_id"],
                "instrument_id": run_row["instrument_id"],
                "timeframe": run_row["timeframe"],
                "window_id": run_row["window_id"],
                "trade_id": f"{run_row['backtest_run_id']}-TRD-{int(trade['id']):04d}",
                "side": direction,
                "status": status,
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_price": float(trade["entry_price"]),
                "exit_price": float(trade["exit_price"]),
                "qty": abs(float(trade["size"])),
                "gross_pnl": float(trade["pnl"]),
                "net_pnl": float(trade["pnl"]) - (float(trade["entry_fees"]) + float(trade["exit_fees"])),
                "commission": float(trade["entry_fees"]) + float(trade["exit_fees"]),
                "slippage": 0.0,
                "holding_bars": duration_bars,
                "stop_ref": run_row["stop_ref"],
                "target_ref": run_row["target_ref"],
              }
              )
    return rows


def _order_rows(
    *,
    portfolio: vbt.Portfolio,
    run_row: dict[str, object],
    series: ResearchSeriesFrame,
) -> list[dict[str, object]]:
    records = portfolio.orders.records
    if records.empty:
        return []
    rows: list[dict[str, object]] = []
    index = list(series.frame.index)
    for _, order in records.iterrows():
        bar_index = int(order["idx"])
        ts = pd.Timestamp(index[bar_index]).isoformat().replace("+00:00", "Z")
        action = "buy" if int(order["side"]) == 0 else "sell"
        size = float(order["size"])
        price = float(order["price"])
        rows.append(
            {
                "backtest_run_id": run_row["backtest_run_id"],
                "campaign_run_id": run_row["campaign_run_id"],
                "strategy_instance_id": run_row["strategy_instance_id"],
                "family_key": run_row["family_key"],
                "contract_id": run_row["contract_id"],
                "instrument_id": run_row["instrument_id"],
                "timeframe": run_row["timeframe"],
                "window_id": run_row["window_id"],
                "order_id": f"{run_row['backtest_run_id']}-ORD-{int(order['id']):04d}",
                "ts": ts,
                "side": action,
                "order_type": "market",
                "price": price,
                "qty": abs(size),
                "fill_price": price,
                "fill_qty": abs(size),
                "commission": float(order["fees"]),
                "slippage": 0.0,
                "status": "filled",
            }
        )
    return rows


def _drawdown_rows(
    *,
    portfolio: vbt.Portfolio,
    run_row: dict[str, object],
    series: ResearchSeriesFrame,
) -> list[dict[str, object]]:
    records = portfolio.drawdowns.records
    if records.empty:
        return []
    rows: list[dict[str, object]] = []
    index = list(series.frame.index)
    for _, record in records.iterrows():
        peak_idx = int(record["peak_idx"])
        start_idx = int(record["start_idx"])
        valley_idx = int(record["valley_idx"])
        end_idx = int(record["end_idx"])
        peak_val = float(record["peak_val"])
        valley_val = float(record["valley_val"])
        end_val = float(record["end_val"])
        drawdown_pct = ((peak_val - valley_val) / peak_val) if peak_val > 0.0 else 0.0
        recovery_pct = ((end_val - valley_val) / peak_val) if peak_val > 0.0 else 0.0
        status_code = int(record["status"])
        status = "active" if end_idx >= len(index) - 1 and end_val < peak_val else "recovered"
        rows.append(
            {
                "backtest_run_id": run_row["backtest_run_id"],
                "campaign_run_id": run_row["campaign_run_id"],
                "strategy_instance_id": run_row["strategy_instance_id"],
                "family_key": run_row["family_key"],
                "timeframe": run_row["timeframe"],
                "ts": pd.Timestamp(index[valley_idx]).isoformat().replace("+00:00", "Z"),
                "equity": valley_val,
                "drawdown": peak_val - valley_val,
                "peak_equity": peak_val,
                "window_id": run_row["window_id"],
                "drawdown_pct": drawdown_pct,
                "status_code": status_code,
                "status": status,
            }
        )
    return rows


def project_series_candidate(
    *,
    series: ResearchSeriesFrame,
    strategy_spec: StrategySpec,
    params: dict[str, object],
    config: BacktestEngineConfig,
    dataset_version: str,
    feature_set_version: str,
    decision_lag_bars_max: int = 1,
) -> dict[str, object] | None:
    if decision_lag_bars_max < 0:
        raise ValueError("decision_lag_bars_max must be non-negative")
    frame = series.frame.copy()
    signals = _strategy_signals(frame, strategy_spec, params, config)
    close = _numeric(frame, "close")
    long_entries = signals.get("entries")
    short_entries = signals.get("short_entries")
    if "direction_signal" in signals:
        direction = pd.Series(signals["direction_signal"], index=frame.index).fillna(0).astype(int)
        long_entries = direction > 0
        short_entries = direction < 0
    long_entries = pd.Series(False, index=frame.index) if long_entries is None else pd.Series(long_entries, index=frame.index).fillna(False)
    short_entries = pd.Series(False, index=frame.index) if short_entries is None else pd.Series(short_entries, index=frame.index).fillna(False)
    entry_candidates: list[tuple[int, str]] = []
    entry_candidates.extend((idx, "long") for idx, active in enumerate(long_entries.tolist()) if bool(active))
    entry_candidates.extend((idx, "short") for idx, active in enumerate(short_entries.tolist()) if bool(active))
    if not entry_candidates:
        return None

    signal_index, side = max(entry_candidates, key=lambda item: item[0])
    freshness_bars = (len(frame) - 1) - signal_index
    if freshness_bars > decision_lag_bars_max:
        return None

    entry_price = float(close.iloc[signal_index])
    if pd.isna(entry_price):
        return None
    sl_stop = pd.Series(signals.get("sl_stop"), index=frame.index)
    tp_stop = pd.Series(signals.get("tp_stop"), index=frame.index)
    stop_relative = float(sl_stop.iloc[signal_index]) if signal_index < len(sl_stop) and not pd.isna(sl_stop.iloc[signal_index]) else 0.0
    target_relative = float(tp_stop.iloc[signal_index]) if signal_index < len(tp_stop) and not pd.isna(tp_stop.iloc[signal_index]) else 0.0

    if side == "long":
        stop_ref = entry_price * max(0.0, 1.0 - stop_relative)
        target_ref = entry_price * (1.0 + target_relative)
    else:
        stop_ref = entry_price * (1.0 + stop_relative)
        target_ref = entry_price * max(0.0, 1.0 - target_relative)

    risk_distance = abs(entry_price - stop_ref)
    reward_distance = abs(target_ref - entry_price)
    if risk_distance <= 0.0 or reward_distance <= 0.0:
        return None
    risk_reward = reward_distance / max(risk_distance, 1e-9)
    freshness_score = max(0.0, 1.0 - (freshness_bars / max(decision_lag_bars_max + 1, 1)))
    signal_strength = max(0.0, min(1.0, 0.35 + (0.35 * min(risk_reward / 2.0, 1.0)) + (0.30 * freshness_score)))
    ts_decision = str(frame["ts"].iloc[signal_index])
    snapshot_id = "FSNAP-" + _stable_hash(
        f"{dataset_version}|{feature_set_version}|{series.contract_id}|{series.timeframe}|{ts_decision}"
    )
    return {
        "side": side,
        "entry_ref": entry_price,
        "stop_ref": stop_ref,
        "target_ref": target_ref,
        "ts_decision": ts_decision,
        "signal_strength_score": signal_strength,
        "feature_snapshot": {
            "dataset_version": dataset_version,
            "snapshot_id": snapshot_id,
        },
    }


def run_backtest_series(
    *,
    series: ResearchSeriesFrame,
    strategy_spec: StrategySpec,
    strategy_instance: "BacktestStrategyInstance",
    config: BacktestEngineConfig,
    backtest_batch_id: str,
    campaign_run_id: str,
    strategy_space_id: str,
    dataset_version: str,
    indicator_set_version: str,
    feature_set_version: str,
    split_windows: tuple[dict[str, object], ...] | None = None,
) -> dict[str, list[dict[str, object]]]:
    params = dict(strategy_instance.parameter_values)
    params_hash = _param_hash(params)
    created_at = _created_at()
    run_rows: list[dict[str, object]] = []
    stat_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    order_rows: list[dict[str, object]] = []
    drawdown_rows: list[dict[str, object]] = []

    for window_id, window_frame in _window_frames(series.frame, window_count=config.window_count, split_windows=split_windows):
        portfolio = _run_portfolio(window_frame, strategy_spec, params, config)
        window_series = ResearchSeriesFrame(
            contract_id=series.contract_id,
            instrument_id=series.instrument_id,
            timeframe=series.timeframe,
            frame=window_frame,
        )
        run_row_seed = {
            "backtest_run_id": _run_id(
                batch_id=backtest_batch_id,
                strategy_instance_id=strategy_instance.strategy_instance_id,
                contract_id=series.contract_id,
                timeframe=series.timeframe,
                window_id=window_id,
            ),
            "backtest_batch_id": backtest_batch_id,
            "campaign_run_id": campaign_run_id,
            "strategy_space_id": strategy_space_id,
            "strategy_instance_id": strategy_instance.strategy_instance_id,
            "strategy_template_id": strategy_instance.strategy_template_id,
            "family_id": strategy_instance.family_id,
            "family_key": strategy_instance.family_key,
            "strategy_version_label": strategy_instance.strategy_version_label,
            "dataset_version": dataset_version,
            "indicator_set_version": indicator_set_version,
            "feature_set_version": feature_set_version,
            "contract_id": series.contract_id,
            "instrument_id": series.instrument_id,
            "timeframe": series.timeframe,
            "window_id": window_id,
            "stop_ref": 0.0,
            "target_ref": 0.0,
        }
        signal_projection = project_series_candidate(
            series=window_series,
            strategy_spec=strategy_spec,
            params=params,
            config=config,
            dataset_version=dataset_version,
            feature_set_version=feature_set_version,
            decision_lag_bars_max=max(len(window_frame), 1),
        )
        run_row_seed["stop_ref"] = float(signal_projection["stop_ref"]) if signal_projection is not None else 0.0
        run_row_seed["target_ref"] = float(signal_projection["target_ref"]) if signal_projection is not None else 0.0
        trade_records = _trade_rows(
            portfolio=portfolio,
            run_row=run_row_seed,
            series=window_series,
        )
        order_records = _order_rows(
            portfolio=portfolio,
            run_row=run_row_seed,
            series=window_series,
        )
        drawdown_records = _drawdown_rows(
            portfolio=portfolio,
            run_row=run_row_seed,
            series=window_series,
        )
        orders = portfolio.orders.records
        slippage_rate = config.slippage_bps / 10_000.0
        slippage_total = float((orders["size"].abs() * orders["price"].abs()).sum()) * slippage_rate if not orders.empty else 0.0
        fees_total = float(orders["fees"].sum()) if not orders.empty else 0.0
        exposure = _scalar(portfolio.gross_exposure().max())
        total_return = _scalar(portfolio.total_return())
        avg_holding_bars = (
            sum(float(row["holding_bars"]) for row in trade_records) / len(trade_records)
            if trade_records else 0.0
        )
        turnover = sum(
            abs(float(row["qty"])) * (abs(float(row["entry_price"])) + abs(float(row["exit_price"])))
            for row in trade_records
        )
        avg_trade = (
            sum(float(row["net_pnl"]) for row in trade_records) / len(trade_records)
            if trade_records else 0.0
        )
        stats_row = {
            "backtest_run_id": _run_id(
                batch_id=backtest_batch_id,
                strategy_instance_id=strategy_instance.strategy_instance_id,
                contract_id=series.contract_id,
                timeframe=series.timeframe,
                window_id=window_id,
            ),
            "campaign_run_id": campaign_run_id,
            "strategy_instance_id": strategy_instance.strategy_instance_id,
            "strategy_template_id": strategy_instance.strategy_template_id,
            "family_id": strategy_instance.family_id,
            "family_key": strategy_instance.family_key,
            "strategy_version_label": strategy_instance.strategy_version_label,
            "dataset_version": dataset_version,
            "contract_id": series.contract_id,
            "instrument_id": series.instrument_id,
            "timeframe": series.timeframe,
            "window_id": window_id,
            "total_return": total_return,
            "annualized_return": _annualized_return(total_return, periods=len(window_frame), timeframe=series.timeframe),
            "sharpe": _scalar(portfolio.sharpe_ratio()),
            "sortino": _scalar(portfolio.sortino_ratio()),
            "calmar": _scalar(portfolio.calmar_ratio()),
            "max_drawdown": _scalar(portfolio.max_drawdown()),
            "win_rate": _scalar(portfolio.trades.win_rate()),
            "profit_factor": _scalar(portfolio.trades.profit_factor()),
            "expectancy": _scalar(portfolio.trades.expectancy()),
            "avg_trade": avg_trade,
            "avg_holding_bars": avg_holding_bars,
            "turnover": turnover,
            "trade_count": len(trade_records),
            "exposure": exposure,
            "commission_total": fees_total,
            "slippage_total": slippage_total,
            "status": "completed",
            "created_at": created_at,
          }
        run_row = {
            "backtest_run_id": stats_row["backtest_run_id"],
            "backtest_batch_id": backtest_batch_id,
            "campaign_run_id": campaign_run_id,
            "strategy_instance_id": strategy_instance.strategy_instance_id,
            "strategy_template_id": strategy_instance.strategy_template_id,
            "family_id": strategy_instance.family_id,
            "family_key": strategy_instance.family_key,
            "strategy_version_label": strategy_instance.strategy_version_label,
            "dataset_version": dataset_version,
            "indicator_set_version": indicator_set_version,
            "feature_set_version": feature_set_version,
            "contract_id": series.contract_id,
            "instrument_id": series.instrument_id,
            "timeframe": series.timeframe,
            "window_id": window_id,
            "validation_split_id": window_id,
            "parameter_values_json": params,
            "execution_mode": strategy_spec.execution_mode,
            "engine_name": config.engine_name,
            "row_count": len(window_frame),
            "trade_count": len(trade_records),
            "status": "completed",
            "started_at": str(window_frame["ts"].iloc[0]),
            "finished_at": str(window_frame["ts"].iloc[-1]),
        }
        for row in trade_records:
            row["backtest_run_id"] = run_row["backtest_run_id"]
        run_rows.append(run_row)
        stat_rows.append(stats_row)
        trade_rows.extend(trade_records)
        order_rows.extend(order_records)
        drawdown_rows.extend(drawdown_records)

    return {
        "run_rows": run_rows,
        "stat_rows": stat_rows,
        "trade_rows": trade_rows,
        "order_rows": order_rows,
        "drawdown_rows": drawdown_rows,
    }
