from __future__ import annotations

import pandas as pd

from trading_advisor_3000.product_plane.research.backtests.engine import (
    BacktestEngineConfig,
    _breakout_signals,
    _ma_cross_signals,
    _squeeze_release_signals,
)
from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame.index = pd.to_datetime(frame["ts"], utc=True)
    return frame


def test_breakout_window_changes_signal_generation() -> None:
    timestamps = (
        "2026-03-16T09:00:00Z",
        "2026-03-16T09:15:00Z",
        "2026-03-16T09:30:00Z",
        "2026-03-16T09:45:00Z",
        "2026-03-16T10:00:00Z",
        "2026-03-16T10:15:00Z",
    )
    frame = _frame(
        [
            {"ts": ts, "close": close, "high": high, "low": low, "adx_14": 25.0, "atr_14": 1.0}
            for ts, (close, high, low) in zip(
                timestamps,
                (
                    (100.0, 100.4, 99.6),
                    (100.6, 100.8, 100.1),
                    (101.0, 101.2, 100.5),
                    (100.7, 100.9, 100.2),
                    (101.5, 101.9, 101.0),
                    (101.7, 101.8, 101.2),
                ),
                strict=True,
            )
        ]
    )
    spec = StrategySpec(
        version="breakout-semantic-v1",
        family="breakout",
        description="semantic test",
        required_columns=("close", "high", "low", "adx_14", "atr_14"),
        parameter_grid=(
            StrategyParameter("breakout_window", (2, 5)),
            StrategyParameter("min_adx", (20,)),
            StrategyParameter("entry_buffer_atr", (0.0,)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(tags=("test",)),
    )
    config = BacktestEngineConfig()

    fast = _breakout_signals(frame, spec, {"breakout_window": 2, "min_adx": 20, "entry_buffer_atr": 0.0}, config)
    slow = _breakout_signals(frame, spec, {"breakout_window": 5, "min_adx": 20, "entry_buffer_atr": 0.0}, config)

    assert fast["entries"].tolist() != slow["entries"].tolist()


def test_risk_policy_drives_signal_stop_take_profit_levels() -> None:
    frame = _frame(
        [
            {
                "ts": f"2026-03-16T09:{minute:02d}:00Z",
                "close": 100.0 + idx,
                "ema_10": 100.0 + idx + 0.4,
                "ema_20": 100.0 + idx,
                "ema_50": 99.0 + idx,
                "atr_14": 2.0,
            }
            for idx, minute in enumerate((0, 15, 30, 45))
        ]
    )
    spec = StrategySpec(
        version="ma-risk-v1",
        family="ma_cross",
        description="risk semantic test",
        required_columns=("close", "ema_10", "ema_20", "ema_50", "atr_14"),
        parameter_grid=(StrategyParameter("fast_window", (10,)), StrategyParameter("slow_window", (20,))),
        signal_builder_key="ma_cross",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=0.5, target_atr_multiple=3.0),
        ranking_metadata=StrategyRankingMetadata(tags=("test",)),
    )

    signals = _ma_cross_signals(frame, spec, {"fast_window": 10, "slow_window": 20}, BacktestEngineConfig())
    assert round(float(signals["sl_stop"].iloc[0]), 4) == 0.01
    assert round(float(signals["tp_stop"].iloc[0]), 4) == 0.06


def test_squeeze_target_multiple_changes_exit_timing() -> None:
    frame = _frame(
        [
            {"ts": "2026-03-16T09:00:00Z", "close": 100.0, "atr_14": 1.0, "ema_20": 100.0, "ema_50": 99.0, "bb_position_20_2": 0.5, "kc_position_20_1_5": 0.5, "cross_close_rolling_high_20_code": 0, "cross_close_rolling_low_20_code": 0},
            {"ts": "2026-03-16T09:15:00Z", "close": 100.1, "atr_14": 1.0, "ema_20": 100.1, "ema_50": 99.1, "bb_position_20_2": 0.5, "kc_position_20_1_5": 0.5, "cross_close_rolling_high_20_code": 0, "cross_close_rolling_low_20_code": 0},
            {"ts": "2026-03-16T09:30:00Z", "close": 100.2, "atr_14": 1.0, "ema_20": 100.2, "ema_50": 99.2, "bb_position_20_2": 0.5, "kc_position_20_1_5": 0.5, "cross_close_rolling_high_20_code": 0, "cross_close_rolling_low_20_code": 0},
            {"ts": "2026-03-16T09:45:00Z", "close": 100.9, "atr_14": 1.0, "ema_20": 100.4, "ema_50": 99.4, "bb_position_20_2": 0.85, "kc_position_20_1_5": 0.75, "cross_close_rolling_high_20_code": 1, "cross_close_rolling_low_20_code": 0},
            {"ts": "2026-03-16T10:00:00Z", "close": 101.5, "atr_14": 1.0, "ema_20": 100.6, "ema_50": 99.6, "bb_position_20_2": 0.9, "kc_position_20_1_5": 0.8, "cross_close_rolling_high_20_code": 0, "cross_close_rolling_low_20_code": 0},
            {"ts": "2026-03-16T10:15:00Z", "close": 100.0, "atr_14": 1.0, "ema_20": 98.0, "ema_50": 99.0, "bb_position_20_2": 0.2, "kc_position_20_1_5": 0.2, "cross_close_rolling_high_20_code": 0, "cross_close_rolling_low_20_code": -1},
        ]
    )
    spec = StrategySpec(
        version="squeeze-semantic-v1",
        family="squeeze_release",
        description="squeeze semantic test",
        required_columns=(
            "close",
            "atr_14",
            "ema_20",
            "ema_50",
            "bb_position_20_2",
            "kc_position_20_1_5",
            "cross_close_rolling_high_20_code",
            "cross_close_rolling_low_20_code",
        ),
        parameter_grid=(
            StrategyParameter("min_squeeze_bars", (3,)),
            StrategyParameter("atr_target_multiple", (0.5, 3.0)),
            StrategyParameter("release_confirmation", (1,)),
        ),
        signal_builder_key="squeeze_release",
        execution_mode="order_func",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=0.75, target_atr_multiple=1.0),
        ranking_metadata=StrategyRankingMetadata(tags=("test",)),
    )
    config = BacktestEngineConfig()

    fast_target = _squeeze_release_signals(
        frame,
        spec,
        {"min_squeeze_bars": 3, "atr_target_multiple": 0.5, "release_confirmation": 1},
        config,
    )
    slow_target = _squeeze_release_signals(
        frame,
        spec,
        {"min_squeeze_bars": 3, "atr_target_multiple": 3.0, "release_confirmation": 1},
        config,
    )

    assert fast_target["exit_signal"].tolist() != slow_target["exit_signal"].tolist()
    assert fast_target["exit_signal"].idxmax() < slow_target["exit_signal"].idxmax()
