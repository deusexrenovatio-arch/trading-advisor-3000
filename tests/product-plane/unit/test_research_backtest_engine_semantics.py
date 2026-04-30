from __future__ import annotations

from collections import Counter

import pandas as pd

from trading_advisor_3000.product_plane.research.backtests.engine import (
    BacktestEngineConfig,
    StrategyFamilySearchSpec,
    _breakout_signals,
    _ma_cross_signals,
    _squeeze_release_signals,
    _windowed_series,
    build_input_bundle,
    build_signal_surface,
    run_surface_portfolio,
    run_vectorbt_family_search,
    strategy_spec_to_search_spec,
)
from trading_advisor_3000.product_plane.research.io.loaders import ResearchSeriesFrame
from trading_advisor_3000.product_plane.research.strategies import build_strategy_registry
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


def _trend_surface_series(instrument_id: str, offset: float = 0.0) -> ResearchSeriesFrame:
    rows: list[dict[str, object]] = []
    for index in range(48):
        close = 100.0 + offset + (index * 0.2)
        rows.append(
            {
                "ts": f"2026-03-16T{9 + (index // 4):02d}:{(index % 4) * 15:02d}:00Z",
                "timeframe": "15m",
                "close": close,
                "ema_20": close + 0.2,
                "ema_50": close - 0.2,
                "macd_hist_12_26_9": 0.2,
                "adx_14": 35.0,
                "atr_14": 1.2,
                "rsi_14": 62.0,
                "close_change_1": 0.2,
                "close_slope_20": 0.002,
                "ema_20_slope_5": 0.002,
                "roc_10_change_1": 0.002,
                "mom_10_change_1": 0.002,
                "cross_close_ema_20_code": 1 if index % 12 == 2 else 0,
                "macd_signal_cross_code": 1 if index % 12 == 2 else 0,
                "ppo_signal_cross_code": 1 if index % 12 == 2 else 0,
                "distance_to_ema_20_atr": -0.1,
                "mtf_1d_to_4h_adx_14": 32.0,
                "mtf_1d_to_4h_ema_20": close + 0.5,
                "mtf_1d_to_4h_ema_50": close - 0.5,
            }
        )
    frame = _frame(rows)
    return ResearchSeriesFrame(
        contract_id=f"{instrument_id}-6.26",
        instrument_id=instrument_id,
        timeframe="15m",
        frame=frame,
    )


def test_mtf_split_windows_use_time_boundaries_before_positional_indices() -> None:
    frame_15m = _frame(
        [
            {"ts": f"2026-03-16T09:{index * 15:02d}:00Z", "timeframe": "15m", "close": 100.0 + index}
            for index in range(4)
        ]
    )
    frame_1h = _frame(
        [
            {"ts": "2026-03-16T09:00:00Z", "timeframe": "1h", "close": 100.0},
            {"ts": "2026-03-16T10:00:00Z", "timeframe": "1h", "close": 101.0},
        ]
    )
    split_windows = (
        {
            "window_id": "wf-time",
            "test_start": 1000,
            "test_stop": 2000,
            "test_start_ts": "2026-03-16T09:00:00Z",
            "test_end_ts": "2026-03-16T10:00:00Z",
        },
    )

    windows = _windowed_series(
        (
            ResearchSeriesFrame(contract_id="BR-6.26", instrument_id="BR", timeframe="15m", frame=frame_15m),
            ResearchSeriesFrame(contract_id="BR-6.26", instrument_id="BR", timeframe="1h", frame=frame_1h),
        ),
        config=BacktestEngineConfig(window_count=1),
        split_windows=split_windows,
    )

    assert [window_id for window_id, _ in windows] == ["wf-time"]
    assert [len(series.frame) for series in windows[0][1]] == [4, 2]


def _native_clock_trend_series(instrument_id: str, timeframe: str, offset: float = 0.0) -> ResearchSeriesFrame:
    if timeframe == "15m":
        rows = [
            {
                "ts": f"2026-03-16T{9 + (index // 4):02d}:{(index % 4) * 15:02d}:00Z",
                "timeframe": "15m",
                "open": 99.8 + offset + (index * 0.2),
                "high": 100.4 + offset + (index * 0.2),
                "low": 99.6 + offset + (index * 0.2),
                "close": 100.0 + offset + (index * 0.2),
                "atr_14": 1.2,
                "obv": 1000.0 + index,
                "mfi_14": 55.0,
                "cmf_20": 0.1,
                "rvol_20": 1.2,
                "volume_z_20": 0.5,
                "oi_change_1": 0.01,
                "oi_roc_10": 0.02,
                "oi_z_20": 0.4,
                "oi_relative_activity_20": 1.1,
                "volume_oi_ratio": 0.05,
                "volume_change_1": 0.1,
                "volume_zscore_20": 0.5,
                "price_volume_corr_20": 0.2,
                "price_oi_corr_20": 0.1,
                "volume_oi_corr_20": 0.1,
                "divergence_price_obv_score": 0.0,
                "divergence_price_mfi_14_score": 0.0,
                "divergence_price_cmf_20_score": 0.0,
                "divergence_price_oi_change_1_score": 0.0,
            }
            for index in range(48)
        ]
    elif timeframe == "1h":
        rows = [
            {
                "ts": f"2026-03-16T{9 + index:02d}:00:00Z",
                "timeframe": "1h",
                "close": 100.0 + offset + index,
                "ema_20": 101.0 + offset + index,
                "ema_50": 99.0 + offset + index,
                "adx_14": 32.0,
                "rsi_14": 58.0,
                "close_change_1": 0.2,
                "close_slope_20": 0.002,
                "sma_20_slope_5": 0.002,
                "ema_20_slope_5": 0.002,
                "roc_10_change_1": 0.002,
                "mom_10_change_1": 0.002,
                "cross_close_sma_20_code": 1,
                "cross_close_ema_20_code": 1,
                "macd_signal_cross_code": 1,
                "ppo_signal_cross_code": 1,
                "trix_signal_cross_code": 1,
                "kst_signal_cross_code": 1,
                "distance_to_ema_20_atr": -0.1,
                "distance_to_ema_50_atr": -0.2,
            }
            for index in range(12)
        ]
    elif timeframe == "4h":
        rows = [
            {
                "ts": f"2026-03-16T{9 + (index * 4):02d}:00:00Z",
                "timeframe": "4h",
                "close": 100.0 + offset + index,
                "ema_20": 101.0 + offset + index,
                "ema_50": 99.0 + offset + index,
                "macd_hist_12_26_9": 0.2,
                "adx_14": 35.0,
                "rsi_14": 62.0,
            }
            for index in range(3)
        ]
    elif timeframe == "1d":
        rows = [
            {
                "ts": "2026-03-16T09:00:00Z",
                "timeframe": "1d",
                "close": 100.0 + offset,
                "ema_20": 101.0 + offset,
                "ema_50": 99.0 + offset,
                "adx_14": 30.0,
                "rsi_14": 62.0,
            },
            {
                "ts": "2026-03-17T09:00:00Z",
                "timeframe": "1d",
                "close": 101.0 + offset,
                "ema_20": 102.0 + offset,
                "ema_50": 100.0 + offset,
                "adx_14": 31.0,
                "rsi_14": 63.0,
            },
        ]
    else:
        raise ValueError(timeframe)
    return ResearchSeriesFrame(
        contract_id=f"{instrument_id}-6.26",
        instrument_id=instrument_id,
        timeframe=timeframe,
        frame=_frame(rows),
    )


def _trend_search_spec(max_parameter_combinations: int = 1_000) -> StrategyFamilySearchSpec:
    return StrategyFamilySearchSpec(
        search_spec_version="vbt-family-search-v1",
        family_key="trend_movement_cross_v1",
        template_key="trend_movement_cross",
        strategy_version_label="trend-movement-cross-v1",
        intent="Parametric trend continuation surface test.",
        allowed_clock_profiles=("short_swing_1h_v1",),
        allowed_market_states=("trend_up", "trend_down"),
        required_price_inputs=("close",),
        required_materialized_indicators=("ema_20", "ema_50", "macd_hist_12_26_9", "adx_14", "atr_14", "rsi_14"),
        required_materialized_derived=(
            "close_change_1",
            "close_slope_20",
            "ema_20_slope_5",
            "roc_10_change_1",
            "mom_10_change_1",
            "cross_close_ema_20_code",
            "macd_signal_cross_code",
            "ppo_signal_cross_code",
            "distance_to_ema_20_atr",
            "mtf_1d_to_4h_adx_14",
            "mtf_1d_to_4h_ema_20",
            "mtf_1d_to_4h_ema_50",
        ),
        signal_surface_key="vbt_surface.trend_movement_cross_v1",
        signal_surface_mode="indicator_factory",
        parameter_mode="product",
        parameter_space={},
        max_parameter_combinations=max_parameter_combinations,
    )


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

    assert fast_target["tp_stop"].tolist() != slow_target["tp_stop"].tolist()
    assert fast_target["tp_stop"].iloc[0] < slow_target["tp_stop"].iloc[0]


def test_trend_surface_runs_1000_param_rows_as_one_vectorbt_surface() -> None:
    series_frames = (
        _trend_surface_series("BR", 0.0),
        _trend_surface_series("RI", 2.0),
        _trend_surface_series("Si", -2.0),
    )
    bundle = build_input_bundle(
        series_frames,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        clock_profile="short_swing_1h_v1",
    )
    param_rows = [
        {
            "adx_min": adx,
            "close_slope_min": slope,
            "ema_slope_min": slope,
            "roc_change_min": roc,
            "mom_change_min": roc,
            "rsi_min_long": 50,
            "rsi_max_short": 50,
            "require_cross_code": False,
            "stop_atr_mult": 1.5,
            "trail_atr_mult": 2.0,
            "max_holding_bars": 24,
        }
        for adx in range(10, 30, 2)
        for slope in (0.0, 0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.0006, 0.0007, 0.0008, 0.0009)
        for roc in (0.0, 0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.0006, 0.0007, 0.0008, 0.0009)
    ]

    surface = build_signal_surface(
        bundle=bundle,
        spec=_trend_search_spec(),
        param_rows=param_rows,
        search_run_id="VBTSEARCH-UNIT",
        config=BacktestEngineConfig(signal_shift_bars=1),
    )
    portfolio = run_surface_portfolio(bundle=bundle, surface=surface, config=BacktestEngineConfig(signal_shift_bars=1))

    assert len(param_rows) == 1_000
    assert surface.diagnostics["surface_engine"] == "vectorbt.SignalFactory.from_choice_func"
    assert surface.diagnostics["input_resolver"] == "mtf_input_resolver"
    assert surface.diagnostics["portfolio_engine"] == "vectorbt.Portfolio.from_signals"
    assert surface.columns.names == ["family_key", "surface_key", "template_key", "param_hash", "instrument_id"]
    assert surface.entries.shape == (48, 3_000)
    assert portfolio.wrapper.shape == (48, 3_000)
    assert surface.entries.iloc[0].sum() == 0
    assert len(surface.parameter_index) == 1_000


def test_trend_search_spec_declares_mtf_input_contract_without_mtf_indicator_inputs() -> None:
    strategy = build_strategy_registry().get("trend-movement-cross-v1")
    spec = strategy_spec_to_search_spec(strategy, template_key="trend_movement_cross")

    assert spec.clock_profile["regime_tf"] == "1d"
    assert spec.clock_profile["signal_tf"] == "4h"
    assert spec.clock_profile["trigger_tf"] == "1h"
    assert spec.clock_profile["execution_tf"] == "15m"
    assert spec.required_inputs_by_clock["regime"]["timeframe"] == "1d"
    assert set(spec.required_inputs_by_clock["regime"]["materialized_indicators"]) >= {"adx_14", "ema_20", "ema_50"}
    assert spec.required_inputs_by_clock["signal"]["timeframe"] == "4h"
    assert spec.required_inputs_by_clock["trigger"]["timeframe"] == "1h"
    assert spec.required_inputs_by_clock["execution"]["timeframe"] == "15m"
    assert not any(column.startswith("mtf_") for column in spec.required_materialized_derived)
    assert spec.parameter_space_by_role["decision"]["adx_min"] == (18, 22, 28)
    assert spec.parameter_space_by_role["trigger"]["close_slope_min"] == (0.0, 0.0005, 0.001)
    assert spec.parameter_space_by_role["trigger"]["require_cross_code"] == (False, True)
    assert spec.parameter_space_by_role["risk"]["stop_atr_mult"] == (1.5, 2.0, 2.5)


def test_trend_surface_uses_mtf_input_resolver_then_aligns_events_to_execution() -> None:
    spec = strategy_spec_to_search_spec(
        build_strategy_registry().get("trend-movement-cross-v1"),
        template_key="trend_movement_cross",
    )
    series_frames = tuple(
        _native_clock_trend_series(instrument_id, timeframe, offset)
        for instrument_id, offset in (("BR", 0.0), ("RI", 2.0))
        for timeframe in ("15m", "1h", "4h", "1d")
    )
    bundle = build_input_bundle(
        series_frames,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        clock_profile=spec.clock_profile,
        execution_timeframe="15m",
    )
    param_rows = (
        {
            "adx_min": 18,
            "close_slope_min": 0.0,
            "ema_slope_min": 0.0,
            "roc_change_min": 0.0,
            "mom_change_min": 0.0,
            "rsi_min_long": 50,
            "rsi_max_short": 50,
            "require_cross_code": False,
            "stop_atr_mult": 1.5,
            "trail_atr_mult": 2.0,
            "max_holding_bars": 24,
        },
        {
            "adx_min": 28,
            "close_slope_min": 0.001,
            "ema_slope_min": 0.001,
            "roc_change_min": 0.001,
            "mom_change_min": 0.001,
            "rsi_min_long": 55,
            "rsi_max_short": 45,
            "require_cross_code": True,
            "stop_atr_mult": 2.5,
            "trail_atr_mult": 3.0,
            "max_holding_bars": 48,
        },
    )
    surface = build_signal_surface(
        bundle=bundle,
        spec=spec,
        param_rows=param_rows,
        search_run_id="VBTSEARCH-NATIVE-CLOCK",
        config=BacktestEngineConfig(),
    )
    portfolio = run_surface_portfolio(bundle=bundle, surface=surface, config=BacktestEngineConfig())

    assert bundle.field_at("ema_20", "4h").shape[0] == 3
    assert bundle.index.shape[0] == 48
    assert surface.diagnostics["surface_engine"] == "vectorbt.SignalFactory.from_choice_func"
    assert surface.diagnostics["state_builder"] == "ta3000.mtf_state_builder"
    assert surface.diagnostics["role_timeframes"]["regime"] == "1d"
    assert "regime__1d__ema_20" in surface.diagnostics["input_names"]
    assert surface.entries.shape == (48, 4)
    assert portfolio.wrapper.shape == (48, 4)
    assert surface.entries.iloc[0].sum() == 0
    assert not any("mtf_" in column for layer in surface.indicator_plan.inputs_by_clock.values() for column in layer.get("materialized_derived", ()))


def test_mtf_pullback_surface_uses_native_signal_adx_index() -> None:
    spec = strategy_spec_to_search_spec(
        build_strategy_registry().get("trend-mtf-pullback-v1"),
        template_key="trend_mtf_pullback",
    )
    series_frames = tuple(
        _native_clock_trend_series(instrument_id, timeframe, offset)
        for instrument_id, offset in (("BR", 0.0), ("RI", 2.0))
        for timeframe in ("15m", "1h", "4h", "1d")
    )
    bundle = build_input_bundle(
        series_frames,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        clock_profile=spec.clock_profile,
        execution_timeframe="15m",
    )
    param_rows = (
        {
            "adx_min": 18,
            "slope_min": 0.0,
            "pullback_atr_min": 0.0,
            "pullback_atr_max": 1.2,
            "rsi_reclaim_long": 50,
            "rsi_reclaim_short": 50,
            "stop_atr_mult": 1.5,
            "trail_atr_mult": 2.0,
            "max_holding_bars": 24,
        },
    )

    surface = build_signal_surface(
        bundle=bundle,
        spec=spec,
        param_rows=param_rows,
        search_run_id="VBTSEARCH-MTF-PULLBACK",
        config=BacktestEngineConfig(),
    )
    portfolio = run_surface_portfolio(bundle=bundle, surface=surface, config=BacktestEngineConfig())

    assert surface.diagnostics["surface_engine"] == "vectorbt.SignalFactory.from_choice_func"
    assert surface.diagnostics["state_builder"] == "ta3000.mtf_state_builder"
    assert surface.diagnostics["input_resolver"] == "mtf_input_resolver"
    assert surface.entries.shape == (48, 2)
    assert portfolio.wrapper.shape == (48, 2)


def test_missing_mtf_input_fails_at_indicator_plan_gate_without_fallback() -> None:
    frame = _trend_surface_series("BR").frame.drop(columns=["mtf_1d_to_4h_adx_14"])
    series = ResearchSeriesFrame(contract_id="BR-6.26", instrument_id="BR", timeframe="15m", frame=frame)

    report = run_vectorbt_family_search(
        series_frames=(series,),
        search_spec=_trend_search_spec(max_parameter_combinations=1),
        config=BacktestEngineConfig(),
        backtest_batch_id="BTBATCH-UNIT",
        campaign_run_id="CRUN-UNIT",
        strategy_space_id="SSPACE-UNIT",
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        split_windows=None,
        param_batch_size=1,
    )

    assert report["param_result_rows"] == []
    assert report["search_run_rows"][0]["status"] == "failed"
    assert report["gate_rows"][0]["gate_name"] == "indicator_plan_valid"
    assert report["gate_rows"][0]["failure_code"] == "MISSING_MTF_INPUT"


def test_optuna_family_search_records_delta_first_optimizer_provenance() -> None:
    base_spec = _trend_search_spec(max_parameter_combinations=16)
    spec = StrategyFamilySearchSpec(
        **{
            **base_spec.to_dict(),
            "parameter_space": {
                "adx_min": [18.0, 22.0, 28.0],
                "rsi_min_long": [50.0, 55.0],
                "require_cross_code": [False, True],
            },
        }
    )

    report = run_vectorbt_family_search(
        series_frames=(_trend_surface_series("BR"),),
        search_spec=spec,
        config=BacktestEngineConfig(window_count=2),
        backtest_batch_id="BTBATCH-OPTUNA",
        campaign_run_id="CRUN-OPTUNA",
        strategy_space_id="SSPACE-OPTUNA",
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        split_windows=None,
        param_batch_size=2,
        optimizer_policy={
            "engine": "optuna",
            "sampler": "tpe",
            "seed": 7,
            "objective": "robust_oos_trial_v1",
            "direction": "maximize",
            "n_trials": 4,
            "top_k": 2,
            "radius": 1,
            "max_neighborhood_trials": 3,
        },
    )

    assert len(report["optimizer_study_rows"]) == 1
    assert report["optimizer_study_rows"][0]["optimizer_engine"] == "optuna"
    assert report["optimizer_study_rows"][0]["objective_name"] == "robust_oos_trial_v1"
    assert report["optimizer_study_rows"][0]["study_config_json"]["selection_owner"] == "optuna.study"
    assert report["optimizer_study_rows"][0]["study_config_json"]["constraints_func"] == "ta3000.robust_oos_trial_constraints"
    assert report["optimizer_study_rows"][0]["study_config_json"]["ask_tell_batch_count"] == 2
    assert report["optimizer_study_rows"][0]["study_config_json"]["ask_tell_batch_size"] == 2
    diagnostics = report["optimizer_study_rows"][0]["study_config_json"]["parameter_space_diagnostics"]
    assert diagnostics["choice_counts"]["adx_min"] == 3
    assert diagnostics["observed_unique_value_counts"]["adx_min"] >= 1
    assert {row["trial_kind"] for row in report["optimizer_trial_rows"]} >= {"optuna_trial"}
    assert "neighborhood_probe" not in {row["trial_kind"] for row in report["optimizer_trial_rows"]}
    assert all(row["param_hash"] for row in report["optimizer_trial_rows"])
    assert all(row["objective_components_json"]["signal_generator"] == "vectorbt.SignalFactory.from_choice_func" for row in report["optimizer_trial_rows"])
    assert all(row["objective_components_json"]["input_resolver"] == "mtf_input_resolver" for row in report["optimizer_trial_rows"])
    assert all(row["objective_components_json"]["selection_owner"] == "optuna.study" for row in report["optimizer_trial_rows"])
    assert all("constraint_values" in row["objective_components_json"] for row in report["optimizer_trial_rows"])
    assert all("net_pnl_total" in row["objective_components_json"] for row in report["optimizer_trial_rows"])
    assert all("profit_factor_mean" in row["objective_components_json"] for row in report["optimizer_trial_rows"])
    assert all("total_return_mean" in row["objective_components_json"] for row in report["optimizer_trial_rows"])
    assert all("policy_metric_vector" in row["objective_components_json"] for row in report["optimizer_trial_rows"])
    assert len({row["param_hash"] for row in report["param_result_rows"]}) <= 4
    assert report["search_run_rows"]


def test_optuna_family_search_does_not_promote_infeasible_trials_to_best() -> None:
    base_spec = _trend_search_spec(max_parameter_combinations=16)
    spec = StrategyFamilySearchSpec(
        **{
            **base_spec.to_dict(),
            "parameter_space": {
                "adx_min": [22.0, 28.0],
                "rsi_min_long": [50.0, 55.0],
                "require_cross_code": [False],
            },
        }
    )

    report = run_vectorbt_family_search(
        series_frames=(_trend_surface_series("BR"),),
        search_spec=spec,
        config=BacktestEngineConfig(window_count=1),
        backtest_batch_id="BTBATCH-OPTUNA-INFEASIBLE",
        campaign_run_id="CRUN-OPTUNA-INFEASIBLE",
        strategy_space_id="SSPACE-OPTUNA-INFEASIBLE",
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        split_windows=None,
        param_batch_size=2,
        optimizer_policy={
            "engine": "optuna",
            "sampler": "tpe",
            "seed": 7,
            "objective": "robust_oos_trial_v1",
            "direction": "maximize",
            "n_trials": 2,
            "min_slippage_score": 1.1,
        },
    )

    study_row = report["optimizer_study_rows"][0]
    completed_trials = [
        row
        for row in report["optimizer_trial_rows"]
        if row["status"] in {"completed", "duplicate"}
    ]
    assert completed_trials
    assert study_row["status"] == "no_feasible_trials"
    assert study_row["best_trial_number"] == -1
    assert all(not row["constraints_passed"] for row in completed_trials)
    assert all(row["objective_components_json"]["constraint_values"][-1] > 0.0 for row in completed_trials)


def test_optuna_family_search_deduplicates_same_batch_trials_before_signal_factory() -> None:
    base_spec = _trend_search_spec(max_parameter_combinations=16)
    spec = StrategyFamilySearchSpec(
        **{
            **base_spec.to_dict(),
            "parameter_space": {
                "adx_min": [22.0],
                "rsi_min_long": [55.0],
                "require_cross_code": [False],
            },
        }
    )

    report = run_vectorbt_family_search(
        series_frames=(_trend_surface_series("BR"),),
        search_spec=spec,
        config=BacktestEngineConfig(window_count=1),
        backtest_batch_id="BTBATCH-OPTUNA-DEDUP",
        campaign_run_id="CRUN-OPTUNA-DEDUP",
        strategy_space_id="SSPACE-OPTUNA-DEDUP",
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        split_windows=None,
        param_batch_size=4,
        optimizer_policy={
            "engine": "optuna",
            "sampler": "tpe",
            "seed": 7,
            "objective": "robust_oos_trial_v1",
            "direction": "maximize",
            "n_trials": 4,
            "top_k": 2,
            "radius": 0,
            "max_neighborhood_trials": 0,
        },
    )

    statuses = Counter(row["status"] for row in report["optimizer_trial_rows"])
    assert statuses == {"completed": 1, "duplicate": 3}
    assert {row["param_hash"] for row in report["optimizer_trial_rows"]} == {
        report["param_result_rows"][0]["param_hash"]
    }
    assert len(report["param_result_rows"]) == 1
    assert report["search_run_rows"][0]["param_count"] == 1


def test_missing_native_clock_input_fails_without_mtf_fallback() -> None:
    spec = strategy_spec_to_search_spec(
        build_strategy_registry().get("trend-movement-cross-v1"),
        template_key="trend_movement_cross",
    )
    series_frames = tuple(
        _native_clock_trend_series("BR", timeframe)
        for timeframe in ("15m", "4h", "1d")
    )

    report = run_vectorbt_family_search(
        series_frames=series_frames,
        search_spec=spec,
        config=BacktestEngineConfig(),
        backtest_batch_id="BTBATCH-UNIT",
        campaign_run_id="CRUN-UNIT",
        strategy_space_id="SSPACE-UNIT",
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        split_windows=None,
        param_batch_size=1,
    )

    assert report["param_result_rows"] == []
    assert report["search_run_rows"][0]["status"] == "failed"
    assert report["gate_rows"][0]["gate_name"] == "indicator_plan_valid"
    assert report["gate_rows"][0]["failure_code"] == "MISSING_CLOCK_INPUT"
    assert "trigger:1h" in report["gate_rows"][0]["failure_reason"]
