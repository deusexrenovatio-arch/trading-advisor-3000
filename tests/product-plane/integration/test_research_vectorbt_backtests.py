from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from trading_advisor_3000.product_plane.contracts import DecisionCandidate
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.research.backtests.batch_runner import _chunked_series_for_spec
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    CandidateProjectionRequest,
    RankingPolicy,
    StrategyFamilySearchSpec,
    build_ephemeral_strategy_space,
    project_runtime_candidates,
    rank_backtest_results,
    run_backtest_batch,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView, ResearchDatasetManifest, research_dataset_store_contract
from trading_advisor_3000.product_plane.research.derived_indicators import (
    DerivedIndicatorFrameRow,
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import IndicatorFrameRow, indicator_store_contract
from trading_advisor_3000.product_plane.research.io import ResearchFrameCache, ResearchSeriesFrame
from trading_advisor_3000.product_plane.research.strategies.catalog import StrategyCatalog
from trading_advisor_3000.product_plane.research.strategies.registry import StrategyRegistry, build_strategy_registry
from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def _ts(index: int) -> str:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    return (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")


def _tf_ts(index: int, timeframe: str) -> str:
    minutes = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}[timeframe]
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    return (start + timedelta(minutes=minutes * index)).isoformat().replace("+00:00", "Z")


def _bar(index: int, close: float) -> ResearchBarView:
    ts = _ts(index)
    return ResearchBarView(
        dataset_version="dataset-v5",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe="15m",
        ts=ts,
        open=close - 0.4,
        high=close + 0.8,
        low=close - 0.9,
        close=close,
        volume=1000 + index * 20,
        open_interest=20000 + index,
        session_date="2026-03-16",
        session_open_ts="2026-03-16T09:00:00Z",
        session_close_ts="2026-03-16T23:45:00Z",
        active_contract_id="BR-6.26",
        ret_1=None if index == 0 else 0.01,
        log_ret_1=None if index == 0 else 0.00995,
        true_range=1.7,
        hl_range=1.7,
        oc_range=0.4,
        bar_index=index,
        slice_role="analysis",
    )


def _indicator(index: int, close: float, ema10: float, ema20: float, ema50: float) -> IndicatorFrameRow:
    ts = _ts(index)
    return IndicatorFrameRow(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe="15m",
        ts=ts,
        values={
            "ema_10": ema10,
            "ema_20": ema20,
            "ema_50": ema50,
            "atr_14": 1.4,
            "rsi_14": 60.0 if index < 5 else 38.0 if index < 9 else 58.0,
            "stoch_k_14_3_3": 25.0 if index < 5 else 75.0,
            "stoch_d_14_3_3": 30.0 if index < 5 else 70.0,
            "cci_20": -110.0 if index < 5 else 110.0,
            "willr_14": -85.0 if index < 5 else -15.0,
            "adx_14": 28.0,
            "chop_14": 58.0,
            "macd_hist_12_26_9": 0.2 if ema20 >= ema50 else -0.2,
            "ppo_hist_12_26_9": 0.2 if ema20 >= ema50 else -0.2,
            "tsi_25_13": 15.0 if ema20 >= ema50 else -15.0,
            "donchian_high_20": close + 0.8,
            "donchian_low_20": close - 0.8,
            "donchian_high_55": close + 0.9,
            "donchian_low_55": close - 0.9,
            "bb_upper_20_2": close + 1.0,
            "bb_mid_20_2": close,
            "bb_lower_20_2": close - 1.0,
            "bb_width_20_2": 0.15,
            "bb_percent_b_20_2": 0.5,
            "kc_upper_20_1_5": close + 1.2,
            "kc_mid_20_1_5": close,
            "kc_lower_20_1_5": close - 1.2,
            "natr_14": 1.4,
            "obv": 1000.0 + index * 10,
            "mfi_14": 45.0 + index,
            "cmf_20": 0.1,
            "rvol_20": 1.3,
            "volume_z_20": 0.5,
            "oi_change_1": 0.01,
            "oi_roc_10": 0.02,
            "oi_z_20": 0.4,
            "oi_relative_activity_20": 1.1,
            "volume_oi_ratio": 0.05,
        },
        source_bars_hash="SRC-BARS",
        row_count=12,
        warmup_span=20,
        null_warmup_span=0,
        created_at="2026-03-16T12:00:00Z",
    )


def _derived(index: int, close: float) -> DerivedIndicatorFrameRow:
    mtf_up = index <= 4 or index >= 10
    channel_mid = index in {0, 1, 2, 5, 6, 7}
    release_up = index in {3, 10}
    release_down = index in {8}
    return DerivedIndicatorFrameRow(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        profile_version="core_v1",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe="15m",
        ts=_ts(index),
        values={
            "rolling_high_20": close + 0.8,
            "rolling_low_20": close - 0.8,
            "opening_range_high": 103.5,
            "opening_range_low": 99.2,
            "swing_high_10": 104.0,
            "swing_low_10": 97.0,
            "session_vwap": 100.5,
            "distance_to_session_vwap": -0.8 if index in {6, 7, 8} else 0.6 if index in {2, 3, 4} else 0.1,
            "distance_to_rolling_high_20": -0.2,
            "distance_to_rolling_low_20": 0.2,
            "distance_to_sma_20_atr": -0.1 if mtf_up else 0.1,
            "distance_to_sma_50_atr": -0.2 if mtf_up else 0.2,
            "distance_to_ema_50_atr": -0.2 if mtf_up else 0.2,
            "bb_position_20_2": 0.5 if channel_mid else 0.9 if release_up else 0.1 if release_down else 0.35,
            "kc_position_20_1_5": 0.5 if channel_mid else 0.8 if release_up else 0.2 if release_down else 0.35,
            "rolling_position_20": 0.5 if channel_mid else 0.9 if release_up else 0.1 if release_down else 0.35,
            "session_position": 0.5 if channel_mid else 0.85 if release_up else 0.15 if release_down else 0.35,
            "week_position": 0.5 if channel_mid else 0.85 if release_up else 0.15 if release_down else 0.35,
            "cross_close_sma_20_code": 1 if release_up else -1 if release_down else 0,
            "cross_close_session_vwap_code": 1 if release_up else -1 if release_down else 0,
            "cross_close_rolling_high_20_code": 1 if release_up else 0,
            "cross_close_rolling_low_20_code": -1 if release_down else 0,
            "close_change_1": 0.2 if mtf_up else -0.2,
            "close_slope_20": 0.002 if mtf_up else -0.002,
            "sma_20_slope_5": 0.002 if mtf_up else -0.002,
            "ema_20_slope_5": 0.002 if mtf_up else -0.002,
            "roc_10_change_1": 0.002 if mtf_up else -0.002,
            "mom_10_change_1": 0.002 if mtf_up else -0.002,
            "cross_close_ema_20_code": 1 if release_up else -1 if release_down else 0,
            "macd_signal_cross_code": 1 if release_up else -1 if release_down else 0,
            "ppo_signal_cross_code": 1 if release_up else -1 if release_down else 0,
            "trix_signal_cross_code": 1 if release_up else -1 if release_down else 0,
            "kst_signal_cross_code": 1 if release_up else -1 if release_down else 0,
            "distance_to_ema_20_atr": -0.1 if mtf_up else 0.1,
            "distance_to_donchian_high_20_atr": -0.2 if release_up else 0.8,
            "distance_to_donchian_low_20_atr": 0.2 if release_down else -0.8,
            "distance_to_donchian_high_55_atr": -0.2 if release_up else 0.9,
            "distance_to_donchian_low_55_atr": 0.2 if release_down else -0.9,
            "distance_to_bb_upper_20_2_atr": -0.2 if release_up else 0.8,
            "distance_to_bb_lower_20_2_atr": 0.2 if release_down else -0.8,
            "distance_to_kc_upper_20_1_5_atr": -0.2 if release_up else 0.8,
            "distance_to_kc_lower_20_1_5_atr": 0.2 if release_down else -0.8,
            "donchian_position_20": 0.9 if release_up else 0.1 if release_down else 0.5,
            "donchian_position_55": 0.9 if release_up else 0.1 if release_down else 0.5,
            "volume_change_1": 0.1,
            "oi_change_1": 0.01,
            "rvol_20": 1.3,
            "volume_zscore_20": 0.5,
            "price_volume_corr_20": 0.2,
            "price_oi_corr_20": 0.1,
            "volume_oi_corr_20": 0.1,
            "divergence_price_rsi_14_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_stoch_k_14_3_3_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_cci_20_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_willr_14_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_macd_hist_12_26_9_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_ppo_hist_12_26_9_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_tsi_25_13_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_mfi_14_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_cmf_20_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_obv_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "divergence_price_oi_change_1_score": 0.6 if release_down else -0.6 if release_up else 0.0,
            "mtf_4h_to_15m_ema_20": close + 1.2 if mtf_up else close - 1.2,
            "mtf_4h_to_15m_ema_50": close - 1.2 if mtf_up else close + 1.2,
            "mtf_4h_to_15m_adx_14": 30.0,
            "mtf_4h_to_15m_rsi_14": 62.0 if mtf_up else 38.0,
            "mtf_1d_to_4h_ema_20": close + 1.5 if mtf_up else close - 1.5,
            "mtf_1d_to_4h_ema_50": close - 1.5 if mtf_up else close + 1.5,
            "mtf_1d_to_4h_adx_14": 30.0,
        },
        source_bars_hash="SRC-BARS",
        source_indicators_hash="SRC-IND",
        row_count=12,
        warmup_span=20,
        null_warmup_span=0,
        created_at="2026-03-16T12:00:00Z",
    )


def _write_materialized_layers(
    root: Path,
    *,
    split_method: str = "full",
    split_windows: list[dict[str, object]] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    bars = [
        _bar(index, close)
        for index, close in enumerate((100.0, 101.0, 102.0, 103.0, 104.0, 103.0, 101.0, 99.0, 97.0, 98.0, 100.0, 102.0))
    ]
    bars.extend(
        replace(_bar(index, 100.0 + index), timeframe="1h", ts=_tf_ts(index, "1h"), bar_index=index)
        for index in range(12)
    )
    bars.extend(
        replace(_bar(index, 101.0 + index), timeframe="4h", ts=_tf_ts(index, "4h"), bar_index=index)
        for index in range(3)
    )
    bars.extend(
        replace(_bar(index, 102.0 + index), timeframe="1d", ts=_tf_ts(index, "1d"), bar_index=index)
        for index in range(2)
    )
    indicators = [
        _indicator(index, close, ema10, ema20, ema50)
        for index, (close, ema10, ema20, ema50) in enumerate(
            (
                (100.0, 99.5, 99.0, 98.5),
                (101.0, 100.0, 99.4, 98.7),
                (102.0, 100.8, 100.1, 99.0),
                (103.0, 101.7, 100.9, 99.5),
                (104.0, 102.5, 101.7, 100.1),
                (103.0, 102.3, 101.8, 100.5),
                (101.0, 101.4, 101.5, 100.8),
                (99.0, 100.0, 100.7, 100.9),
                (97.0, 98.6, 99.7, 100.7),
                (98.0, 98.4, 99.2, 100.2),
                (100.0, 99.1, 99.0, 99.8),
                (102.0, 100.3, 99.7, 99.7),
            )
        )
    ]
    indicators.extend(
        replace(
            _indicator(index, 100.0 + index, 100.5 + index, 101.0 + index, 99.0 + index),
            timeframe="1h",
            ts=_tf_ts(index, "1h"),
            row_count=12,
        )
        for index in range(12)
    )
    indicators.extend(
        replace(
            _indicator(index, 101.0 + index, 101.5 + index, 102.0 + index, 100.0 + index),
            timeframe="4h",
            ts=_tf_ts(index, "4h"),
            row_count=3,
        )
        for index in range(3)
    )
    indicators.extend(
        replace(
            _indicator(index, 102.0 + index, 102.5 + index, 103.0 + index, 101.0 + index),
            timeframe="1d",
            ts=_tf_ts(index, "1d"),
            row_count=2,
        )
        for index in range(2)
    )
    derived = [_derived(index, bar.close) for index, bar in enumerate(bars) if bar.timeframe == "15m"]
    derived.extend(
        replace(_derived(index, 100.0 + index), timeframe="1h", ts=_tf_ts(index, "1h"), row_count=12)
        for index in range(12)
    )
    derived.extend(
        replace(_derived(index, 101.0 + index), timeframe="4h", ts=_tf_ts(index, "4h"), row_count=3)
        for index in range(3)
    )

    dataset_manifest = ResearchDatasetManifest(
        dataset_version="dataset-v5",
        dataset_name="stage5 synthetic",
        universe_id="moex-futures",
        timeframes=("15m", "1h", "4h", "1d"),
        base_timeframe="15m",
        split_method=split_method,
        warmup_bars=0,
        bars_hash="SRC-BARS",
        created_at="2026-03-16T12:00:00Z",
        code_version="test",
        split_params={"windows": split_windows or []},
    )
    dataset_contract = research_dataset_store_contract()
    indicator_contract = indicator_store_contract()
    derived_contract = research_derived_indicator_store_contract()
    write_delta_table_rows(
        table_path=root / "research_datasets.delta",
        rows=[dataset_manifest.to_dict()],
        columns=dataset_contract["research_datasets"]["columns"],
    )
    write_delta_table_rows(
        table_path=root / "research_bar_views.delta",
        rows=[row.to_dict() for row in bars],
        columns=dataset_contract["research_bar_views"]["columns"],
    )
    write_delta_table_rows(
        table_path=root / "research_indicator_frames.delta",
        rows=[row.to_dict() for row in indicators],
        columns=indicator_contract["research_indicator_frames"]["columns"],
    )
    write_delta_table_rows(
        table_path=root / "research_derived_indicator_frames.delta",
        rows=[row.to_dict() for row in derived],
        columns=derived_contract["research_derived_indicator_frames"]["columns"],
    )
    return root


def _custom_registry(*specs: StrategySpec) -> StrategyRegistry:
    return StrategyRegistry(catalog=StrategyCatalog(version="stage5-test-catalog", strategies=tuple(specs)))


def test_vectorbt_batch_runner_chunks_mtf_contract_series_by_execution_contract() -> None:
    spec = StrategyFamilySearchSpec(
        search_spec_version="search-v1",
        family_key="mtf_contract_chunk",
        template_key="mtf_contract_chunk",
        strategy_version_label="mtf-contract-chunk-v1",
        intent="Keep MTF frames for the same contract together.",
        allowed_clock_profiles=("short_swing_1h_v1",),
        allowed_market_states=(),
        required_price_inputs=("close",),
        required_materialized_indicators=("ema_20",),
        required_materialized_derived=(),
        signal_surface_key="ma_cross",
        signal_surface_mode="long_short",
        parameter_mode="table",
        parameter_space={"rows": ({"fast_window": 10, "slow_window": 20},)},
        clock_profile={"execution_tf": "15m"},
        required_inputs_by_clock={
            "entry": {"timeframe": "15m", "price_inputs": ["close"]},
            "signal": {"timeframe": "1h", "materialized_indicators": ["ema_20"]},
        },
    )
    frames = [
        ResearchSeriesFrame(contract_id=contract_id, instrument_id="FUT_BR", timeframe=timeframe, frame=pd.DataFrame())
        for contract_id in ("BRF6@MOEX", "BRG6@MOEX")
        for timeframe in ("15m", "1h")
    ]

    chunks = _chunked_series_for_spec(frames, 1, spec, "15m")

    assert len(chunks) == 2
    assert [
        {(series.contract_id, series.timeframe) for series in chunk}
        for chunk in chunks
    ] == [
        {("BRF6@MOEX", "15m"), ("BRF6@MOEX", "1h")},
        {("BRG6@MOEX", "15m"), ("BRG6@MOEX", "1h")},
    ]


def _backtest_request(
    *,
    strategy_labels: tuple[str, ...],
    combinations_per_strategy: int,
    registry: StrategyRegistry | None = None,
    dataset_version: str = "dataset-v5",
    indicator_set_version: str = "indicators-v1",
    derived_indicator_set_version: str = "derived-v1",
    timeframe: str = "15m",
    param_batch_size: int = 25,
    series_batch_size: int = 4,
) -> BacktestBatchRequest:
    resolved_registry = registry or build_strategy_registry()
    strategy_space = build_ephemeral_strategy_space(
        strategy_registry=resolved_registry,
        strategy_version_labels=strategy_labels,
        instances_per_strategy=combinations_per_strategy,
    )
    return BacktestBatchRequest(
        campaign_run_id="crun_stage5_test",
        strategy_space_id=strategy_space.strategy_space_id,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        search_specs=strategy_space.search_specs,
        combination_count=sum(len(spec.parameter_space.get("rows", ())) or 1 for spec in strategy_space.search_specs),
        param_batch_size=param_batch_size,
        series_batch_size=series_batch_size,
        timeframe=timeframe,
    )


def test_vectorbt_batch_runner_materializes_signal_and_order_func_artifacts(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized")
    output_dir = tmp_path / "backtests"
    report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=output_dir,
        request=_backtest_request(
            strategy_labels=("ma-cross-v1", "volatility-squeeze-release-v1"),
            combinations_per_strategy=2,
            param_batch_size=1,
            series_batch_size=1,
        ),
        engine_config=BacktestEngineConfig(
            fees_bps=5.0,
            slippage_bps=2.0,
            allow_short=True,
            window_count=2,
        ),
    )

    assert report["cache_hit"] is False
    assert report["backtest_batch"]["combination_count"] == 4
    run_rows = read_delta_table_rows(Path(str(report["output_paths"]["research_backtest_runs"])))
    stats_rows = read_delta_table_rows(Path(str(report["output_paths"]["research_strategy_stats"])))
    trade_rows = read_delta_table_rows(Path(str(report["output_paths"]["research_trade_records"])))
    order_rows = read_delta_table_rows(Path(str(report["output_paths"]["research_order_records"])))
    drawdown_rows = read_delta_table_rows(Path(str(report["output_paths"]["research_drawdown_records"])))

    assert run_rows
    assert stats_rows
    assert trade_rows
    assert order_rows
    assert drawdown_rows
    assert {row["execution_mode"] for row in run_rows} == {"from_signals"}
    assert all(row["trade_count"] >= 0 for row in run_rows)
    assert all("total_return" in row for row in stats_rows)
    assert all(row["side"] in {"long", "short"} for row in trade_rows)
    assert all(row["side"] in {"buy", "sell"} for row in order_rows)
    assert all("drawdown_pct" in row for row in drawdown_rows)
    for path_text in report["output_paths"].values():
        assert (Path(path_text) / "_delta_log").exists()


def test_vectorbt_batch_runner_runs_new_launch_families_on_mtf_input_resolver(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-launch-families")
    output_dir = tmp_path / "backtests-launch-families"
    report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=output_dir,
        request=_backtest_request(
            strategy_labels=("trend-movement-cross-v1", "channel-breakout-continuation-v1"),
            combinations_per_strategy=2,
            param_batch_size=2,
            series_batch_size=1,
            timeframe="15m",
        ),
        engine_config=BacktestEngineConfig(
            fees_bps=5.0,
            slippage_bps=2.0,
            allow_short=True,
            window_count=1,
        ),
    )

    assert report["backtest_batch"]["combination_count"] == 4
    assert {row["family_key"] for row in report["search_spec_rows"]} == {
        "trend_movement_cross_v1",
        "channel_breakout_continuation_v1",
    }
    assert {row["family_key"] for row in report["param_result_rows"]} == {
        "trend_movement_cross_v1",
        "channel_breakout_continuation_v1",
    }
    assert {row["execution_mode"] for row in report["run_rows"]} == {"from_signals"}
    assert all(row["timeframe"] == "15m" for row in report["run_rows"])
    assert len(report["gate_event_rows"]) >= len(report["param_result_rows"])


def test_vectorbt_batch_runner_is_reproducible_and_uses_hot_cache(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-cache")
    cache = ResearchFrameCache()
    request = _backtest_request(
        strategy_labels=("ma-cross-v1",),
        combinations_per_strategy=2,
        param_batch_size=2,
        series_batch_size=1,
    )
    first = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-cache-a",
        request=request,
        engine_config=BacktestEngineConfig(window_count=2),
        cache=cache,
    )
    second = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-cache-b",
        request=request,
        engine_config=BacktestEngineConfig(window_count=2),
        cache=cache,
    )

    assert first["backtest_batch"]["backtest_batch_id"] == second["backtest_batch"]["backtest_batch_id"]
    assert first["cache_id"] == second["cache_id"]
    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert [row["backtest_run_id"] for row in first["run_rows"]] == [row["backtest_run_id"] for row in second["run_rows"]]
    assert len(first["trade_rows"]) == len(second["trade_rows"])


def test_vectorbt_batch_runner_respects_dataset_windows_and_short_only_direction(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(
        tmp_path / "materialized-wf",
        split_method="walk_forward",
        split_windows=[
            {"window_id": "wf-01", "test_start": 0, "test_stop": 6},
            {"window_id": "wf-02", "test_start": 6, "test_stop": 12},
        ],
    )
    short_only_spec = StrategySpec(
        version="ma-cross-short-only-v1",
        family="ma_cross",
        description="Short-only MA cross validation spec.",
        required_columns=("close", "ema_10", "ema_20", "ema_50", "atr_14"),
        parameter_grid=(
            StrategyParameter("fast_window", (10,)),
            StrategyParameter("slow_window", (20,)),
        ),
        signal_builder_key="ma_cross",
        direction_mode="short_only",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(tags=("test",)),
    )

    report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-wf",
        request=_backtest_request(
            strategy_labels=("ma-cross-short-only-v1",),
            combinations_per_strategy=1,
            registry=_custom_registry(short_only_spec),
        ),
        engine_config=BacktestEngineConfig(allow_short=True, window_count=99),
        strategy_registry=_custom_registry(short_only_spec),
    )

    assert {row["window_id"] for row in report["run_rows"]} == {"wf-01", "wf-02"}
    assert report["trade_rows"]
    assert {row["side"] for row in report["trade_rows"]} == {"short"}


def test_vectorbt_batch_runner_parameters_and_risk_policy_change_execution(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-param-effects")
    breakout_spec = StrategySpec(
        version="breakout-param-v1",
        family="breakout",
        description="Breakout parameter effect validation spec.",
        required_columns=("close", "high", "low", "adx_14", "atr_14"),
        parameter_grid=(
            StrategyParameter("breakout_window", (2, 8)),
            StrategyParameter("min_adx", (10,)),
            StrategyParameter("entry_buffer_atr", (0.0,)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=0.8, target_atr_multiple=1.6),
        ranking_metadata=StrategyRankingMetadata(tags=("test",)),
    )

    report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-param-effects",
        request=_backtest_request(
            strategy_labels=("breakout-param-v1",),
            combinations_per_strategy=2,
            registry=_custom_registry(breakout_spec),
        ),
        engine_config=BacktestEngineConfig(allow_short=True),
        strategy_registry=_custom_registry(breakout_spec),
    )

    breakout_runs = [row for row in report["run_rows"] if row["strategy_version_label"] == "breakout-param-v1"]
    breakout_stats = [row for row in report["stat_rows"] if row["strategy_version_label"] == "breakout-param-v1"]
    assert len({str(row["parameter_values_json"]) for row in breakout_runs}) == 2
    assert len({row["trade_count"] for row in breakout_stats}) > 1 or len({row["total_return"] for row in breakout_stats}) > 1


def test_vectorbt_batch_runner_handles_100_combinations_with_batching_and_cache(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-benchmark")
    cache = ResearchFrameCache()
    benchmark_spec = StrategySpec(
        version="breakout-benchmark-v1",
        family="breakout",
        description="Large sweep validation spec.",
        required_columns=("close", "high", "low", "adx_14", "atr_14"),
        parameter_grid=(
            StrategyParameter("breakout_window", (3, 4, 5, 6, 7)),
            StrategyParameter("min_adx", (10, 12, 14, 16, 18, 20, 22, 24, 26, 28)),
            StrategyParameter("entry_buffer_atr", (0.0, 0.25)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(tags=("benchmark",)),
    )
    request = _backtest_request(
        strategy_labels=("breakout-benchmark-v1",),
        combinations_per_strategy=100,
        registry=_custom_registry(benchmark_spec),
        param_batch_size=10,
        series_batch_size=1,
    )

    first = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-benchmark-a",
        request=request,
        engine_config=BacktestEngineConfig(),
        strategy_registry=_custom_registry(benchmark_spec),
        cache=cache,
    )
    second = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-benchmark-b",
        request=request,
        engine_config=BacktestEngineConfig(),
        strategy_registry=_custom_registry(benchmark_spec),
        cache=cache,
    )

    assert first["backtest_batch"]["combination_count"] == 100
    assert first["backtest_batch"]["param_batch_size"] == 10
    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert len(first["run_rows"]) == 100
    assert len(second["run_rows"]) == 100


def test_stage6_ranking_and_projection_build_runtime_compatible_candidates(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-stage6")
    output_dir = tmp_path / "backtests-stage6"
    backtest_report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=output_dir,
        request=_backtest_request(
            strategy_labels=("ma-cross-v1", "breakout-v1"),
            combinations_per_strategy=3,
            param_batch_size=2,
            series_batch_size=1,
        ),
        engine_config=BacktestEngineConfig(window_count=2, fees_bps=5.0, slippage_bps=2.0),
    )
    policy = RankingPolicy(
        policy_id="stage6-selection-v1",
        metric_order=("total_return", "profit_factor", "max_drawdown"),
        min_trade_count=1,
        max_drawdown_cap=0.8,
        min_positive_fold_ratio=0.0,
        min_parameter_stability=0.0,
        min_slippage_score=0.0,
    )

    ranking_report = rank_backtest_results(
        backtest_output_dir=output_dir,
        policy=policy,
    )
    assert ranking_report["ranking_rows"]
    assert any(row["selected_rank"] >= 1 for row in ranking_report["ranking_rows"])
    assert any(row["policy_pass"] == 1 for row in ranking_report["ranking_rows"])

    projection_report = project_runtime_candidates(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=output_dir,
        request=CandidateProjectionRequest(
            ranking_policy_id="stage6-selection-v1",
            selection_policy="top_by_family_per_series",
            max_candidates_per_partition=2,
            min_robust_score=0.0,
            decision_lag_bars_max=12,
        ),
        ranking_rows=ranking_report["ranking_rows"],
    )

    assert projection_report["candidate_rows"]
    ranking_rows = read_delta_table_rows(Path(str(ranking_report["output_paths"]["research_strategy_rankings"])))
    candidate_rows = read_delta_table_rows(Path(str(projection_report["output_paths"]["research_signal_candidates"])))
    assert ranking_rows
    assert candidate_rows
    assert any(row["score_total"] >= 0.0 for row in ranking_rows)
    assert all("indicator_context_json" in row for row in candidate_rows)
    assert all(float(row["score"]) >= 0.0 for row in candidate_rows)
    for payload in projection_report["candidate_contracts"]:
        contract = DecisionCandidate.from_dict(payload)
        assert contract.to_dict() == payload
    assert backtest_report["trade_rows"]
    assert backtest_report["order_rows"]
    assert backtest_report["drawdown_rows"]

