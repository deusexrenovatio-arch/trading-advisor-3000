from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import DecisionCandidate
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    CandidateProjectionRequest,
    RankingPolicy,
    project_runtime_candidates,
    rank_backtest_results,
    run_backtest_batch,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView, ResearchDatasetManifest, phase2_research_dataset_store_contract
from trading_advisor_3000.product_plane.research.features import FeatureFrameRow, phase2b_feature_store_contract
from trading_advisor_3000.product_plane.research.indicators import IndicatorFrameRow, phase3_indicator_store_contract
from trading_advisor_3000.product_plane.research.io import ResearchFrameCache
from trading_advisor_3000.product_plane.research.strategies.catalog import StrategyCatalog
from trading_advisor_3000.product_plane.research.strategies.registry import StrategyRegistry
from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def _ts(index: int) -> str:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    return (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")


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
            "adx_14": 28.0,
            "bb_width_20_2": 0.15,
            "kc_upper_20_1_5": close + 1.2,
            "kc_mid_20_1_5": close,
            "kc_lower_20_1_5": close - 1.2,
            "rvol_20": 1.3,
            "volume_z_20": 0.5,
        },
        source_bars_hash="SRC-BARS",
        row_count=12,
        warmup_span=20,
        null_warmup_span=0,
        created_at="2026-03-16T12:00:00Z",
    )


def _feature(index: int, close: float) -> FeatureFrameRow:
    trend = 1 if index <= 4 else -1 if 5 <= index <= 9 else 1
    breakout = 1 if index in {2, 3, 10} else -1 if index in {7, 8} else 0
    squeeze = 1 if index in {1, 2, 3, 6, 7, 8} else 0
    return FeatureFrameRow(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        profile_version="core_v1",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe="15m",
        ts=_ts(index),
        values={
            "trend_state_fast_slow_code": trend,
            "trend_strength": 1.2 if trend != 0 else 0.2,
            "ma_stack_state_code": 2 if trend > 0 else -2 if trend < 0 else 0,
            "regime_state_code": 0 if index not in {2, 7} else 2,
            "rolling_high_20": close - 0.2 if breakout == 1 else close + 0.8,
            "rolling_low_20": close + 0.2 if breakout == -1 else close - 0.8,
            "opening_range_high": 103.5,
            "opening_range_low": 99.2,
            "swing_high_10": 104.0,
            "swing_low_10": 97.0,
            "session_vwap": 100.5,
            "distance_to_session_vwap": -0.8 if index in {6, 7, 8} else 0.6 if index in {2, 3, 4} else 0.1,
            "distance_to_rolling_high_20": -0.2,
            "distance_to_rolling_low_20": 0.2,
            "bb_width_20_2": 0.15,
            "kc_width_20_1_5": 0.18,
            "squeeze_on_code": squeeze,
            "breakout_ready_state_code": breakout,
            "rvol_20": 1.3,
            "volume_zscore_20": 0.5,
            "above_below_vwma_code": 1 if close >= 100 else -1,
            "session_volume_state_code": 1,
            "htf_ma_relation_code": 1 if trend > 0 else -1,
            "htf_trend_state_code": 1 if trend > 0 else -1,
            "htf_adx_14": 30.0,
            "htf_rsi_14": 60.0 if trend > 0 else 40.0,
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
    features = [_feature(index, bar.close) for index, bar in enumerate(bars)]

    dataset_manifest = ResearchDatasetManifest(
        dataset_version="dataset-v5",
        dataset_name="stage5 synthetic",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        split_method=split_method,
        warmup_bars=0,
        bars_hash="SRC-BARS",
        created_at="2026-03-16T12:00:00Z",
        code_version="test",
        split_params={"windows": split_windows or []},
    )
    dataset_contract = phase2_research_dataset_store_contract()
    indicator_contract = phase3_indicator_store_contract()
    feature_contract = phase2b_feature_store_contract()
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
        table_path=root / "research_feature_frames.delta",
        rows=[row.to_dict() for row in features],
        columns=feature_contract["research_feature_frames"]["columns"],
    )
    return root


def _custom_registry(*specs: StrategySpec) -> StrategyRegistry:
    return StrategyRegistry(catalog=StrategyCatalog(version="stage5-test-catalog", strategies=tuple(specs)))


def test_vectorbt_batch_runner_materializes_signal_and_order_func_artifacts(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized")
    output_dir = tmp_path / "backtests"
    report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        output_dir=output_dir,
        request=BacktestBatchRequest(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            feature_set_version="features-v1",
            strategy_versions=("ma-cross-v1", "squeeze-release-v1"),
            combination_count=2,
            param_batch_size=1,
            series_batch_size=1,
            timeframe="15m",
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

    assert run_rows
    assert stats_rows
    assert trade_rows
    assert {"signals", "order_func"} <= {row["execution_mode"] for row in run_rows}
    assert all(row["trade_count"] >= 0 for row in run_rows)
    assert all("total_return" in row for row in stats_rows)
    assert all(row["direction"] in {"long", "short"} for row in trade_rows)
    for path_text in report["output_paths"].values():
        assert (Path(path_text) / "_delta_log").exists()


def test_vectorbt_batch_runner_is_reproducible_and_uses_hot_cache(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-cache")
    cache = ResearchFrameCache()
    request = BacktestBatchRequest(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        strategy_versions=("ma-cross-v1",),
        combination_count=2,
        param_batch_size=2,
        series_batch_size=1,
        timeframe="15m",
    )
    first = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-cache-a",
        request=request,
        engine_config=BacktestEngineConfig(window_count=2),
        cache=cache,
    )
    second = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
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
        required_columns=("close", "ema_10", "ema_20", "ema_50", "atr_14", "trend_state_fast_slow_code", "ma_stack_state_code"),
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
        feature_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-wf",
        request=BacktestBatchRequest(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            feature_set_version="features-v1",
            strategy_versions=("ma-cross-short-only-v1",),
            combination_count=1,
            timeframe="15m",
        ),
        engine_config=BacktestEngineConfig(allow_short=True, window_count=99),
        strategy_registry=_custom_registry(short_only_spec),
    )

    assert {row["window_id"] for row in report["run_rows"]} == {"wf-01", "wf-02"}
    assert report["trade_rows"]
    assert {row["direction"] for row in report["trade_rows"]} == {"short"}


def test_vectorbt_batch_runner_parameters_and_risk_policy_change_execution(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-param-effects")
    breakout_spec = StrategySpec(
        version="breakout-param-v1",
        family="breakout",
        description="Breakout parameter effect validation spec.",
        required_columns=("close", "high", "low", "rolling_high_20", "rolling_low_20", "adx_14", "atr_14", "breakout_ready_state_code", "trend_state_fast_slow_code"),
        parameter_grid=(
            StrategyParameter("breakout_window", (2, 8)),
            StrategyParameter("min_adx", (10,)),
            StrategyParameter("entry_buffer_atr", (1.0,)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=0.8, target_atr_multiple=1.6),
        ranking_metadata=StrategyRankingMetadata(tags=("test",)),
    )

    report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-param-effects",
        request=BacktestBatchRequest(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            feature_set_version="features-v1",
            strategy_versions=("breakout-param-v1",),
            combination_count=2,
            timeframe="15m",
        ),
        engine_config=BacktestEngineConfig(allow_short=True),
        strategy_registry=_custom_registry(breakout_spec),
    )

    breakout_stats = [row for row in report["stat_rows"] if row["strategy_version"] == "breakout-param-v1"]
    assert len({row["params_hash"] for row in breakout_stats}) == 2
    assert len({row["trade_count"] for row in breakout_stats}) > 1 or len({row["total_return"] for row in breakout_stats}) > 1


def test_vectorbt_batch_runner_handles_100_combinations_with_batching_and_cache(tmp_path: Path) -> None:
    materialized_dir = _write_materialized_layers(tmp_path / "materialized-benchmark")
    cache = ResearchFrameCache()
    benchmark_spec = StrategySpec(
        version="breakout-benchmark-v1",
        family="breakout",
        description="Large sweep validation spec.",
        required_columns=("close", "high", "low", "rolling_high_20", "rolling_low_20", "adx_14", "atr_14", "breakout_ready_state_code", "trend_state_fast_slow_code"),
        parameter_grid=(
            StrategyParameter("breakout_window", (3, 4, 5, 6, 7)),
            StrategyParameter("min_adx", (10, 12, 14, 16, 18, 20, 22, 24, 26, 28)),
            StrategyParameter("entry_buffer_atr", (0.0, 0.25)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(tags=("benchmark",)),
    )
    request = BacktestBatchRequest(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        strategy_versions=("breakout-benchmark-v1",),
        combination_count=100,
        param_batch_size=10,
        series_batch_size=1,
        timeframe="15m",
    )

    first = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        output_dir=tmp_path / "backtests-benchmark-a",
        request=request,
        engine_config=BacktestEngineConfig(),
        strategy_registry=_custom_registry(benchmark_spec),
        cache=cache,
    )
    second = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
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
        feature_output_dir=materialized_dir,
        output_dir=output_dir,
        request=BacktestBatchRequest(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            feature_set_version="features-v1",
            strategy_versions=("ma-cross-v1", "breakout-v1"),
            combination_count=3,
            param_batch_size=2,
            series_batch_size=1,
            timeframe="15m",
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
        feature_output_dir=materialized_dir,
        output_dir=output_dir,
        request=CandidateProjectionRequest(
            ranking_policy_id="stage6-selection-v1",
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
    assert any(row["robust_score"] >= 0.0 for row in ranking_rows)
    assert all("feature_snapshot_json" in row for row in candidate_rows)
    for payload in projection_report["candidate_contracts"]:
        contract = DecisionCandidate.from_dict(payload)
        assert contract.to_dict() == payload
    assert backtest_report["trade_rows"]
