from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research.backtests import BacktestBatchRequest, backtest_store_contract, build_ephemeral_strategy_space
from trading_advisor_3000.product_plane.research.backtests.input_requirements import loader_columns_for_search_specs
from trading_advisor_3000.product_plane.research.datasets import research_dataset_store_contract
from trading_advisor_3000.product_plane.research.derived_indicators import research_derived_indicator_store_contract
from trading_advisor_3000.product_plane.research.indicators import indicator_store_contract
from trading_advisor_3000.product_plane.research.io import loaders
from trading_advisor_3000.product_plane.research.io.loaders import ResearchSliceRequest
from trading_advisor_3000.product_plane.research.strategies import build_strategy_registry


def test_strategy_registry_exposes_stage5_parameter_combinations_and_execution_modes() -> None:
    registry = build_strategy_registry()
    assert registry.catalog_version() == "research-strategy-catalog-v1"

    ma_cross = registry.get("ma-cross-v1")
    assert ma_cross.execution_mode == "signals"
    assert {"close", "ema_10", "ema_20", "ema_50", "atr_14"} <= set(ma_cross.required_columns)
    assert len(registry.parameter_combinations("ma-cross-v1")) == 4

    squeeze = registry.get("volatility-squeeze-release-v1")
    assert squeeze.execution_mode == "signals"
    assert {
        "close",
        "bb_position_20_2",
        "kc_position_20_1_5",
        "bb_width_20_2",
        "atr_14",
    } <= set(squeeze.required_columns)
    assert "mtf_4h_to_15m_bb_position_20_2" not in squeeze.required_columns
    assert set(squeeze.required_columns_for_role("decision")) >= {
        "bb_width_20_2",
        "bb_position_20_2",
        "kc_position_20_1_5",
    }
    assert len(registry.parameter_combinations("volatility-squeeze-release-v1")) == 1944


def test_backtest_batch_request_id_is_deterministic_with_batch_sizes() -> None:
    request = BacktestBatchRequest(
        campaign_run_id="crun_test",
        strategy_space_id="sspace_test",
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        search_specs=build_ephemeral_strategy_space(
            strategy_version_labels=("ma-cross-v1", "volatility-squeeze-release-v1"),
            instances_per_strategy=2,
        ).search_specs,
        combination_count=3,
        param_batch_size=2,
        series_batch_size=1,
        timeframe="15m",
    )
    assert request.batch_id() == request.batch_id()


def test_loader_columns_for_search_specs_include_native_clock_inputs() -> None:
    search_specs = build_ephemeral_strategy_space(
        strategy_version_labels=("ma-cross-v1",),
        instances_per_strategy=2,
    ).search_specs

    columns = loader_columns_for_search_specs(search_specs)

    assert "close" in columns.price_columns
    assert {"ema_10", "ema_20", "ema_50", "atr_14"} <= set(columns.indicator_columns)


def test_backtest_store_contract_contains_stage5_artifacts() -> None:
    contract = backtest_store_contract()
    assert {
        "research_strategy_search_specs",
        "research_vbt_search_runs",
        "research_optimizer_studies",
        "research_optimizer_trials",
        "research_vbt_param_results",
        "research_vbt_param_gate_events",
        "research_vbt_ephemeral_indicator_cache",
        "research_strategy_promotion_events",
        "research_backtest_batches",
        "research_backtest_runs",
        "research_strategy_stats",
        "research_trade_records",
        "research_order_records",
        "research_drawdown_records",
    } == set(contract)
    stats_columns = set(contract["research_strategy_stats"]["columns"])
    assert {
        "total_return",
        "annualized_return",
        "sharpe",
        "sortino",
        "calmar",
        "max_drawdown",
        "win_rate",
        "profit_factor",
        "expectancy",
        "trade_count",
        "exposure",
        "avg_holding_bars",
        "commission_total",
        "slippage_total",
    } <= stats_columns
    assert {"order_id", "side", "fill_qty"} <= set(contract["research_order_records"]["columns"])
    assert {"ts", "drawdown_pct", "status"} <= set(contract["research_drawdown_records"]["columns"])
    assert {"optimizer_study_id", "sampler", "n_trials_requested", "best_param_hash", "study_config_json"} <= set(
        contract["research_optimizer_studies"]["columns"]
    )
    assert {"optimizer_trial_id", "trial_number", "trial_kind", "status", "objective_components_json"} <= set(
        contract["research_optimizer_trials"]["columns"]
    )
    assert {
        "duration_seconds",
        "evaluations_per_second",
        "run_rows_per_second",
        "trade_rows_per_second",
    } <= set(contract["research_backtest_batches"]["columns"])


def test_backtest_loader_uses_native_delta_projection_without_python_row_reload(
    tmp_path: Path,
) -> None:
    materialized = tmp_path / "materialized"
    dataset_contract = research_dataset_store_contract()
    indicator_contract = indicator_store_contract()
    derived_contract = research_derived_indicator_store_contract()
    write_delta_table_rows(
        table_path=materialized / "research_bar_views.delta",
        columns=dataset_contract["research_bar_views"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:00:00Z",
                "open": 99.0,
                "high": 101.0,
                "low": 98.5,
                "close": 100.0,
                "volume": 10_000,
                "open_interest": 20_000,
                "session_date": "2026-04-01",
                "session_open_ts": "2026-04-01T07:00:00Z",
                "session_close_ts": "2026-04-01T21:00:00Z",
                "active_contract_id": "BR-6.26",
                "bar_index": 0,
                "slice_role": "analysis",
            },
            {
                "dataset_version": "dataset-v1",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:15:00Z",
                "open": 100.0,
                "high": 102.0,
                "low": 99.5,
                "close": 101.0,
                "volume": 11_000,
                "open_interest": 20_100,
                "session_date": "2026-04-01",
                "session_open_ts": "2026-04-01T07:00:00Z",
                "session_close_ts": "2026-04-01T21:00:00Z",
                "active_contract_id": "BR-6.26",
                "bar_index": 1,
                "slice_role": "warmup",
            },
        ],
    )
    write_delta_table_rows(
        table_path=materialized / "research_indicator_frames.delta",
        columns=indicator_contract["research_indicator_frames"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "indicator_set_version": "indicators-v1",
                "profile_version": "core_v1",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:00:00Z",
                "rsi_14": 42.0,
                "atr_14": 1.5,
                "ema_10": 100.0,
                "source_bars_hash": "SRC",
                "row_count": 2,
                "warmup_span": 0,
                "null_warmup_span": 0,
                "created_at": "2026-04-01T10:00:00Z",
            }
        ],
    )
    write_delta_table_rows(
        table_path=materialized / "research_derived_indicator_frames.delta",
        columns=derived_contract["research_derived_indicator_frames"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "indicator_set_version": "indicators-v1",
                "derived_indicator_set_version": "derived-v1",
                "profile_version": "core_v1",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:00:00Z",
                "distance_to_session_vwap": -0.25,
                "rolling_high_20": 102.0,
                "source_bars_hash": "SRC",
                "source_indicators_hash": "IND",
                "row_count": 2,
                "warmup_span": 0,
                "null_warmup_span": 0,
                "created_at": "2026-04-01T10:00:00Z",
            }
        ],
    )

    frames, _, cache_hit = loaders.load_backtest_frames(
        dataset_output_dir=materialized,
        indicator_output_dir=materialized,
        derived_indicator_output_dir=materialized,
        request=ResearchSliceRequest(
            dataset_version="dataset-v1",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            timeframe="15m",
            instrument_ids=("FUT_BR",),
            price_columns=("close",),
            indicator_columns=("rsi_14",),
            derived_columns=("distance_to_session_vwap",),
        ),
    )

    assert cache_hit is False
    assert len(frames) == 1
    frame = frames[0].frame
    assert len(frame) == 1
    assert {"close", "rsi_14", "distance_to_session_vwap"} <= set(frame.columns)
    assert {"open", "high", "low", "volume", "slice_role", "atr_14", "ema_10", "rolling_high_20"}.isdisjoint(frame.columns)
