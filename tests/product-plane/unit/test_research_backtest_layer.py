from __future__ import annotations

from trading_advisor_3000.product_plane.research.backtests import BacktestBatchRequest, backtest_store_contract, build_ephemeral_strategy_space
from trading_advisor_3000.product_plane.research.backtests.input_requirements import loader_columns_for_search_specs
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
        "research_vbt_param_results",
        "research_vbt_param_gate_events",
        "research_vbt_ephemeral_indicator_cache",
        "research_strategy_promotion_events",
        "research_optimizer_studies",
        "research_optimizer_trials",
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
    assert {"optimizer_study_id", "sampler", "n_trials_requested"} <= set(
        contract["research_optimizer_studies"]["columns"]
    )
    assert {"optimizer_trial_id", "trial_number", "trial_kind", "status"} <= set(
        contract["research_optimizer_trials"]["columns"]
    )
