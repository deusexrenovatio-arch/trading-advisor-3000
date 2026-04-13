from __future__ import annotations

from trading_advisor_3000.product_plane.research.backtests import BacktestBatchRequest, phase5_backtest_store_contract
from trading_advisor_3000.product_plane.research.strategies import build_phase1_strategy_registry


def test_strategy_registry_exposes_stage5_parameter_combinations_and_execution_modes() -> None:
    registry = build_phase1_strategy_registry()
    assert registry.catalog_version() == "research-strategy-catalog-v1"

    ma_cross = registry.get("ma-cross-v1")
    assert ma_cross.execution_mode == "signals"
    assert {"close", "ema_10", "ema_20", "ema_50", "trend_state_fast_slow_code"} <= set(ma_cross.required_columns)
    assert len(registry.parameter_combinations("ma-cross-v1")) == 4

    squeeze = registry.get("squeeze-release-v1")
    assert squeeze.execution_mode == "order_func"
    assert {"close", "squeeze_on_code", "breakout_ready_state_code", "atr_14"} <= set(squeeze.required_columns)
    assert len(registry.parameter_combinations("squeeze-release-v1")) == 18


def test_backtest_batch_request_id_is_deterministic_with_batch_sizes() -> None:
    request = BacktestBatchRequest(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        strategy_versions=("ma-cross-v1", "squeeze-release-v1"),
        combination_count=3,
        param_batch_size=2,
        series_batch_size=1,
        timeframe="15m",
    )
    assert request.batch_id() == request.batch_id()


def test_backtest_store_contract_contains_stage5_artifacts() -> None:
    contract = phase5_backtest_store_contract()
    assert {
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
        "avg_trade_duration_bars",
        "fees_total",
        "slippage_total",
    } <= stats_columns
    assert {"order_id", "action", "notional"} <= set(contract["research_order_records"]["columns"])
    assert {"drawdown_id", "drawdown_pct", "status"} <= set(contract["research_drawdown_records"]["columns"])
