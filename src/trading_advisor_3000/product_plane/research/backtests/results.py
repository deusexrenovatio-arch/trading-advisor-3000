from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows, write_delta_table_rows


@dataclass(frozen=True)
class BacktestBatchArtifact:
    backtest_batch_id: str
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str
    strategy_catalog_version: str
    combination_count: int


@dataclass(frozen=True)
class BacktestRunArtifact:
    backtest_run_id: str
    backtest_batch_id: str
    strategy_version: str
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str


def phase5_backtest_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_backtest_batches": {
            "format": "delta",
            "partition_by": ["dataset_version", "strategy_catalog_version"],
            "columns": {
                "backtest_batch_id": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "strategy_catalog_version": "string",
                "engine_name": "string",
                "param_batch_size": "int",
                "series_batch_size": "int",
                "combination_count": "int",
                "series_count": "int",
                "cache_id": "string",
                "cache_hit": "int",
                "created_at": "timestamp",
            },
        },
        "research_backtest_runs": {
            "format": "delta",
            "partition_by": ["dataset_version", "strategy_version", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "backtest_batch_id": "string",
                "strategy_version": "string",
                "strategy_family": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "params_hash": "string",
                "params_json": "json",
                "execution_mode": "string",
                "engine_name": "string",
                "row_count": "int",
                "trade_count": "int",
                "started_at": "timestamp",
                "finished_at": "timestamp",
            },
        },
        "research_strategy_stats": {
            "format": "delta",
            "partition_by": ["dataset_version", "strategy_version", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "backtest_batch_id": "string",
                "strategy_version": "string",
                "strategy_family": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "params_hash": "string",
                "total_return": "double",
                "annualized_return": "double",
                "sharpe": "double",
                "sortino": "double",
                "calmar": "double",
                "max_drawdown": "double",
                "win_rate": "double",
                "profit_factor": "double",
                "expectancy": "double",
                "trade_count": "int",
                "exposure": "double",
                "avg_trade_duration_bars": "double",
                "fees_total": "double",
                "slippage_total": "double",
                "created_at": "timestamp",
            },
        },
        "research_trade_records": {
            "format": "delta",
            "partition_by": ["dataset_version", "strategy_version", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "backtest_batch_id": "string",
                "strategy_version": "string",
                "strategy_family": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "trade_id": "string",
                "position_id": "int",
                "direction": "string",
                "status": "string",
                "entry_ts": "timestamp",
                "exit_ts": "timestamp",
                "entry_price": "double",
                "exit_price": "double",
                "size": "double",
                "pnl": "double",
                "return": "double",
                "fees_total": "double",
                "duration_bars": "int",
            },
        },
    }


def phase6_results_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_strategy_rankings": {
            "format": "delta",
            "partition_by": ["dataset_version", "ranking_policy_id", "timeframe"],
            "columns": {
                "ranking_id": "string",
                "backtest_batch_id": "string",
                "ranking_policy_id": "string",
                "selected_rank": "int",
                "representative_backtest_run_id": "string",
                "strategy_version": "string",
                "strategy_family": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "params_hash": "string",
                "params_json": "json",
                "fold_count": "int",
                "window_ids_json": "json",
                "trade_count_total": "int",
                "trade_count_min_fold": "int",
                "positive_fold_ratio": "double",
                "mean_total_return": "double",
                "median_total_return": "double",
                "mean_annualized_return": "double",
                "mean_sharpe": "double",
                "mean_sortino": "double",
                "mean_calmar": "double",
                "mean_profit_factor": "double",
                "mean_win_rate": "double",
                "worst_max_drawdown": "double",
                "slippage_stressed_pnl": "double",
                "slippage_sensitivity_score": "double",
                "fold_consistency_score": "double",
                "parameter_stability_score": "double",
                "preferred_metric_score": "double",
                "robust_score": "double",
                "out_of_sample_pass": "int",
                "policy_pass": "int",
                "created_at": "timestamp",
            },
        },
        "research_signal_candidates": {
            "format": "delta",
            "partition_by": ["dataset_version", "strategy_version_id", "timeframe"],
            "columns": {
                "candidate_id": "string",
                "signal_id": "string",
                "backtest_batch_id": "string",
                "ranking_policy_id": "string",
                "selection_policy": "string",
                "selected_rank": "int",
                "source_backtest_run_id": "string",
                "strategy_version_id": "string",
                "strategy_family": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "mode": "string",
                "side": "string",
                "entry_ref": "double",
                "stop_ref": "double",
                "target_ref": "double",
                "confidence": "double",
                "ts_decision": "timestamp",
                "params_hash": "string",
                "params_json": "json",
                "robust_score": "double",
                "signal_strength_score": "double",
                "feature_snapshot_json": "json",
                "created_at": "timestamp",
            },
        },
    }


def write_backtest_artifacts(
    *,
    output_dir: Path,
    batch_rows: list[dict[str, object]],
    run_rows: list[dict[str, object]],
    stat_rows: list[dict[str, object]],
    trade_rows: list[dict[str, object]],
) -> dict[str, str]:
    contract = phase5_backtest_store_contract()
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_path = output_dir / "research_backtest_batches.delta"
    run_path = output_dir / "research_backtest_runs.delta"
    stats_path = output_dir / "research_strategy_stats.delta"
    trades_path = output_dir / "research_trade_records.delta"

    write_delta_table_rows(
        table_path=batch_path,
        rows=batch_rows,
        columns=contract["research_backtest_batches"]["columns"],
    )
    write_delta_table_rows(
        table_path=run_path,
        rows=run_rows,
        columns=contract["research_backtest_runs"]["columns"],
    )
    write_delta_table_rows(
        table_path=stats_path,
        rows=stat_rows,
        columns=contract["research_strategy_stats"]["columns"],
    )
    write_delta_table_rows(
        table_path=trades_path,
        rows=trade_rows,
        columns=contract["research_trade_records"]["columns"],
    )
    return {
        "research_backtest_batches": batch_path.as_posix(),
        "research_backtest_runs": run_path.as_posix(),
        "research_strategy_stats": stats_path.as_posix(),
        "research_trade_records": trades_path.as_posix(),
    }


def write_stage6_artifacts(
    *,
    output_dir: Path,
    ranking_rows: list[dict[str, object]],
    candidate_rows: list[dict[str, object]] | None = None,
) -> dict[str, str]:
    contract = phase6_results_store_contract()
    output_dir.mkdir(parents=True, exist_ok=True)
    ranking_path = output_dir / "research_strategy_rankings.delta"
    write_delta_table_rows(
        table_path=ranking_path,
        rows=ranking_rows,
        columns=contract["research_strategy_rankings"]["columns"],
    )

    output_paths = {
        "research_strategy_rankings": ranking_path.as_posix(),
    }
    if candidate_rows is not None:
        candidates_path = output_dir / "research_signal_candidates.delta"
        write_delta_table_rows(
            table_path=candidates_path,
            rows=candidate_rows,
            columns=contract["research_signal_candidates"]["columns"],
        )
        output_paths["research_signal_candidates"] = candidates_path.as_posix()
    return output_paths


def load_backtest_artifacts(output_dir: Path) -> dict[str, list[dict[str, object]]]:
    return {
        "research_backtest_batches": _read_if_present(output_dir / "research_backtest_batches.delta"),
        "research_backtest_runs": _read_if_present(output_dir / "research_backtest_runs.delta"),
        "research_strategy_stats": _read_if_present(output_dir / "research_strategy_stats.delta"),
        "research_trade_records": _read_if_present(output_dir / "research_trade_records.delta"),
        "research_strategy_rankings": _read_if_present(output_dir / "research_strategy_rankings.delta"),
        "research_signal_candidates": _read_if_present(output_dir / "research_signal_candidates.delta"),
    }


def _read_if_present(table_path: Path) -> list[dict[str, object]]:
    if not has_delta_log(table_path):
        return []
    return read_delta_table_rows(table_path)
