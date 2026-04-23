from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows, write_delta_table_rows


@dataclass(frozen=True)
class BacktestBatchArtifact:
    backtest_batch_id: str
    campaign_run_id: str
    strategy_space_id: str
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str
    combination_count: int


@dataclass(frozen=True)
class BacktestRunArtifact:
    backtest_run_id: str
    backtest_batch_id: str
    campaign_run_id: str
    strategy_instance_id: str
    strategy_template_id: str
    family_id: str
    family_key: str
    dataset_version: str


def phase5_backtest_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_backtest_batches": {
            "format": "delta",
            "partition_by": ["dataset_version", "strategy_space_id"],
            "columns": {
                "backtest_batch_id": "string",
                "campaign_run_id": "string",
                "strategy_space_id": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
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
            "partition_by": ["family_key", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "backtest_batch_id": "string",
                "campaign_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "strategy_version_label": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "validation_split_id": "string",
                "parameter_values_json": "json",
                "execution_mode": "string",
                "engine_name": "string",
                "row_count": "int",
                "trade_count": "int",
                "status": "string",
                "started_at": "timestamp",
                "finished_at": "timestamp",
            },
        },
        "research_strategy_stats": {
            "format": "delta",
            "partition_by": ["family_key", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "campaign_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "strategy_version_label": "string",
                "dataset_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "total_return": "double",
                "annualized_return": "double",
                "sharpe": "double",
                "sortino": "double",
                "calmar": "double",
                "max_drawdown": "double",
                "profit_factor": "double",
                "win_rate": "double",
                "expectancy": "double",
                "avg_trade": "double",
                "avg_holding_bars": "double",
                "turnover": "double",
                "exposure": "double",
                "commission_total": "double",
                "slippage_total": "double",
                "trade_count": "int",
                "status": "string",
                "created_at": "timestamp",
            },
        },
        "research_trade_records": {
            "format": "delta",
            "partition_by": ["instrument_id", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "campaign_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "trade_id": "string",
                "side": "string",
                "status": "string",
                "entry_ts": "timestamp",
                "exit_ts": "timestamp",
                "entry_price": "double",
                "exit_price": "double",
                "qty": "double",
                "gross_pnl": "double",
                "net_pnl": "double",
                "commission": "double",
                "slippage": "double",
                "holding_bars": "int",
                "stop_ref": "double",
                "target_ref": "double",
            },
        },
        "research_order_records": {
            "format": "delta",
            "partition_by": ["instrument_id", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "campaign_run_id": "string",
                "strategy_instance_id": "string",
                "family_key": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "order_id": "string",
                "ts": "timestamp",
                "side": "string",
                "order_type": "string",
                "price": "double",
                "qty": "double",
                "fill_price": "double",
                "fill_qty": "double",
                "commission": "double",
                "slippage": "double",
                "status": "string",
            },
        },
        "research_drawdown_records": {
            "format": "delta",
            "partition_by": ["family_key", "timeframe"],
            "columns": {
                "backtest_run_id": "string",
                "campaign_run_id": "string",
                "strategy_instance_id": "string",
                "family_key": "string",
                "timeframe": "string",
                "ts": "timestamp",
                "equity": "double",
                "drawdown": "double",
                "drawdown_pct": "double",
                "peak_equity": "double",
                "window_id": "string",
                "status_code": "int",
                "status": "string",
            },
        },
    }


def phase6_results_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_strategy_rankings": {
            "format": "delta",
            "partition_by": ["family_key", "timeframe"],
            "columns": {
                "ranking_id": "string",
                "campaign_run_id": "string",
                "backtest_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "strategy_version_label": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "feature_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "rank": "int",
                "objective_score": "double",
                "score_total": "double",
                "ranking_policy_id": "string",
                "ranking_policy_json": "json",
                "rank_reason_json": "json",
                "qualifies_for_projection": "bool",
                "window_ids_json": "json",
                "created_at": "timestamp",
            },
        },
        "research_signal_candidates": {
            "format": "delta",
            "partition_by": ["instrument_id", "timeframe"],
            "columns": {
                "candidate_id": "string",
                "campaign_run_id": "string",
                "ranking_id": "string",
                "backtest_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "signal_id": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "ts_signal": "timestamp",
                "side": "string",
                "entry_ref": "double",
                "stop_ref": "double",
                "target_ref": "double",
                "score": "double",
                "estimated_commission": "double",
                "estimated_slippage": "double",
                "window_id": "string",
                "feature_snapshot_json": "json",
                "created_at": "timestamp",
            },
        },
        "research_run_findings": {
            "format": "delta",
            "partition_by": ["campaign_run_id", "finding_type"],
            "columns": {
                "finding_id": "string",
                "campaign_run_id": "string",
                "backtest_run_id": "string",
                "strategy_instance_id": "string",
                "finding_type": "string",
                "severity": "string",
                "summary": "string",
                "evidence_json": "json",
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
    order_rows: list[dict[str, object]] | None = None,
    drawdown_rows: list[dict[str, object]] | None = None,
) -> dict[str, str]:
    contract = phase5_backtest_store_contract()
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_path = output_dir / "research_backtest_batches.delta"
    run_path = output_dir / "research_backtest_runs.delta"
    stats_path = output_dir / "research_strategy_stats.delta"
    trades_path = output_dir / "research_trade_records.delta"
    orders_path = output_dir / "research_order_records.delta"
    drawdowns_path = output_dir / "research_drawdown_records.delta"
    order_rows = order_rows or []
    drawdown_rows = drawdown_rows or []

    write_delta_table_rows(table_path=batch_path, rows=batch_rows, columns=contract["research_backtest_batches"]["columns"])
    write_delta_table_rows(table_path=run_path, rows=run_rows, columns=contract["research_backtest_runs"]["columns"])
    write_delta_table_rows(table_path=stats_path, rows=stat_rows, columns=contract["research_strategy_stats"]["columns"])
    write_delta_table_rows(table_path=trades_path, rows=trade_rows, columns=contract["research_trade_records"]["columns"])
    write_delta_table_rows(table_path=orders_path, rows=order_rows, columns=contract["research_order_records"]["columns"])
    write_delta_table_rows(table_path=drawdowns_path, rows=drawdown_rows, columns=contract["research_drawdown_records"]["columns"])
    return {
        "research_backtest_batches": batch_path.as_posix(),
        "research_backtest_runs": run_path.as_posix(),
        "research_strategy_stats": stats_path.as_posix(),
        "research_trade_records": trades_path.as_posix(),
        "research_order_records": orders_path.as_posix(),
        "research_drawdown_records": drawdowns_path.as_posix(),
    }


def write_stage6_artifacts(
    *,
    output_dir: Path,
    ranking_rows: list[dict[str, object]],
    candidate_rows: list[dict[str, object]] | None = None,
    finding_rows: list[dict[str, object]] | None = None,
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
    if finding_rows is not None:
        findings_path = output_dir / "research_run_findings.delta"
        write_delta_table_rows(
            table_path=findings_path,
            rows=finding_rows,
            columns=contract["research_run_findings"]["columns"],
        )
        output_paths["research_run_findings"] = findings_path.as_posix()
    return output_paths


def load_backtest_artifacts(output_dir: Path) -> dict[str, list[dict[str, object]]]:
    return {
        "research_backtest_batches": _read_if_present(output_dir / "research_backtest_batches.delta"),
        "research_backtest_runs": _read_if_present(output_dir / "research_backtest_runs.delta"),
        "research_strategy_stats": _read_if_present(output_dir / "research_strategy_stats.delta"),
        "research_trade_records": _read_if_present(output_dir / "research_trade_records.delta"),
        "research_order_records": _read_if_present(output_dir / "research_order_records.delta"),
        "research_drawdown_records": _read_if_present(output_dir / "research_drawdown_records.delta"),
        "research_strategy_rankings": _read_if_present(output_dir / "research_strategy_rankings.delta"),
        "research_signal_candidates": _read_if_present(output_dir / "research_signal_candidates.delta"),
        "research_run_findings": _read_if_present(output_dir / "research_run_findings.delta"),
    }


def _read_if_present(table_path: Path) -> list[dict[str, object]]:
    if not has_delta_log(table_path):
        return []
    return read_delta_table_rows(table_path)
