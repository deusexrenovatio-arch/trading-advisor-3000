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
    derived_indicator_set_version: str
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


def backtest_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_strategy_search_specs": {
            "format": "delta",
            "partition_by": ["family_key", "search_spec_version"],
            "columns": {
                "search_spec_id": "string",
                "family_key": "string",
                "template_key": "string",
                "search_spec_version": "string",
                "intent": "string",
                "clock_profiles": "json",
                "market_states": "json",
                "required_price_inputs_json": "json",
                "required_materialized_indicators_json": "json",
                "required_materialized_derived_json": "json",
                "optional_indicator_plan_json": "json",
                "signal_surface_key": "string",
                "signal_surface_mode": "string",
                "parameter_mode": "string",
                "parameter_space_json": "json",
                "parameter_constraints_json": "json",
                "clock_profile_json": "json",
                "required_inputs_by_clock_json": "json",
                "parameter_space_by_role_json": "json",
                "parameter_clock_map_json": "json",
                "exit_parameter_space_json": "json",
                "risk_parameter_space_json": "json",
                "execution_assumptions_json": "json",
                "created_at": "timestamp",
            },
        },
        "research_vbt_search_runs": {
            "format": "delta",
            "partition_by": ["family_key", "clock_profile"],
            "columns": {
                "search_run_id": "string",
                "search_spec_id": "string",
                "campaign_id": "string",
                "family_key": "string",
                "template_key": "string",
                "clock_profile": "string",
                "dataset_id": "string",
                "dataset_snapshot": "string",
                "indicator_profile_version": "string",
                "derived_indicator_profile_version": "string",
                "universe_key": "string",
                "fold_id": "string",
                "param_count": "int",
                "instrument_count": "int",
                "chunk_count": "int",
                "status": "string",
                "started_at": "timestamp",
                "finished_at": "timestamp",
                "error_message": "string",
            },
        },
        "research_vbt_param_results": {
            "format": "delta",
            "partition_by": ["family_key", "clock_profile"],
            "columns": {
                "search_run_id": "string",
                "param_hash": "string",
                "family_key": "string",
                "template_key": "string",
                "clock_profile": "string",
                "instrument_id": "string",
                "fold_id": "string",
                "params_json": "json",
                "indicator_plan_hash": "string",
                "net_pnl": "double",
                "sharpe": "double",
                "sortino": "double",
                "calmar": "double",
                "max_drawdown": "double",
                "profit_factor": "double",
                "win_rate": "double",
                "avg_trade": "double",
                "trade_count": "int",
                "turnover": "double",
                "avg_holding_minutes": "double",
                "fees_paid": "double",
                "slippage_paid": "double",
                "exposure_avg": "double",
                "stress_label": "string",
                "created_at": "timestamp",
            },
        },
        "research_vbt_param_gate_events": {
            "format": "delta",
            "partition_by": ["gate_name", "passed"],
            "columns": {
                "search_run_id": "string",
                "param_hash": "string",
                "gate_name": "string",
                "passed": "int",
                "failure_code": "string",
                "failure_reason": "string",
                "metric_snapshot_json": "json",
                "created_at": "timestamp",
            },
        },
        "research_vbt_ephemeral_indicator_cache": {
            "format": "delta",
            "partition_by": ["family_key", "provider"],
            "columns": {
                "cache_id": "string",
                "search_run_id": "string",
                "family_key": "string",
                "clock_profile": "string",
                "dataset_id": "string",
                "dataset_snapshot": "string",
                "provider": "string",
                "provider_version": "string",
                "indicator_key": "string",
                "indicator_params_json": "json",
                "input_columns": "json",
                "output_columns": "json",
                "output_hash": "string",
                "warmup_bars": "int",
                "index_start": "timestamp",
                "index_end": "timestamp",
                "instrument_count": "int",
                "row_count": "int",
                "cache_scope": "string",
                "created_at": "timestamp",
            },
        },
        "research_strategy_promotion_events": {
            "format": "delta",
            "partition_by": ["family_key", "promotion_stage"],
            "columns": {
                "promotion_id": "string",
                "search_run_id": "string",
                "param_hash": "string",
                "promoted_to_instance_id": "string",
                "family_key": "string",
                "template_key": "string",
                "promotion_stage": "string",
                "promotion_reason": "string",
                "params_json": "json",
                "metrics_snapshot_json": "json",
                "manifest_hash": "string",
                "created_at": "timestamp",
            },
        },
        "research_backtest_batches": {
            "format": "delta",
            "partition_by": ["dataset_version", "strategy_space_id"],
            "columns": {
                "backtest_batch_id": "string",
                "campaign_run_id": "string",
                "strategy_space_id": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "derived_indicator_set_version": "string",
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
                "search_run_id": "string",
                "backtest_batch_id": "string",
                "campaign_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "template_key": "string",
                "strategy_version_label": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "derived_indicator_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "clock_profile": "string",
                "window_id": "string",
                "validation_split_id": "string",
                "param_hash": "string",
                "params_hash": "string",
                "parameter_values_json": "json",
                "params_json": "json",
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
                "search_run_id": "string",
                "campaign_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "template_key": "string",
                "strategy_version_label": "string",
                "dataset_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "param_hash": "string",
                "params_hash": "string",
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
                "param_hash": "string",
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
                "param_hash": "string",
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
                "param_hash": "string",
                "status_code": "int",
                "status": "string",
            },
        },
    }


def results_store_contract() -> dict[str, dict[str, object]]:
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
                "derived_indicator_set_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "rank": "int",
                "family_rank": "int",
                "selected_rank": "int",
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
                "indicator_context_json": "json",
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
    search_spec_rows: list[dict[str, object]] | None = None,
    search_run_rows: list[dict[str, object]] | None = None,
    param_result_rows: list[dict[str, object]] | None = None,
    gate_event_rows: list[dict[str, object]] | None = None,
    ephemeral_indicator_rows: list[dict[str, object]] | None = None,
    promotion_event_rows: list[dict[str, object]] | None = None,
    order_rows: list[dict[str, object]] | None = None,
    drawdown_rows: list[dict[str, object]] | None = None,
) -> dict[str, str]:
    contract = backtest_store_contract()
    output_dir.mkdir(parents=True, exist_ok=True)
    search_specs_path = output_dir / "research_strategy_search_specs.delta"
    search_runs_path = output_dir / "research_vbt_search_runs.delta"
    param_results_path = output_dir / "research_vbt_param_results.delta"
    gate_events_path = output_dir / "research_vbt_param_gate_events.delta"
    ephemeral_path = output_dir / "research_vbt_ephemeral_indicator_cache.delta"
    promotion_path = output_dir / "research_strategy_promotion_events.delta"
    batch_path = output_dir / "research_backtest_batches.delta"
    run_path = output_dir / "research_backtest_runs.delta"
    stats_path = output_dir / "research_strategy_stats.delta"
    trades_path = output_dir / "research_trade_records.delta"
    orders_path = output_dir / "research_order_records.delta"
    drawdowns_path = output_dir / "research_drawdown_records.delta"
    search_spec_rows = search_spec_rows or []
    search_run_rows = search_run_rows or []
    param_result_rows = param_result_rows or []
    gate_event_rows = gate_event_rows or []
    ephemeral_indicator_rows = ephemeral_indicator_rows or []
    promotion_event_rows = promotion_event_rows or []
    order_rows = order_rows or []
    drawdown_rows = drawdown_rows or []

    write_delta_table_rows(table_path=search_specs_path, rows=search_spec_rows, columns=contract["research_strategy_search_specs"]["columns"])
    write_delta_table_rows(table_path=search_runs_path, rows=search_run_rows, columns=contract["research_vbt_search_runs"]["columns"])
    write_delta_table_rows(table_path=param_results_path, rows=param_result_rows, columns=contract["research_vbt_param_results"]["columns"])
    write_delta_table_rows(table_path=gate_events_path, rows=gate_event_rows, columns=contract["research_vbt_param_gate_events"]["columns"])
    write_delta_table_rows(table_path=ephemeral_path, rows=ephemeral_indicator_rows, columns=contract["research_vbt_ephemeral_indicator_cache"]["columns"])
    write_delta_table_rows(table_path=promotion_path, rows=promotion_event_rows, columns=contract["research_strategy_promotion_events"]["columns"])
    write_delta_table_rows(table_path=batch_path, rows=batch_rows, columns=contract["research_backtest_batches"]["columns"])
    write_delta_table_rows(table_path=run_path, rows=run_rows, columns=contract["research_backtest_runs"]["columns"])
    write_delta_table_rows(table_path=stats_path, rows=stat_rows, columns=contract["research_strategy_stats"]["columns"])
    write_delta_table_rows(table_path=trades_path, rows=trade_rows, columns=contract["research_trade_records"]["columns"])
    write_delta_table_rows(table_path=orders_path, rows=order_rows, columns=contract["research_order_records"]["columns"])
    write_delta_table_rows(table_path=drawdowns_path, rows=drawdown_rows, columns=contract["research_drawdown_records"]["columns"])
    return {
        "research_strategy_search_specs": search_specs_path.as_posix(),
        "research_vbt_search_runs": search_runs_path.as_posix(),
        "research_vbt_param_results": param_results_path.as_posix(),
        "research_vbt_param_gate_events": gate_events_path.as_posix(),
        "research_vbt_ephemeral_indicator_cache": ephemeral_path.as_posix(),
        "research_strategy_promotion_events": promotion_path.as_posix(),
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
    contract = results_store_contract()
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
        "research_strategy_search_specs": _read_if_present(output_dir / "research_strategy_search_specs.delta"),
        "research_vbt_search_runs": _read_if_present(output_dir / "research_vbt_search_runs.delta"),
        "research_vbt_param_results": _read_if_present(output_dir / "research_vbt_param_results.delta"),
        "research_vbt_param_gate_events": _read_if_present(output_dir / "research_vbt_param_gate_events.delta"),
        "research_vbt_ephemeral_indicator_cache": _read_if_present(output_dir / "research_vbt_ephemeral_indicator_cache.delta"),
        "research_strategy_promotion_events": _read_if_present(output_dir / "research_strategy_promotion_events.delta"),
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
