from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Mapping

import pandas as pd

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows
from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_strategy_registry

from .results import backtest_store_contract, results_store_contract, write_stage6_artifacts


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clip_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _finding_id(*, campaign_run_id: str, backtest_run_id: str, finding_type: str, ranking_id: str = "") -> str:
    return "FIND-" + _stable_hash(f"{campaign_run_id}|{ranking_id}|{backtest_run_id}|{finding_type}")


_RUN_KEY_FIELDS = ("backtest_run_id", "campaign_run_id", "contract_id", "instrument_id", "timeframe", "window_id")


def _read_backtest_result_rows(output_dir: Path, table_name: str) -> list[dict[str, object]]:
    table_path = output_dir / f"{table_name}.delta"
    if not has_delta_log(table_path):
        return []
    columns = list(backtest_store_contract()[table_name]["columns"])
    return read_delta_table_rows(table_path, columns=columns)


def _run_identity(row: Mapping[str, object]) -> tuple[str, ...]:
    defaults = {
        "campaign_run_id": "crun_unbound",
        "contract_id": "",
        "instrument_id": "",
        "timeframe": "",
        "window_id": "",
    }
    return tuple(str(row.get(field, defaults.get(field, ""))) for field in _RUN_KEY_FIELDS)


def _score_metric(metric_name: str, value: float, *, drawdown_cap: float) -> float:
    if metric_name in {"total_return", "annualized_return"}:
        return _clip_unit(0.5 + (0.5 * math.tanh(value * 6.0)))
    if metric_name in {"sharpe", "sortino", "calmar"}:
        return _clip_unit((value + 0.5) / 2.5)
    if metric_name == "profit_factor":
        return _clip_unit((value - 0.8) / 1.2)
    if metric_name == "win_rate":
        return _clip_unit((value - 0.4) / 0.3)
    if metric_name == "max_drawdown":
        return _clip_unit(1.0 - (value / max(drawdown_cap, 1e-9)))
    return 0.5


def _coerce_json(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        payload = json.loads(value)
        if isinstance(payload, dict):
            return {str(key): item for key, item in payload.items()}
    return {}


def _param_distance(left: dict[str, object], right: dict[str, object]) -> int:
    keys = set(left) | set(right)
    return sum(1 for key in keys if left.get(key) != right.get(key))


def _trade_stress(rows: list[dict[str, object]], *, extra_slippage_bps: float) -> tuple[float, float]:
    if not rows:
        return 0.0, 0.0
    gross_pnl = sum(float(row.get("net_pnl", 0.0) or 0.0) for row in rows)
    extra_cost = sum(
        (
            (abs(float(row.get("entry_price", 0.0) or 0.0)) + abs(float(row.get("exit_price", 0.0) or 0.0)))
            * abs(float(row.get("qty", 0.0) or 0.0))
            * (extra_slippage_bps / 10_000.0)
        )
        for row in rows
    )
    stressed_pnl = gross_pnl - extra_cost
    if gross_pnl <= 0.0:
        return stressed_pnl, 0.0
    if stressed_pnl > 0.0:
        return stressed_pnl, 1.0
    return stressed_pnl, _clip_unit(gross_pnl / max(gross_pnl + extra_cost, 1e-9))


def _policy_failure_reasons(
    *,
    out_of_sample_pass: bool,
    fold_count: int,
    trade_count_total: int,
    trade_count_min_fold: int,
    positive_fold_ratio: float,
    max_drawdown: float,
    parameter_stability_score: float,
    slippage_score: float,
    policy: RankingPolicy,
) -> list[str]:
    reasons: list[str] = []
    if not out_of_sample_pass and policy.require_out_of_sample_pass:
        if fold_count < policy.min_fold_count:
            reasons.append("min_fold_count")
        if trade_count_total < policy.min_trade_count:
            reasons.append("min_trade_count")
        if trade_count_min_fold < max(1, min(policy.min_trade_count, trade_count_total)):
            reasons.append("min_fold_trade_count")
        if positive_fold_ratio < policy.min_positive_fold_ratio:
            reasons.append("positive_fold_ratio")
    if max_drawdown > policy.max_drawdown_cap:
        reasons.append("max_drawdown")
    if parameter_stability_score < policy.min_parameter_stability:
        reasons.append("parameter_stability")
    if slippage_score < policy.min_slippage_score:
        reasons.append("slippage_sensitivity")
    return reasons


def _metric_value(metric_name: str, payload: dict[str, float]) -> float:
    return float(payload.get(metric_name, 0.0))


def _policy_metric_components(
    *,
    metric_lookup: dict[str, float],
    policy: RankingPolicy,
) -> tuple[float, dict[str, float]]:
    ordered_scores: dict[str, float] = {}
    weighted_sum = 0.0
    total_weight = 0.0
    metric_count = len(policy.metric_order)
    for index, metric_name in enumerate(policy.metric_order):
        score = _score_metric(metric_name, _metric_value(metric_name, metric_lookup), drawdown_cap=policy.max_drawdown_cap)
        ordered_scores[metric_name] = score
        weight = float(metric_count - index)
        weighted_sum += weight * score
        total_weight += weight
    combined = weighted_sum / total_weight if total_weight > 0.0 else 0.0
    return combined, ordered_scores


@dataclass(frozen=True)
class RankingPolicy:
    policy_id: str
    metric_order: tuple[str, ...]
    require_out_of_sample_pass: bool = True
    min_trade_count: int = 4
    min_fold_count: int = 1
    max_drawdown_cap: float = 0.35
    min_positive_fold_ratio: float = 0.5
    stress_slippage_bps: float = 7.5
    min_parameter_stability: float = 0.35
    min_slippage_score: float = 0.45

    def __post_init__(self) -> None:
        if not self.metric_order:
            raise ValueError("metric_order must not be empty")
        if self.min_trade_count <= 0:
            raise ValueError("min_trade_count must be positive")
        if self.min_fold_count <= 0:
            raise ValueError("min_fold_count must be positive")
        if self.max_drawdown_cap <= 0.0:
            raise ValueError("max_drawdown_cap must be positive")
        if self.stress_slippage_bps < 0.0:
            raise ValueError("stress_slippage_bps must be non-negative")


def default_ranking_policy() -> RankingPolicy:
    return RankingPolicy(
        policy_id="robust_oos_v1",
        metric_order=("total_return", "profit_factor", "max_drawdown"),
    )


OPTUNA_TRIAL_CONSTRAINT_NAMES = (
    "min_fold_count",
    "min_trade_count",
    "min_fold_trade_count",
    "positive_fold_ratio",
    "max_drawdown",
    "parameter_stability",
    "slippage_sensitivity",
)


def score_optimizer_trial(
    *,
    param_rows: list[dict[str, object]],
    trade_rows: list[dict[str, object]],
    policy: RankingPolicy | None = None,
    parameter_stability_score: float | None = None,
) -> dict[str, object]:
    resolved_policy = policy or default_ranking_policy()
    if not param_rows:
        constraint_values = (1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        return {
            "value": -1.0,
            "constraints_passed": False,
            "constraint_names": OPTUNA_TRIAL_CONSTRAINT_NAMES,
            "constraint_values": constraint_values,
            "failure": "NO_PARAM_RESULTS",
            "row_count": 0,
            "score_formula": resolved_policy.policy_id,
            "selection_owner": "optuna.study",
        }

    net_pnls = [float(row.get("net_pnl", 0.0) or 0.0) for row in param_rows]
    total_returns = [value / 100.0 for value in net_pnls]
    trade_counts = [int(float(row.get("trade_count", 0) or 0)) for row in param_rows]
    fold_count = len(param_rows)
    drawdowns = [abs(float(row.get("max_drawdown", 0.0) or 0.0)) for row in param_rows]
    profit_factors = [float(row.get("profit_factor", 0.0) or 0.0) for row in param_rows]
    win_rates = [float(row.get("win_rate", 0.0) or 0.0) for row in param_rows]
    avg_trades = [float(row.get("avg_trade", 0.0) or 0.0) for row in param_rows]
    turnovers = [float(row.get("turnover", 0.0) or 0.0) for row in param_rows]
    fees_paid = [float(row.get("fees_paid", 0.0) or 0.0) for row in param_rows]
    slippage_paid = [float(row.get("slippage_paid", 0.0) or 0.0) for row in param_rows]
    trade_count_total = int(sum(trade_counts))
    trade_count_min_fold = int(min(trade_counts)) if trade_counts else 0
    mean_total_return = mean(total_returns) if total_returns else 0.0
    mean_net_pnl = mean(net_pnls) if net_pnls else 0.0
    total_return_std = pd.Series(total_returns, dtype="float64").std(ddof=0) if len(total_returns) > 1 else 0.0
    positive_fold_ratio = sum(1 for value in total_returns if value > 0.0) / len(total_returns)
    fold_consistency_score = _clip_unit(
        positive_fold_ratio * (1.0 / (1.0 + (float(total_return_std) / max(abs(mean_total_return), 0.02))))
    )
    max_drawdown = max(drawdowns) if drawdowns else 0.0
    metric_lookup = {
        "total_return": float(mean_total_return),
        "annualized_return": float(mean_total_return),
        "sharpe": float(mean(float(row.get("sharpe", 0.0) or 0.0) for row in param_rows)),
        "sortino": float(mean(float(row.get("sortino", 0.0) or 0.0) for row in param_rows)),
        "calmar": float(mean(float(row.get("calmar", 0.0) or 0.0) for row in param_rows)),
        "profit_factor": float(mean(profit_factors)) if profit_factors else 0.0,
        "win_rate": float(mean(win_rates)) if win_rates else 0.0,
        "max_drawdown": float(max_drawdown),
    }
    objective_score, policy_metric_vector = _policy_metric_components(
        metric_lookup=metric_lookup,
        policy=resolved_policy,
    )
    _, slippage_score = _trade_stress(trade_rows, extra_slippage_bps=resolved_policy.stress_slippage_bps)
    stability_score = _clip_unit(0.5 if parameter_stability_score is None else float(parameter_stability_score))
    preferred_metric_score = objective_score
    score_total = (
        (0.30 * objective_score)
        + (0.20 * fold_consistency_score)
        + (0.20 * stability_score)
        + (0.15 * slippage_score)
        + (0.15 * preferred_metric_score)
    )
    gross_pnl_total = float(sum(float(row.get("gross_pnl", 0.0) or 0.0) for row in trade_rows))
    trade_net_pnl_total = float(sum(float(row.get("net_pnl", 0.0) or 0.0) for row in trade_rows))
    trade_commission_total = float(sum(float(row.get("commission", 0.0) or 0.0) for row in trade_rows))
    min_fold_trades = max(1, min(resolved_policy.min_trade_count, trade_count_total))
    constraint_values = (
        float(resolved_policy.min_fold_count - fold_count),
        float(resolved_policy.min_trade_count - trade_count_total),
        float(min_fold_trades - trade_count_min_fold),
        float(resolved_policy.min_positive_fold_ratio - positive_fold_ratio),
        float(max_drawdown - resolved_policy.max_drawdown_cap),
        float(resolved_policy.min_parameter_stability - stability_score),
        float(resolved_policy.min_slippage_score - slippage_score),
    )
    if not resolved_policy.require_out_of_sample_pass:
        constraint_values = (
            min(0.0, constraint_values[0]),
            min(0.0, constraint_values[1]),
            min(0.0, constraint_values[2]),
            min(0.0, constraint_values[3]),
            constraint_values[4],
            constraint_values[5],
            constraint_values[6],
        )
    constraints_passed = all(value <= 0.0 for value in constraint_values)
    return {
        "value": float(score_total),
        "constraints_passed": constraints_passed,
        "constraint_names": OPTUNA_TRIAL_CONSTRAINT_NAMES,
        "constraint_values": constraint_values,
        "row_count": len(param_rows),
        "fold_count": fold_count,
        "trade_count_total": trade_count_total,
        "trade_count_min_fold": trade_count_min_fold,
        "net_pnl_total": float(sum(net_pnls)),
        "net_pnl_mean": float(mean_net_pnl),
        "trade_gross_pnl_total": gross_pnl_total,
        "trade_net_pnl_total": trade_net_pnl_total,
        "trade_commission_total": trade_commission_total,
        "fees_paid_total": float(sum(fees_paid)),
        "slippage_paid_total": float(sum(slippage_paid)),
        "mean_total_return": float(mean_total_return),
        "total_return_mean": float(mean_total_return),
        "total_return_min": float(min(total_returns)) if total_returns else 0.0,
        "total_return_max": float(max(total_returns)) if total_returns else 0.0,
        "total_return_std": float(total_return_std),
        "profit_factor_mean": float(mean(profit_factors)) if profit_factors else 0.0,
        "profit_factor_min": float(min(profit_factors)) if profit_factors else 0.0,
        "profit_factor_max": float(max(profit_factors)) if profit_factors else 0.0,
        "win_rate_mean": float(mean(win_rates)) if win_rates else 0.0,
        "avg_trade_mean": float(mean(avg_trades)) if avg_trades else 0.0,
        "turnover_mean": float(mean(turnovers)) if turnovers else 0.0,
        "positive_fold_ratio": float(positive_fold_ratio),
        "max_drawdown": float(max_drawdown),
        "objective_score": float(objective_score),
        "policy_metric_vector": policy_metric_vector,
        "fold_consistency_score": float(fold_consistency_score),
        "parameter_stability_score": float(stability_score),
        "parameter_stability_source": "single_trial_default" if parameter_stability_score is None else "provided",
        "slippage_sensitivity_score": float(slippage_score),
        "preferred_metric_score": float(preferred_metric_score),
        "score_total": float(score_total),
        "score_formula": resolved_policy.policy_id,
        "selection_owner": "optuna.study",
    }


def rank_backtest_results(
    *,
    backtest_output_dir: Path | None = None,
    output_dir: Path | None = None,
    batch_rows: list[dict[str, object]] | None = None,
    run_rows: list[dict[str, object]] | None = None,
    stat_rows: list[dict[str, object]] | None = None,
    trade_rows: list[dict[str, object]] | None = None,
    policy: RankingPolicy | None = None,
    strategy_registry: StrategyRegistry | None = None,
) -> dict[str, object]:
    resolved_policy = policy or default_ranking_policy()
    registry = strategy_registry or build_strategy_registry()
    if backtest_output_dir is not None and any(rows is None for rows in (batch_rows, run_rows, stat_rows, trade_rows)):
        batch_rows = batch_rows if batch_rows is not None else _read_backtest_result_rows(backtest_output_dir, "research_backtest_batches")
        run_rows = run_rows if run_rows is not None else _read_backtest_result_rows(backtest_output_dir, "research_backtest_runs")
        stat_rows = stat_rows if stat_rows is not None else _read_backtest_result_rows(backtest_output_dir, "research_strategy_stats")
        trade_rows = trade_rows if trade_rows is not None else _read_backtest_result_rows(backtest_output_dir, "research_trade_records")

    batch_rows = batch_rows or []
    run_rows = run_rows or []
    stat_rows = stat_rows or []
    trade_rows = trade_rows or []
    if not run_rows or not stat_rows:
        raise ValueError("ranking requires backtest run rows and strategy stat rows")

    run_frame = pd.DataFrame(run_rows)
    stat_frame = pd.DataFrame(stat_rows)
    for frame in (run_frame, stat_frame):
        if "campaign_run_id" not in frame.columns:
            frame["campaign_run_id"] = "crun_unbound"
        if "family_key" not in frame.columns and "strategy_family" in frame.columns:
            frame["family_key"] = frame["strategy_family"]
        if "family_id" not in frame.columns:
            frame["family_id"] = frame["family_key"].map(lambda value: f"sfam_{value}")
        if "strategy_version_label" not in frame.columns and "strategy_version" in frame.columns:
            frame["strategy_version_label"] = frame["strategy_version"]
        if "strategy_template_id" not in frame.columns:
            frame["strategy_template_id"] = frame["family_key"].map(lambda value: f"stpl_{value}")
        if "strategy_instance_id" not in frame.columns:
            fallback_hash = frame["backtest_run_id"] if "backtest_run_id" in frame.columns else frame.index
            if "params_hash" in frame.columns:
                fallback_hash = frame["params_hash"]
            frame["strategy_instance_id"] = [f"sinst_{value}" for value in fallback_hash]
    if "parameter_values_json" not in run_frame.columns:
        if "params_json" in run_frame.columns:
            run_frame["parameter_values_json"] = run_frame["params_json"]
        else:
            run_frame["parameter_values_json"] = [{} for _ in range(len(run_frame))]

    trades_by_run: dict[tuple[str, ...], list[dict[str, object]]] = {}
    for row in trade_rows:
        trades_by_run.setdefault(_run_identity(row), []).append(row)

    grouped_rows: list[dict[str, object]] = []
    group_columns = [
        "campaign_run_id",
        "strategy_instance_id",
        "strategy_template_id",
        "family_id",
        "family_key",
        "strategy_version_label",
        "dataset_version",
        "contract_id",
        "instrument_id",
        "timeframe",
    ]

    for _, group in stat_frame.groupby(group_columns, dropna=False, sort=True):
        first = group.iloc[0]
        matching_runs = run_frame[
            (run_frame["campaign_run_id"] == first["campaign_run_id"])
            & (run_frame["strategy_instance_id"] == first["strategy_instance_id"])
            & (run_frame["contract_id"] == first["contract_id"])
            & (run_frame["instrument_id"] == first["instrument_id"])
            & (run_frame["timeframe"] == first["timeframe"])
        ]
        stressed_runs = [
            _trade_stress(trades_by_run.get(_run_identity(row), []), extra_slippage_bps=resolved_policy.stress_slippage_bps)
            for row in group.to_dict(orient="records")
        ]
        slippage_score = mean(item[1] for item in stressed_runs) if stressed_runs else 0.0

        mean_total_return = float(group["total_return"].mean())
        total_return_std = float(group["total_return"].std(ddof=0)) if len(group) > 1 else 0.0
        positive_fold_ratio = float((group["total_return"] > 0.0).mean())
        fold_consistency_score = _clip_unit(
            positive_fold_ratio * (1.0 / (1.0 + (total_return_std / max(abs(mean_total_return), 0.02))))
        )

        strategy_spec = registry.get(str(first["strategy_version_label"]))
        metric_lookup = {
            "total_return": mean_total_return,
            "annualized_return": float(group["annualized_return"].mean()),
            "sharpe": float(group["sharpe"].mean()),
            "sortino": float(group["sortino"].mean()),
            "calmar": float(group["calmar"].mean()),
            "profit_factor": float(group["profit_factor"].mean()),
            "win_rate": float(group["win_rate"].mean()),
            "max_drawdown": float(group["max_drawdown"].max()),
        }
        policy_metric_score, policy_metric_vector = _policy_metric_components(
            metric_lookup=metric_lookup,
            policy=resolved_policy,
        )
        representative_row = max(
            group.to_dict(orient="records"),
            key=lambda item: (
                float(item.get("total_return", 0.0) or 0.0),
                float(item.get("sharpe", 0.0) or 0.0),
                -float(item.get("max_drawdown", 0.0) or 0.0),
            ),
        )
        representative_run_id = str(representative_row["backtest_run_id"])
        representative_run_rows = matching_runs[
            matching_runs["backtest_run_id"] == representative_run_id
        ]
        representative_run_row = (
            representative_run_rows.iloc[0]
            if not representative_run_rows.empty
            else matching_runs.iloc[0]
        )
        preferred_metric_score = mean(
            _score_metric(metric_name, metric_lookup.get(metric_name, 0.0), drawdown_cap=resolved_policy.max_drawdown_cap)
            for metric_name in strategy_spec.ranking_metadata.preferred_metrics
        )
        params_lookup = {
            str(row["strategy_instance_id"]): _coerce_json(row.get("parameter_values_json"))
            for row in matching_runs.to_dict(orient="records")
        }
        params_dict = params_lookup.get(str(first["strategy_instance_id"]), {})

        grouped_rows.append(
            {
                "ranking_id": "",
                "campaign_run_id": str(first["campaign_run_id"]),
                "backtest_run_id": representative_run_id,
                "strategy_instance_id": str(first["strategy_instance_id"]),
                "strategy_template_id": str(first["strategy_template_id"]),
                "family_id": str(first["family_id"]),
                "family_key": str(first["family_key"]),
                "strategy_version_label": str(first["strategy_version_label"]),
                "dataset_version": str(first["dataset_version"]),
                "indicator_set_version": str(representative_run_row.get("indicator_set_version", "")),
                "derived_indicator_set_version": str(representative_run_row.get("derived_indicator_set_version", "derived-v1")),
                "contract_id": str(first["contract_id"]),
                "instrument_id": str(first["instrument_id"]),
                "timeframe": str(first["timeframe"]),
                "mean_total_return": mean_total_return,
                "params_hash": str(representative_row.get("params_hash", first["strategy_instance_id"])),
                "rank": 0,
                "family_rank": 0,
                "selected_rank": 0,
                "objective_score": policy_metric_score,
                "policy_metric_score": policy_metric_score,
                "fold_consistency_score": fold_consistency_score,
                "score_total": 0.0,
                "robust_score": 0.0,
                "ranking_policy_id": resolved_policy.policy_id,
                "policy_metric_order_json": tuple(resolved_policy.metric_order),
                "ranking_policy_json": {
                    "policy_id": resolved_policy.policy_id,
                    "metric_order": list(resolved_policy.metric_order),
                    "require_out_of_sample_pass": resolved_policy.require_out_of_sample_pass,
                    "min_trade_count": resolved_policy.min_trade_count,
                    "min_fold_count": resolved_policy.min_fold_count,
                    "max_drawdown_cap": resolved_policy.max_drawdown_cap,
                    "min_positive_fold_ratio": resolved_policy.min_positive_fold_ratio,
                    "stress_slippage_bps": resolved_policy.stress_slippage_bps,
                    "min_parameter_stability": resolved_policy.min_parameter_stability,
                    "min_slippage_score": resolved_policy.min_slippage_score,
                },
                "rank_reason_json": {
                    "policy_metric_vector": policy_metric_vector,
                    "fold_count": int(len(group)),
                    "positive_fold_ratio": positive_fold_ratio,
                    "trade_count_total": int(group["trade_count"].sum()),
                    "trade_count_min_fold": int(group["trade_count"].min()),
                    "fold_consistency_score": fold_consistency_score,
                    "parameter_values": params_dict,
                    "window_ids": tuple(str(value) for value in group["window_id"].tolist()),
                },
                "qualifies_for_projection": False,
                "window_ids_json": tuple(str(value) for value in group["window_id"].tolist()),
                "trade_count_total": int(group["trade_count"].sum()),
                "worst_max_drawdown": float(group["max_drawdown"].max()),
                "representative_backtest_run_id": representative_run_id,
                "slippage_sensitivity_score": slippage_score,
                "preferred_metric_score": preferred_metric_score,
                "created_at": _created_at(),
            }
        )

    _apply_parameter_stability(grouped_rows)
    _apply_policy_scores(grouped_rows, policy=resolved_policy)
    _assign_partition_ranks(grouped_rows)
    finding_rows = _policy_finding_rows(grouped_rows)

    resolved_output_dir = output_dir or backtest_output_dir
    output_paths: dict[str, str] = {}
    if resolved_output_dir is not None:
        output_paths = write_stage6_artifacts(
            output_dir=resolved_output_dir,
            ranking_rows=grouped_rows,
            finding_rows=finding_rows,
        )

    return {
        "ranking_policy": resolved_policy,
        "ranking_rows": grouped_rows,
        "finding_rows": finding_rows,
        "delta_manifest": results_store_contract(),
        "output_paths": output_paths,
    }


def _apply_parameter_stability(rows: list[dict[str, object]]) -> None:
    groups: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (
            str(row["campaign_run_id"]),
            str(row["family_key"]),
            str(row["contract_id"]),
            str(row["timeframe"]),
        )
        groups.setdefault(key, []).append(row)

    for group_rows in groups.values():
        params_by_instance = {
            str(row["strategy_instance_id"]): _coerce_json(row.get("rank_reason_json", {})).get("parameter_values", {})
            for row in group_rows
        }
        for row in group_rows:
            params = params_by_instance.get(str(row["strategy_instance_id"]), {})
            if not isinstance(params, dict):
                row["parameter_stability_score"] = 0.5
                row["parameter_stability_source"] = "missing_params_default"
                continue
            neighbor_returns = [
                float(other["mean_total_return"])
                for other in group_rows
                if other is not row
                and _param_distance(params, params_by_instance.get(str(other["strategy_instance_id"]), {})) == 1
            ]
            if not neighbor_returns:
                row["parameter_stability_score"] = 0.5
                row["parameter_stability_source"] = "no_one_step_neighbors_default"
                continue
            reference = mean(neighbor_returns)
            deviation = abs(float(row["mean_total_return"]) - reference)
            scale = max(abs(reference), 0.05)
            row["parameter_stability_score"] = _clip_unit(1.0 - (deviation / scale))
            row["parameter_stability_source"] = "one_step_neighbor_return_distance"


def _apply_policy_scores(rows: list[dict[str, object]], *, policy: RankingPolicy) -> None:
    for row in rows:
        reason = _coerce_json(row["rank_reason_json"])
        fold_count = int(reason.get("fold_count", 0))
        trade_count_total = int(reason.get("trade_count_total", 0))
        trade_count_min_fold = int(reason.get("trade_count_min_fold", 0))
        positive_fold_ratio = float(reason.get("positive_fold_ratio", 0.0))
        fold_consistency_score = float(reason.get("fold_consistency_score", 0.0))
        max_drawdown = float(row.get("worst_max_drawdown", 0.0) or 0.0)
        parameter_stability_score = float(row.get("parameter_stability_score", 0.5))
        slippage_score = float(row.get("slippage_sensitivity_score", 0.0))
        preferred_metric_score = float(row.get("preferred_metric_score", 0.0))
        out_of_sample_pass = (
            fold_count >= policy.min_fold_count
            and trade_count_total >= policy.min_trade_count
            and trade_count_min_fold >= max(1, min(policy.min_trade_count, trade_count_total))
            and positive_fold_ratio >= policy.min_positive_fold_ratio
            and max_drawdown <= policy.max_drawdown_cap
        )
        failure_reasons = _policy_failure_reasons(
            out_of_sample_pass=out_of_sample_pass,
            fold_count=fold_count,
            trade_count_total=trade_count_total,
            trade_count_min_fold=trade_count_min_fold,
            positive_fold_ratio=positive_fold_ratio,
            max_drawdown=max_drawdown,
            parameter_stability_score=parameter_stability_score,
            slippage_score=slippage_score,
            policy=policy,
        )
        policy_pass = (
            (out_of_sample_pass or not policy.require_out_of_sample_pass)
            and max_drawdown <= policy.max_drawdown_cap
            and parameter_stability_score >= policy.min_parameter_stability
            and slippage_score >= policy.min_slippage_score
        )
        if policy.min_parameter_stability <= 0.0 and policy.min_slippage_score <= 0.0:
            score_total = float(row["objective_score"])
        else:
            score_total = (
                (0.30 * float(row["objective_score"]))
                + (0.20 * fold_consistency_score)
                + (0.20 * parameter_stability_score)
                + (0.15 * slippage_score)
                + (0.15 * preferred_metric_score)
            )
        row["score_total"] = score_total
        row["robust_score"] = score_total
        row["qualifies_for_projection"] = bool(policy_pass)
        row["out_of_sample_pass"] = 1 if out_of_sample_pass else 0
        row["policy_pass"] = 1 if policy_pass else 0
        row["policy_failure_reasons_json"] = tuple(failure_reasons)
        reason["policy_scores"] = {
            "objective_score": float(row["objective_score"]),
            "fold_consistency_score": fold_consistency_score,
            "parameter_stability_score": parameter_stability_score,
            "parameter_stability_source": str(row.get("parameter_stability_source", "")),
            "slippage_sensitivity_score": slippage_score,
            "preferred_metric_score": preferred_metric_score,
            "score_total": score_total,
        }
        reason["policy_thresholds"] = {
            "min_fold_count": policy.min_fold_count,
            "min_trade_count": policy.min_trade_count,
            "max_drawdown_cap": policy.max_drawdown_cap,
            "min_positive_fold_ratio": policy.min_positive_fold_ratio,
            "min_parameter_stability": policy.min_parameter_stability,
            "min_slippage_score": policy.min_slippage_score,
        }
        reason["policy_failure_reasons"] = tuple(failure_reasons)
        row["rank_reason_json"] = reason


def _assign_partition_ranks(rows: list[dict[str, object]]) -> None:
    family_partitions: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (
            str(row["dataset_version"]),
            str(row["contract_id"]),
            str(row["timeframe"]),
            str(row["family_key"]),
        )
        family_partitions.setdefault(key, []).append(row)

    for group_rows in family_partitions.values():
        sorted_family_rows = sorted(
            group_rows,
            key=lambda row: (
                -int(row.get("policy_pass", 0)),
                -float(row["score_total"]),
                -float(row["objective_score"]),
                str(row["strategy_instance_id"]),
            ),
        )
        for index, row in enumerate(sorted_family_rows, start=1):
            row["family_rank"] = index
            reason = _coerce_json(row.get("rank_reason_json", {}))
            reason["family_first_ranking"] = {
                "family_rank": index,
                "family_key": row["family_key"],
            }
            row["rank_reason_json"] = reason

    partitions: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (
            str(row["dataset_version"]),
            str(row["contract_id"]),
            str(row["timeframe"]),
        )
        partitions.setdefault(key, []).append(row)

    for group_rows in partitions.values():
        sorted_rows = sorted(
            group_rows,
            key=lambda row: (
                -int(row.get("policy_pass", 0)),
                int(row.get("family_rank", 999_999) or 999_999),
                -float(row["score_total"]),
                -float(row["objective_score"]),
                str(row["strategy_instance_id"]),
            ),
        )
        for index, row in enumerate(sorted_rows, start=1):
            row["rank"] = index
            row["selected_rank"] = index
            row["ranking_id"] = "RANK-" + _stable_hash(
                "|".join(
                    (
                        str(row["campaign_run_id"]),
                        str(row["strategy_instance_id"]),
                        str(row["contract_id"]),
                        str(row["timeframe"]),
                    )
                )
            )


def _policy_finding_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    finding_rows: list[dict[str, object]] = []
    for row in rows:
        if int(row.get("policy_pass", 0) or 0) == 1:
            continue
        reason = _coerce_json(row.get("rank_reason_json", {}))
        finding_type = "ranking_policy_reject"
        campaign_run_id = str(row.get("campaign_run_id", ""))
        backtest_run_id = str(row.get("backtest_run_id", row.get("representative_backtest_run_id", "")))
        ranking_id = str(row.get("ranking_id", ""))
        finding_rows.append(
            {
                "finding_id": _finding_id(
                    campaign_run_id=campaign_run_id,
                    backtest_run_id=backtest_run_id,
                    finding_type=finding_type,
                    ranking_id=ranking_id,
                ),
                "campaign_run_id": campaign_run_id,
                "backtest_run_id": backtest_run_id,
                "strategy_instance_id": str(row.get("strategy_instance_id", "")),
                "finding_type": finding_type,
                "severity": "warning",
                "summary": (
                    "strategy instance did not pass ranking policy "
                    f"{row.get('ranking_policy_id', '')}"
                ),
                "evidence_json": {
                    "ranking_id": ranking_id,
                    "policy_pass": bool(row.get("policy_pass", False)),
                    "out_of_sample_pass": bool(row.get("out_of_sample_pass", False)),
                    "policy_failure_reasons": row.get("policy_failure_reasons_json", ()),
                    "parameter_stability_score": row.get("parameter_stability_score"),
                    "slippage_sensitivity_score": row.get("slippage_sensitivity_score"),
                    "score_total": row.get("score_total"),
                    "objective_score": row.get("objective_score"),
                    "rank_reason": reason,
                },
                "created_at": _created_at(),
            }
        )
    return finding_rows
