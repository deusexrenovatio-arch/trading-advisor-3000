from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median

import pandas as pd

from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_phase1_strategy_registry

from .results import load_backtest_artifacts, phase6_results_store_contract, write_stage6_artifacts


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clip_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


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
    gross_pnl = sum(float(row.get("pnl", 0.0) or 0.0) for row in rows)
    extra_cost = sum(
        (
            (abs(float(row.get("entry_price", 0.0) or 0.0)) + abs(float(row.get("exit_price", 0.0) or 0.0)))
            * abs(float(row.get("size", 0.0) or 0.0))
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


@dataclass(frozen=True)
class RankingPolicy:
    policy_id: str
    metric_order: tuple[str, ...]
    require_out_of_sample_pass: bool = True
    min_trade_count: int = 4
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
        if self.max_drawdown_cap <= 0.0:
            raise ValueError("max_drawdown_cap must be positive")
        if self.stress_slippage_bps < 0.0:
            raise ValueError("stress_slippage_bps must be non-negative")


def default_ranking_policy() -> RankingPolicy:
    return RankingPolicy(
        policy_id="robust_oos_v1",
        metric_order=("total_return", "profit_factor", "max_drawdown"),
    )


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
    registry = strategy_registry or build_phase1_strategy_registry()
    if backtest_output_dir is not None and any(rows is None for rows in (batch_rows, run_rows, stat_rows, trade_rows)):
        artifacts = load_backtest_artifacts(backtest_output_dir)
        batch_rows = batch_rows if batch_rows is not None else artifacts["research_backtest_batches"]
        run_rows = run_rows if run_rows is not None else artifacts["research_backtest_runs"]
        stat_rows = stat_rows if stat_rows is not None else artifacts["research_strategy_stats"]
        trade_rows = trade_rows if trade_rows is not None else artifacts["research_trade_records"]

    batch_rows = batch_rows or []
    run_rows = run_rows or []
    stat_rows = stat_rows or []
    trade_rows = trade_rows or []
    if not run_rows or not stat_rows:
        raise ValueError("ranking requires backtest run rows and strategy stat rows")

    run_frame = pd.DataFrame(run_rows)
    stat_frame = pd.DataFrame(stat_rows)
    trade_frame = pd.DataFrame(trade_rows)
    run_frame["params_dict"] = run_frame["params_json"].map(_coerce_json)
    stat_frame["params_dict"] = stat_frame["params_hash"].map(
        {
            row["params_hash"]: _coerce_json(row.get("params_json"))
            for row in run_rows
            if isinstance(row, dict) and row.get("params_hash")
        }
    )

    trades_by_run: dict[str, list[dict[str, object]]] = {}
    for row in trade_rows:
        run_id = str(row.get("backtest_run_id", ""))
        trades_by_run.setdefault(run_id, []).append(row)

    grouped_rows: list[dict[str, object]] = []
    group_columns = [
        "backtest_batch_id",
        "strategy_version",
        "strategy_family",
        "dataset_version",
        "indicator_set_version",
        "feature_set_version",
        "contract_id",
        "instrument_id",
        "timeframe",
        "params_hash",
    ]

    for _, group in stat_frame.groupby(group_columns, dropna=False, sort=True):
        first = group.iloc[0]
        params_hash = str(first["params_hash"])
        matching_runs = run_frame[
            (run_frame["backtest_batch_id"] == first["backtest_batch_id"])
            & (run_frame["strategy_version"] == first["strategy_version"])
            & (run_frame["contract_id"] == first["contract_id"])
            & (run_frame["instrument_id"] == first["instrument_id"])
            & (run_frame["timeframe"] == first["timeframe"])
            & (run_frame["params_hash"] == params_hash)
        ]
        params_dict = _coerce_json(matching_runs.iloc[0]["params_json"]) if not matching_runs.empty else {}
        representative_row = group.sort_values("total_return", ascending=False).iloc[0]
        representative_run_id = str(representative_row["backtest_run_id"])

        stressed_runs = [
            _trade_stress(trades_by_run.get(str(run_id), []), extra_slippage_bps=resolved_policy.stress_slippage_bps)
            for run_id in group["backtest_run_id"].tolist()
        ]
        stressed_pnl = mean(item[0] for item in stressed_runs) if stressed_runs else 0.0
        slippage_score = mean(item[1] for item in stressed_runs) if stressed_runs else 0.0

        mean_total_return = float(group["total_return"].mean())
        total_return_std = float(group["total_return"].std(ddof=0)) if len(group) > 1 else 0.0
        positive_fold_ratio = float((group["total_return"] > 0.0).mean())
        fold_consistency_score = _clip_unit(
            positive_fold_ratio * (1.0 / (1.0 + (total_return_std / max(abs(mean_total_return), 0.02))))
        )

        strategy_spec = registry.get(str(first["strategy_version"]))
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
        preferred_metric_score = mean(
            _score_metric(metric_name, metric_lookup.get(metric_name, 0.0), drawdown_cap=resolved_policy.max_drawdown_cap)
            for metric_name in strategy_spec.ranking_metadata.preferred_metrics
        )

        grouped_rows.append(
            {
                "ranking_id": "",
                "backtest_batch_id": str(first["backtest_batch_id"]),
                "ranking_policy_id": resolved_policy.policy_id,
                "selected_rank": 0,
                "representative_backtest_run_id": representative_run_id,
                "strategy_version": str(first["strategy_version"]),
                "strategy_family": str(first["strategy_family"]),
                "dataset_version": str(first["dataset_version"]),
                "indicator_set_version": str(first["indicator_set_version"]),
                "feature_set_version": str(first["feature_set_version"]),
                "contract_id": str(first["contract_id"]),
                "instrument_id": str(first["instrument_id"]),
                "timeframe": str(first["timeframe"]),
                "params_hash": params_hash,
                "params_json": params_dict,
                "fold_count": int(len(group)),
                "window_ids_json": tuple(str(value) for value in group["window_id"].tolist()),
                "trade_count_total": int(group["trade_count"].sum()),
                "trade_count_min_fold": int(group["trade_count"].min()),
                "positive_fold_ratio": positive_fold_ratio,
                "mean_total_return": mean_total_return,
                "median_total_return": float(group["total_return"].median()),
                "mean_annualized_return": float(group["annualized_return"].mean()),
                "mean_sharpe": float(group["sharpe"].mean()),
                "mean_sortino": float(group["sortino"].mean()),
                "mean_calmar": float(group["calmar"].mean()),
                "mean_profit_factor": float(group["profit_factor"].mean()),
                "mean_win_rate": float(group["win_rate"].mean()),
                "worst_max_drawdown": float(group["max_drawdown"].max()),
                "slippage_stressed_pnl": stressed_pnl,
                "slippage_sensitivity_score": slippage_score,
                "fold_consistency_score": fold_consistency_score,
                "parameter_stability_score": 0.5,
                "preferred_metric_score": preferred_metric_score,
                "robust_score": 0.0,
                "out_of_sample_pass": 0,
                "policy_pass": 0,
                "created_at": _created_at(),
            }
        )

    _apply_parameter_stability(grouped_rows)
    _apply_policy_scores(grouped_rows, policy=resolved_policy)
    _assign_partition_ranks(grouped_rows)

    resolved_output_dir = output_dir or backtest_output_dir
    output_paths: dict[str, str] = {}
    if resolved_output_dir is not None:
        output_paths = write_stage6_artifacts(
            output_dir=resolved_output_dir,
            ranking_rows=grouped_rows,
        )

    return {
        "ranking_policy": resolved_policy,
        "ranking_rows": grouped_rows,
        "delta_manifest": phase6_results_store_contract(),
        "output_paths": output_paths,
    }


def _apply_parameter_stability(rows: list[dict[str, object]]) -> None:
    groups: dict[tuple[str, str, str, str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (
            str(row["backtest_batch_id"]),
            str(row["strategy_version"]),
            str(row["contract_id"]),
            str(row["instrument_id"]),
            str(row["timeframe"]),
        )
        groups.setdefault(key, []).append(row)

    for group_rows in groups.values():
        for row in group_rows:
            params = _coerce_json(row["params_json"])
            neighbor_returns = [
                float(other["mean_total_return"])
                for other in group_rows
                if other is not row and _param_distance(params, _coerce_json(other["params_json"])) == 1
            ]
            if not neighbor_returns:
                row["parameter_stability_score"] = 0.5
                continue
            reference = mean(neighbor_returns)
            scale = max(abs(float(row["mean_total_return"])), abs(reference), 0.05)
            row["parameter_stability_score"] = _clip_unit(
                1.0 - (abs(float(row["mean_total_return"]) - reference) / (2.0 * scale))
            )


def _apply_policy_scores(rows: list[dict[str, object]], *, policy: RankingPolicy) -> None:
    for row in rows:
        trade_count_score = _clip_unit(float(row["trade_count_total"]) / float(max(policy.min_trade_count * 2, 1)))
        drawdown_score = _score_metric(
            "max_drawdown",
            float(row["worst_max_drawdown"]),
            drawdown_cap=policy.max_drawdown_cap,
        )
        out_of_sample_pass = (
            not policy.require_out_of_sample_pass
            or (float(row["mean_total_return"]) > 0.0 and float(row["preferred_metric_score"]) >= 0.55)
        )
        policy_pass = (
            out_of_sample_pass
            and int(row["trade_count_total"]) >= policy.min_trade_count
            and float(row["worst_max_drawdown"]) <= policy.max_drawdown_cap
            and float(row["positive_fold_ratio"]) >= policy.min_positive_fold_ratio
            and float(row["slippage_sensitivity_score"]) >= policy.min_slippage_score
            and float(row["parameter_stability_score"]) >= policy.min_parameter_stability
        )
        robust_score = (
            0.35 * float(row["preferred_metric_score"])
            + 0.20 * float(row["fold_consistency_score"])
            + 0.15 * float(row["slippage_sensitivity_score"])
            + 0.10 * float(row["parameter_stability_score"])
            + 0.10 * trade_count_score
            + 0.10 * drawdown_score
        )
        row["robust_score"] = round(robust_score, 6)
        row["out_of_sample_pass"] = 1 if out_of_sample_pass else 0
        row["policy_pass"] = 1 if policy_pass else 0
        row["ranking_id"] = "RANK-" + _stable_hash(
            "|".join(
                (
                    str(row["backtest_batch_id"]),
                    policy.policy_id,
                    str(row["strategy_version"]),
                    str(row["contract_id"]),
                    str(row["timeframe"]),
                    str(row["params_hash"]),
                )
            )
        )


def _assign_partition_ranks(rows: list[dict[str, object]]) -> None:
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (
            str(row["backtest_batch_id"]),
            str(row["dataset_version"]),
            str(row["contract_id"]),
            str(row["timeframe"]),
        )
        grouped.setdefault(key, []).append(row)

    for group_rows in grouped.values():
        ordered = sorted(
            group_rows,
            key=lambda item: (
                int(item["policy_pass"]),
                float(item["robust_score"]),
                float(item["preferred_metric_score"]),
                float(item["mean_total_return"]),
                -float(item["worst_max_drawdown"]),
            ),
            reverse=True,
        )
        for index, row in enumerate(ordered, start=1):
            row["selected_rank"] = index
