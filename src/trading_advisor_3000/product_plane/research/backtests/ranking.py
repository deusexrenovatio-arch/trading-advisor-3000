from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean

import pandas as pd

from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_strategy_registry

from .results import load_backtest_artifacts, results_store_contract, write_stage6_artifacts


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clip_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _finding_id(*, campaign_run_id: str, backtest_run_id: str, finding_type: str) -> str:
    return "FIND-" + _stable_hash(f"{campaign_run_id}|{backtest_run_id}|{finding_type}")


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
    registry = strategy_registry or build_strategy_registry()
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

    trades_by_run: dict[str, list[dict[str, object]]] = {}
    for row in trade_rows:
        run_id = str(row.get("backtest_run_id", ""))
        trades_by_run.setdefault(run_id, []).append(row)

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
            _trade_stress(trades_by_run.get(str(run_id), []), extra_slippage_bps=resolved_policy.stress_slippage_bps)
            for run_id in group["backtest_run_id"].tolist()
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
                "selected_rank": 0,
                "objective_score": policy_metric_score,
                "policy_metric_score": policy_metric_score,
                "score_total": 0.0,
                "robust_score": 0.0,
                "ranking_policy_id": resolved_policy.policy_id,
                "policy_metric_order_json": tuple(resolved_policy.metric_order),
                "ranking_policy_json": {
                    "policy_id": resolved_policy.policy_id,
                    "metric_order": list(resolved_policy.metric_order),
                    "require_out_of_sample_pass": resolved_policy.require_out_of_sample_pass,
                    "min_trade_count": resolved_policy.min_trade_count,
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
                continue
            neighbor_returns = [
                float(other["mean_total_return"])
                for other in group_rows
                if other is not row
                and _param_distance(params, params_by_instance.get(str(other["strategy_instance_id"]), {})) == 1
            ]
            if not neighbor_returns:
                row["parameter_stability_score"] = 0.5
                continue
            reference = mean(neighbor_returns)
            deviation = abs(float(row["mean_total_return"]) - reference)
            scale = max(abs(reference), 0.05)
            row["parameter_stability_score"] = _clip_unit(1.0 - (deviation / scale))


def _apply_policy_scores(rows: list[dict[str, object]], *, policy: RankingPolicy) -> None:
    for row in rows:
        reason = _coerce_json(row["rank_reason_json"])
        trade_count_total = int(reason.get("trade_count_total", 0))
        trade_count_min_fold = int(reason.get("trade_count_min_fold", 0))
        positive_fold_ratio = float(reason.get("positive_fold_ratio", 0.0))
        fold_consistency_score = float(reason.get("fold_consistency_score", 0.0))
        parameter_stability_score = float(row.get("parameter_stability_score", 0.5))
        slippage_score = float(row.get("slippage_sensitivity_score", 0.0))
        preferred_metric_score = float(row.get("preferred_metric_score", 0.0))
        out_of_sample_pass = (
            trade_count_total >= policy.min_trade_count
            and trade_count_min_fold >= max(1, min(policy.min_trade_count, trade_count_total))
            and positive_fold_ratio >= policy.min_positive_fold_ratio
        )
        policy_pass = (
            (out_of_sample_pass or not policy.require_out_of_sample_pass)
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


def _assign_partition_ranks(rows: list[dict[str, object]]) -> None:
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
        finding_rows.append(
            {
                "finding_id": _finding_id(
                    campaign_run_id=campaign_run_id,
                    backtest_run_id=backtest_run_id,
                    finding_type=finding_type,
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
                    "ranking_id": row.get("ranking_id"),
                    "policy_pass": bool(row.get("policy_pass", False)),
                    "out_of_sample_pass": bool(row.get("out_of_sample_pass", False)),
                    "score_total": row.get("score_total"),
                    "objective_score": row.get("objective_score"),
                    "rank_reason": reason,
                },
                "created_at": _created_at(),
            }
        )
    return finding_rows
