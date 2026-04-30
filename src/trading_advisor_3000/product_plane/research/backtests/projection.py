from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import DecisionCandidate, IndicatorContextRef, Mode, Timeframe, TradeSide
from trading_advisor_3000.product_plane.research.ids import candidate_id
from trading_advisor_3000.product_plane.research.io import ResearchFrameCache, ResearchSliceRequest, load_backtest_frames
from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_strategy_registry

from .engine import BacktestEngineConfig, project_family_candidate, strategy_spec_to_search_spec
from .input_requirements import loader_columns_for_search_specs
from .results import results_store_contract, write_stage6_artifacts


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clip_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _coerce_json(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        payload = json.loads(value)
        if isinstance(payload, dict):
            return {str(key): item for key, item in payload.items()}
    return {}


def supported_selection_policies() -> tuple[str, ...]:
    return (
        "top_robust_per_series",
        "top_policy_per_series",
        "top_by_family_per_series",
        "all_policy_pass",
    )


@dataclass(frozen=True)
class CandidateProjectionRequest:
    backtest_run_id: str | None = None
    ranking_policy_id: str = "robust_oos_v1"
    selection_policy: str = "top_robust_per_series"
    runtime_contract: str = "DecisionCandidate"
    max_candidates_per_partition: int = 1
    min_robust_score: float = 0.55
    decision_lag_bars_max: int = 1

    def __post_init__(self) -> None:
        if not self.selection_policy.strip():
            raise ValueError("selection_policy must be non-empty")
        if self.selection_policy not in supported_selection_policies():
            raise ValueError(f"unsupported selection_policy: {self.selection_policy}")
        if self.max_candidates_per_partition <= 0:
            raise ValueError("max_candidates_per_partition must be positive")
        if self.decision_lag_bars_max < 0:
            raise ValueError("decision_lag_bars_max must be non-negative")


def project_runtime_candidates(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    derived_indicator_output_dir: Path,
    output_dir: Path,
    request: CandidateProjectionRequest,
    ranking_rows: list[dict[str, object]] | None = None,
    strategy_registry: StrategyRegistry | None = None,
    cache: ResearchFrameCache | None = None,
    config: BacktestEngineConfig | None = None,
) -> dict[str, object]:
    registry = strategy_registry or build_strategy_registry()
    engine_config = config or BacktestEngineConfig()
    if ranking_rows is None:
        raise ValueError("projection requires explicit ranking rows from the Delta-backed orchestration layer")
    if not ranking_rows:
        raise ValueError("projection requires accepted Optuna trial rows or ranking rows")

    selected_rows = _select_rows(ranking_rows, request=request)
    projected_rows: list[dict[str, object]] = []
    contracts: list[dict[str, object]] = []

    for ranking_row in selected_rows:
        dataset_version = str(ranking_row["dataset_version"])
        indicator_set_version = str(ranking_row.get("indicator_set_version", ""))
        derived_indicator_set_version = str(ranking_row.get("derived_indicator_set_version", "derived-v1"))
        contract_id = str(ranking_row["contract_id"])
        instrument_id = str(ranking_row["instrument_id"])
        timeframe = str(ranking_row["timeframe"])
        strategy_spec = registry.get(str(ranking_row["strategy_version_label"]))
        search_spec = strategy_spec_to_search_spec(
            strategy_spec,
            template_key=str(ranking_row.get("template_key", ranking_row.get("strategy_template_id", "")) or strategy_spec.signal_builder_key),
        )
        input_columns = loader_columns_for_search_specs((search_spec,))
        required_timeframes = {
            str(payload.get("timeframe"))
            for payload in search_spec.required_inputs_by_clock.values()
            if isinstance(payload, dict)
            and any(payload.get(key) for key in ("price_inputs", "materialized_indicators", "materialized_derived"))
            and payload.get("timeframe")
        }
        input_columns = loader_columns_for_search_specs((search_spec,))
        series_frames, _, _ = load_backtest_frames(
            dataset_output_dir=dataset_output_dir,
            indicator_output_dir=indicator_output_dir,
            derived_indicator_output_dir=derived_indicator_output_dir,
            request=ResearchSliceRequest(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                derived_indicator_set_version=derived_indicator_set_version,
                timeframe="" if len(required_timeframes) > 1 else timeframe,
                contract_ids=(contract_id,),
                instrument_ids=(instrument_id,),
                analysis_only=True,
                price_columns=input_columns.price_columns,
                indicator_columns=input_columns.indicator_columns,
                derived_columns=input_columns.derived_columns,
            ),
            cache=cache,
        )
        if not series_frames:
            continue

        params = _coerce_json(_coerce_json(ranking_row.get("rank_reason_json")).get("parameter_values", {}))
        projection = project_family_candidate(
            series=series_frames if len(required_timeframes) > 1 else series_frames[0],
            search_spec=search_spec,
            params=params,
            config=engine_config,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            decision_lag_bars_max=request.decision_lag_bars_max,
        )
        if projection is None:
            continue

        indicator_context = IndicatorContextRef.from_dict(projection["indicator_context"])
        confidence = _clip_unit((0.65 * float(ranking_row["score_total"])) + (0.35 * float(projection["signal_strength_score"])))
        signal_id = "SIG-" + _stable_hash(
            "|".join(
                (
                    str(ranking_row["strategy_instance_id"]),
                    contract_id,
                    timeframe,
                    str(projection["ts_decision"]),
                )
            )
        )
        candidate_contract = DecisionCandidate(
            signal_id=signal_id,
            contract_id=contract_id,
            timeframe=Timeframe(timeframe),
            strategy_version_id=str(ranking_row["strategy_version_label"]),
            mode=Mode.SHADOW,
            side=TradeSide(str(projection["side"])),
            entry_ref=float(projection["entry_ref"]),
            stop_ref=float(projection["stop_ref"]),
            target_ref=float(projection["target_ref"]),
            confidence=confidence,
            ts_decision=str(projection["ts_decision"]),
            indicator_context=indicator_context,
        )
        contracts.append(candidate_contract.to_dict())
        projected_rows.append(
            {
                "candidate_id": candidate_id(
                    strategy_instance_id=str(ranking_row["strategy_instance_id"]),
                    contract_id=candidate_contract.contract_id,
                    timeframe=candidate_contract.timeframe.value,
                    ts_signal=candidate_contract.ts_decision,
                ),
                "campaign_run_id": str(ranking_row["campaign_run_id"]),
                "ranking_id": str(ranking_row["ranking_id"]),
                "backtest_run_id": str(ranking_row["backtest_run_id"]),
                "strategy_instance_id": str(ranking_row["strategy_instance_id"]),
                "strategy_template_id": str(ranking_row["strategy_template_id"]),
                "family_id": str(ranking_row["family_id"]),
                "family_key": str(ranking_row["family_key"]),
                "signal_id": candidate_contract.signal_id,
                "contract_id": contract_id,
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "ts_signal": candidate_contract.ts_decision,
                "side": candidate_contract.side.value,
                "entry_ref": candidate_contract.entry_ref,
                "stop_ref": candidate_contract.stop_ref,
                "target_ref": candidate_contract.target_ref,
                "score": candidate_contract.confidence,
                "estimated_commission": 0.0,
                "estimated_slippage": 0.0,
                "window_id": str(ranking_row.get("window_ids_json", ["wf-01"])[0]) if ranking_row.get("window_ids_json") else "wf-01",
                "indicator_context_json": candidate_contract.indicator_context.to_dict(),
                "created_at": _created_at(),
            }
        )

    output_paths = write_stage6_artifacts(
        output_dir=output_dir,
        ranking_rows=ranking_rows,
        candidate_rows=projected_rows,
    )
    return {
        "projection_request": request,
        "candidate_rows": projected_rows,
        "candidate_contracts": contracts,
        "delta_manifest": results_store_contract(),
        "output_paths": output_paths,
    }


def _optimizer_selection_rows(artifacts: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    studies = artifacts.get("research_optimizer_studies", [])
    trials = artifacts.get("research_optimizer_trials", [])
    runs = artifacts.get("research_backtest_runs", [])
    if not studies or not trials or not runs:
        return []

    studies_by_id = {
        str(row.get("optimizer_study_id", "")): row
        for row in studies
        if row.get("optimizer_study_id")
    }
    runs_by_campaign_param: dict[tuple[str, str], list[dict[str, object]]] = {}
    runs_by_param: dict[str, list[dict[str, object]]] = {}
    for row in runs:
        param_hash = str(row.get("param_hash", row.get("params_hash", "")))
        campaign_run_id = str(row.get("campaign_run_id", ""))
        if not param_hash:
            continue
        runs_by_campaign_param.setdefault((campaign_run_id, param_hash), []).append(row)
        runs_by_param.setdefault(param_hash, []).append(row)

    selected_rows: list[dict[str, object]] = []
    for trial in trials:
        status = str(trial.get("status", ""))
        if status not in {"completed", "duplicate"} or not bool(trial.get("constraints_passed", False)):
            continue
        study = studies_by_id.get(str(trial.get("optimizer_study_id", "")), {})
        campaign_run_id = str(study.get("campaign_run_id", ""))
        param_hash = str(trial.get("param_hash", ""))
        matching_runs = runs_by_campaign_param.get((campaign_run_id, param_hash), runs_by_param.get(param_hash, []))
        if not matching_runs:
            continue
        components = _coerce_json(trial.get("objective_components_json", {}))
        study_config = _coerce_json(study.get("study_config_json", {}))
        ranking_policy = _coerce_json(study_config.get("ranking_policy", {}))
        policy_id = str(ranking_policy.get("policy_id", "robust_oos_v1"))
        params = _coerce_json(trial.get("params_json", {}))
        grouped_runs: dict[tuple[str, str, str], list[dict[str, object]]] = {}
        for run in matching_runs:
            key = (
                str(run.get("contract_id", "")),
                str(run.get("instrument_id", "")),
                str(run.get("timeframe", "")),
            )
            grouped_runs.setdefault(key, []).append(run)
        for (contract_id, instrument_id, timeframe), group in sorted(grouped_runs.items()):
            representative = sorted(group, key=lambda row: str(row.get("window_id", "")))[0]
            strategy_version_label = str(
                representative.get(
                    "strategy_version_label",
                    representative.get("strategy_version", study.get("family_key", "")),
                )
            )
            family_key = str(representative.get("family_key", representative.get("strategy_family", study.get("family_key", ""))))
            template_key = str(study.get("template_key", representative.get("strategy_template_id", "")))
            ranking_id = "OPTSEL-" + _stable_hash(
                "|".join(
                    (
                        str(trial.get("optimizer_trial_id", "")),
                        contract_id,
                        instrument_id,
                        timeframe,
                    )
                )
            )
            selected_rows.append(
                {
                    "ranking_id": ranking_id,
                    "ranking_policy_id": policy_id,
                    "campaign_run_id": campaign_run_id or str(representative.get("campaign_run_id", "")),
                    "backtest_run_id": str(representative.get("backtest_run_id", "")),
                    "representative_backtest_run_id": str(representative.get("backtest_run_id", "")),
                    "strategy_instance_id": str(representative.get("strategy_instance_id", f"sinst_{param_hash}")),
                    "strategy_template_id": str(representative.get("strategy_template_id", template_key)),
                    "template_key": template_key,
                    "family_id": str(representative.get("family_id", f"sfam_{family_key}")),
                    "family_key": family_key,
                    "strategy_version_label": strategy_version_label,
                    "dataset_version": str(representative.get("dataset_version", "")),
                    "indicator_set_version": str(representative.get("indicator_set_version", "")),
                    "derived_indicator_set_version": str(representative.get("derived_indicator_set_version", "derived-v1")),
                    "contract_id": contract_id,
                    "instrument_id": instrument_id,
                    "timeframe": timeframe,
                    "params_hash": param_hash,
                    "rank": int(trial.get("trial_number", 0) or 0),
                    "family_rank": int(trial.get("trial_number", 0) or 0),
                    "selected_rank": int(trial.get("trial_number", 0) or 0),
                    "objective_score": float(components.get("objective_score", trial.get("value", 0.0)) or 0.0),
                    "policy_metric_score": float(components.get("objective_score", trial.get("value", 0.0)) or 0.0),
                    "score_total": float(components.get("score_total", trial.get("value", 0.0)) or 0.0),
                    "robust_score": float(components.get("score_total", trial.get("value", 0.0)) or 0.0),
                    "qualifies_for_projection": True,
                    "policy_pass": 1,
                    "rank_reason_json": {
                        "parameter_values": params,
                        "optimizer_trial_id": str(trial.get("optimizer_trial_id", "")),
                        "optimizer_study_id": str(trial.get("optimizer_study_id", "")),
                        "constraint_names": components.get("constraint_names", ()),
                        "constraint_values": components.get("constraint_values", ()),
                        "selection_owner": components.get("selection_owner", "optuna.study"),
                        "window_ids": tuple(str(row.get("window_id", "")) for row in group if row.get("window_id")),
                    },
                    "window_ids_json": tuple(str(row.get("window_id", "")) for row in group if row.get("window_id")),
                    "created_at": _created_at(),
                }
            )
    return selected_rows


def _select_rows(
    ranking_rows: list[dict[str, object]],
    *,
    request: CandidateProjectionRequest,
) -> list[dict[str, object]]:
    filtered = [
        row
        for row in ranking_rows
        if str(row.get("ranking_policy_id")) == request.ranking_policy_id
        and bool(row.get("qualifies_for_projection", row.get("policy_pass", False)))
        and float(row.get("score_total", row.get("robust_score", 0.0)) or 0.0) >= request.min_robust_score
    ]
    if request.backtest_run_id:
        filtered = [
            row
            for row in filtered
            if str(row.get("backtest_run_id", row.get("representative_backtest_run_id", ""))) == request.backtest_run_id
        ]
    filtered = _sorted_rows(filtered, selection_policy=request.selection_policy)
    if request.selection_policy == "all_policy_pass":
        return filtered

    selected: list[dict[str, object]] = []
    counts: dict[tuple[str, str, str], int] = {}
    family_counts: dict[tuple[str, str, str, str], int] = {}
    for row in filtered:
        series_key = (
            str(row.get("dataset_version", "")),
            str(row.get("contract_id", "")),
            str(row.get("timeframe", "")),
        )
        current = counts.get(series_key, 0)
        if current >= request.max_candidates_per_partition:
            continue
        if request.selection_policy == "top_by_family_per_series":
            family_key = (*series_key, str(row.get("family_key", row.get("strategy_family", ""))))
            if family_counts.get(family_key, 0) >= 1:
                continue
            family_counts[family_key] = 1
        counts[series_key] = current + 1
        selected.append(row)
    return selected


def _sorted_rows(rows: list[dict[str, object]], *, selection_policy: str) -> list[dict[str, object]]:
    if selection_policy == "top_policy_per_series":
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("dataset_version", "")),
                str(row.get("contract_id", "")),
                str(row.get("timeframe", "")),
                -float(row.get("objective_score", row.get("policy_metric_score", 0.0)) or 0.0),
                int(row.get("rank", row.get("selected_rank", 0))),
                -float(row.get("score_total", row.get("robust_score", 0.0)) or 0.0),
            ),
        )
    if selection_policy == "top_by_family_per_series":
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("dataset_version", "")),
                str(row.get("contract_id", "")),
                str(row.get("timeframe", "")),
                str(row.get("family_key", row.get("strategy_family", ""))),
                int(row.get("rank", row.get("selected_rank", 0))),
                -float(row.get("score_total", row.get("robust_score", 0.0)) or 0.0),
            ),
        )
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("dataset_version", "")),
            str(row.get("contract_id", "")),
            str(row.get("timeframe", "")),
            int(row.get("rank", row.get("selected_rank", 0))),
            -float(row.get("score_total", row.get("robust_score", 0.0)) or 0.0),
        ),
    )
