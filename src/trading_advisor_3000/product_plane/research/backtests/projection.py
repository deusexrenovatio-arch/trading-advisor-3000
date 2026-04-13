from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import DecisionCandidate, FeatureSnapshotRef, Mode, Timeframe, TradeSide
from trading_advisor_3000.product_plane.research.ids import candidate_id
from trading_advisor_3000.product_plane.research.io import ResearchFrameCache, ResearchSliceRequest, load_backtest_frames
from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_phase1_strategy_registry

from .engine import BacktestEngineConfig, project_series_candidate
from .results import load_backtest_artifacts, phase6_results_store_contract, write_stage6_artifacts


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
    feature_output_dir: Path,
    output_dir: Path,
    request: CandidateProjectionRequest,
    ranking_rows: list[dict[str, object]] | None = None,
    strategy_registry: StrategyRegistry | None = None,
    cache: ResearchFrameCache | None = None,
    config: BacktestEngineConfig | None = None,
) -> dict[str, object]:
    registry = strategy_registry or build_phase1_strategy_registry()
    engine_config = config or BacktestEngineConfig()
    if ranking_rows is None:
        ranking_rows = load_backtest_artifacts(output_dir)["research_strategy_rankings"]
    if not ranking_rows:
        raise ValueError("projection requires ranking rows")

    selected_rows = _select_rows(ranking_rows, request=request)
    projected_rows: list[dict[str, object]] = []
    contracts: list[dict[str, object]] = []

    for ranking_row in selected_rows:
        params = _coerce_json(ranking_row.get("params_json"))
        dataset_version = str(ranking_row["dataset_version"])
        indicator_set_version = str(ranking_row["indicator_set_version"])
        feature_set_version = str(ranking_row["feature_set_version"])
        contract_id = str(ranking_row["contract_id"])
        instrument_id = str(ranking_row["instrument_id"])
        timeframe = str(ranking_row["timeframe"])
        series_frames, _, _ = load_backtest_frames(
            dataset_output_dir=dataset_output_dir,
            indicator_output_dir=indicator_output_dir,
            feature_output_dir=feature_output_dir,
            request=ResearchSliceRequest(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                feature_set_version=feature_set_version,
                timeframe=timeframe,
                contract_ids=(contract_id,),
                instrument_ids=(instrument_id,),
                analysis_only=True,
            ),
            cache=cache,
        )
        if not series_frames:
            continue

        strategy_spec = registry.get(str(ranking_row["strategy_version"]))
        projection = project_series_candidate(
            series=series_frames[0],
            strategy_spec=strategy_spec,
            params=params,
            config=engine_config,
            dataset_version=dataset_version,
            feature_set_version=feature_set_version,
            decision_lag_bars_max=request.decision_lag_bars_max,
        )
        if projection is None:
            continue

        feature_snapshot = FeatureSnapshotRef.from_dict(projection["feature_snapshot"])
        confidence = _clip_unit((0.65 * float(ranking_row["robust_score"])) + (0.35 * float(projection["signal_strength_score"])))
        signal_id = "SIG-" + _stable_hash(
            "|".join(
                (
                    str(ranking_row["strategy_version"]),
                    contract_id,
                    timeframe,
                    str(projection["ts_decision"]),
                    str(ranking_row["params_hash"]),
                )
            )
        )
        candidate_contract = DecisionCandidate(
            signal_id=signal_id,
            contract_id=contract_id,
            timeframe=Timeframe(timeframe),
            strategy_version_id=str(ranking_row["strategy_version"]),
            mode=Mode.SHADOW,
            side=TradeSide(str(projection["side"])),
            entry_ref=float(projection["entry_ref"]),
            stop_ref=float(projection["stop_ref"]),
            target_ref=float(projection["target_ref"]),
            confidence=confidence,
            ts_decision=str(projection["ts_decision"]),
            feature_snapshot=feature_snapshot,
        )
        contracts.append(candidate_contract.to_dict())
        projected_rows.append(
            {
                "candidate_id": candidate_id(
                    strategy_version_id=candidate_contract.strategy_version_id,
                    contract_id=candidate_contract.contract_id,
                    timeframe=candidate_contract.timeframe.value,
                    ts_signal=candidate_contract.ts_decision,
                ),
                "signal_id": candidate_contract.signal_id,
                "backtest_batch_id": str(ranking_row["backtest_batch_id"]),
                "ranking_policy_id": request.ranking_policy_id,
                "selection_policy": request.selection_policy,
                "selected_rank": int(ranking_row["selected_rank"]),
                "source_backtest_run_id": str(ranking_row["representative_backtest_run_id"]),
                "strategy_version_id": candidate_contract.strategy_version_id,
                "strategy_family": str(ranking_row["strategy_family"]),
                "dataset_version": dataset_version,
                "indicator_set_version": indicator_set_version,
                "feature_set_version": feature_set_version,
                "contract_id": contract_id,
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "mode": candidate_contract.mode.value,
                "side": candidate_contract.side.value,
                "entry_ref": candidate_contract.entry_ref,
                "stop_ref": candidate_contract.stop_ref,
                "target_ref": candidate_contract.target_ref,
                "confidence": candidate_contract.confidence,
                "ts_decision": candidate_contract.ts_decision,
                "params_hash": str(ranking_row["params_hash"]),
                "params_json": params,
                "robust_score": float(ranking_row["robust_score"]),
                "signal_strength_score": float(projection["signal_strength_score"]),
                "feature_snapshot_json": candidate_contract.feature_snapshot.to_dict(),
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
        "delta_manifest": phase6_results_store_contract(),
        "output_paths": output_paths,
    }


def _select_rows(
    ranking_rows: list[dict[str, object]],
    *,
    request: CandidateProjectionRequest,
) -> list[dict[str, object]]:
    filtered = [
        row
        for row in ranking_rows
        if str(row.get("ranking_policy_id")) == request.ranking_policy_id
        and int(row.get("policy_pass", 0)) == 1
        and float(row.get("robust_score", 0.0) or 0.0) >= request.min_robust_score
    ]
    if request.backtest_run_id:
        filtered = [row for row in filtered if str(row.get("representative_backtest_run_id")) == request.backtest_run_id]
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
            family_key = (
                *series_key,
                str(row.get("strategy_family", "")),
            )
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
                -float(row.get("policy_metric_score", 0.0) or 0.0),
                int(row.get("selected_rank", 0)),
                -float(row.get("robust_score", 0.0) or 0.0),
            ),
        )
    if selection_policy == "top_by_family_per_series":
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("dataset_version", "")),
                str(row.get("contract_id", "")),
                str(row.get("timeframe", "")),
                str(row.get("strategy_family", "")),
                int(row.get("selected_rank", 0)),
                -float(row.get("robust_score", 0.0) or 0.0),
            ),
        )
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("dataset_version", "")),
            str(row.get("contract_id", "")),
            str(row.get("timeframe", "")),
            int(row.get("selected_rank", 0)),
            -float(row.get("robust_score", 0.0) or 0.0),
        ),
    )
