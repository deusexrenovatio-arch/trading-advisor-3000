from __future__ import annotations

import argparse
import gc
import json
import time
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_frame
from trading_advisor_3000.product_plane.research.backtests.engine import (
    BacktestEngineConfig,
    run_vectorbt_family_search,
    search_spec_id,
    strategy_spec_to_search_spec,
)
from trading_advisor_3000.product_plane.research.backtests.input_requirements import loader_columns_for_search_specs
from trading_advisor_3000.product_plane.research.backtests.results import write_backtest_artifacts
from trading_advisor_3000.product_plane.research.io.loaders import ResearchSeriesFrame, ResearchSliceRequest, load_backtest_frames
from trading_advisor_3000.product_plane.research.strategies.registry import build_strategy_registry


BATTLE_SPACES: dict[str, dict[str, tuple[object, ...]]] = {
    "mean-reversion-v1": {
        "entry_distance_atr": (0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0),
        "entry_rsi": (15, 20, 25, 30, 35),
        "exit_rsi": (40, 45, 50, 55, 60),
        "stop_atr_mult": (0.6, 0.9, 1.2, 1.5, 2.0),
        "target_atr_mult": (1.0, 1.5, 2.0, 2.5, 3.0),
    },
    "trend-mtf-pullback-v1": {
        "adx_min": (18, 22, 28),
        "slope_min": (0.0, 0.0005, 0.001),
        "pullback_atr_min": (0.0, 0.3),
        "pullback_atr_max": (0.8, 1.2, 1.8),
        "rsi_reclaim_long": (45, 50, 55),
        "rsi_reclaim_short": (45, 50, 55),
        "stop_atr_mult": (1.5, 2.0, 2.5),
        "trail_atr_mult": (2.0, 2.5, 3.0),
        "max_holding_bars": (24, 48, 96),
    },
}
TEMPLATE_KEYS = {
    "mean-reversion-v1": "mean_reversion_core",
    "trend-mtf-pullback-v1": "trend_mtf_pullback",
}
DEFAULT_RESEARCH_RUNS_ROOT = Path("D:/TA3000-data/trading-advisor-3000-nightly/research/runs")
BENCHMARK_OUTPUT_ROOT = DEFAULT_RESEARCH_RUNS_ROOT / "_benchmarks"
BENCHMARK_BACKTEST_BATCH_ID = "BTBATCH-BENCH-DELTA-HOTPATH"
BENCHMARK_CAMPAIGN_RUN_ID = "bench_delta_hotpath"
BENCHMARK_STRATEGY_SPACE_ID = "bench_space_delta_hotpath"
BENCHMARK_ARTIFACT_ROLE = "benchmark_noncanonical"
CANONICAL_CAMPAIGN_ROUTE = (
    "py -3.11 -m trading_advisor_3000.product_plane.research.jobs.run_campaign "
    "--config <campaign.yaml>"
)


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_slug(value: str) -> str:
    return value.strip().replace("-", "_").replace(" ", "_")


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(parent.resolve(strict=False))
    except ValueError:
        return False
    return True


def _default_output_dir(*, dataset_version: str, strategy_label: str, run_stamp: str) -> Path:
    return BENCHMARK_OUTPUT_ROOT / (
        f"{_safe_slug(dataset_version)}_{_safe_slug(strategy_label)}_optuna_owned_{run_stamp}"
    )


def _validate_benchmark_output_dir(output_dir: Path) -> None:
    if _is_relative_to(output_dir, DEFAULT_RESEARCH_RUNS_ROOT) and not _is_relative_to(
        output_dir,
        BENCHMARK_OUTPUT_ROOT,
    ):
        raise ValueError(
            "benchmark output cannot be written directly under canonical research/runs; "
            f"use {BENCHMARK_OUTPUT_ROOT.as_posix()} or run the canonical campaign route: "
            f"{CANONICAL_CAMPAIGN_ROUTE}"
        )


def _load_split_windows(materialized_root: Path, *, dataset_version: str) -> tuple[dict[str, object], ...]:
    frame = read_delta_table_frame(
        materialized_root / "research_datasets.delta",
        columns=["dataset_version", "split_params_json"],
        filters=[("dataset_version", "=", dataset_version)],
    )
    if frame.empty:
        return tuple()
    raw = frame.iloc[0].to_dict().get("split_params_json")
    payload = json.loads(raw) if isinstance(raw, str) and raw.strip() else {}
    windows = payload.get("windows", []) if isinstance(payload, dict) else []
    return tuple(dict(item) for item in windows if isinstance(item, dict))


def _load_contract_frames(
    *,
    materialized_root: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    instrument_id: str,
    timeframe: str,
    search_spec: object,
) -> tuple[list[ResearchSeriesFrame], dict[str, float]]:
    started = time.perf_counter()
    input_columns = loader_columns_for_search_specs((search_spec,))
    required_timeframes = _required_timeframes(search_spec, fallback=timeframe)
    loader_timeframe = next(iter(required_timeframes)) if len(required_timeframes) == 1 else ""
    frames, _, _ = load_backtest_frames(
        dataset_output_dir=materialized_root,
        indicator_output_dir=materialized_root,
        derived_indicator_output_dir=materialized_root,
        request=ResearchSliceRequest(
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            timeframe=loader_timeframe,
            instrument_ids=(instrument_id,),
            analysis_only=True,
            price_columns=input_columns.price_columns,
            indicator_columns=input_columns.indicator_columns,
            derived_columns=input_columns.derived_columns,
        ),
    )
    elapsed = time.perf_counter() - started
    return list(frames), {
        "delta_native_loader_seconds": elapsed,
        "delta_load_total_seconds": elapsed,
    }


def _required_timeframes(search_spec: object, *, fallback: str) -> tuple[str, ...]:
    values: list[str] = []
    for payload in getattr(search_spec, "required_inputs_by_clock", {}).values():
        if not isinstance(payload, dict):
            continue
        has_inputs = any(payload.get(key) for key in ("price_inputs", "materialized_indicators", "materialized_derived"))
        timeframe = str(payload.get("timeframe", "")).strip()
        if has_inputs and timeframe and timeframe not in values:
            values.append(timeframe)
    if not values:
        values.append(fallback)
    return tuple(values)


def _execution_timeframe(search_spec: object, *, fallback: str) -> str:
    profile = getattr(search_spec, "clock_profile", {}) or {}
    value = profile.get("execution_tf") if isinstance(profile, dict) else None
    resolved = str(value).strip() if value is not None else ""
    return resolved or fallback


def _group_series_frames(
    frames: list[ResearchSeriesFrame],
    *,
    search_spec: object,
    fallback_timeframe: str,
    max_contracts: int,
) -> list[tuple[ResearchSeriesFrame, ...]]:
    required_timeframes = _required_timeframes(search_spec, fallback=fallback_timeframe)
    required_timeframe_set = set(required_timeframes)
    execution_tf = _execution_timeframe(search_spec, fallback=fallback_timeframe)
    grouped: dict[tuple[str, str], list[ResearchSeriesFrame]] = {}
    for frame in frames:
        if frame.timeframe not in required_timeframe_set:
            continue
        grouped.setdefault((frame.contract_id, frame.instrument_id), []).append(frame)
    result: list[tuple[ResearchSeriesFrame, ...]] = []
    missing_groups: list[dict[str, object]] = []
    selected_groups = list(sorted(grouped.items()))
    if max_contracts > 0:
        selected_groups = selected_groups[:max_contracts]
    for (contract_id, instrument_id), items in selected_groups:
        by_tf = {item.timeframe: item for item in items}
        missing = [timeframe for timeframe in required_timeframes if timeframe not in by_tf]
        if execution_tf not in by_tf and execution_tf not in missing:
            missing.append(execution_tf)
        if missing:
            missing_groups.append(
                {
                    "contract_id": contract_id,
                    "instrument_id": instrument_id,
                    "missing_timeframes": missing,
                    "present_timeframes": sorted(by_tf),
                }
            )
            continue
        result.append(tuple(by_tf[timeframe] for timeframe in required_timeframes))
    if missing_groups:
        sample = json.dumps(missing_groups[:5], ensure_ascii=False, sort_keys=True)
        raise ValueError(f"incomplete MTF input groups for {search_spec.family_key}: {sample}")
    if not result:
        loaded_timeframes = sorted({frame.timeframe for frame in frames})
        raise ValueError(
            "no complete contract groups found for "
            f"{search_spec.family_key}; required={list(required_timeframes)} loaded={loaded_timeframes}"
        )
    return result


def _input_summary(
    search_spec: object,
    frames: list[ResearchSeriesFrame],
    *,
    fallback_timeframe: str,
    contract_group_count: int = 0,
) -> dict[str, object]:
    roles: dict[str, object] = {}
    used_timeframes_by_role: dict[str, str] = {}
    for role, payload in getattr(search_spec, "required_inputs_by_clock", {}).items():
        if not isinstance(payload, dict):
            continue
        counts = {
            "price": len(payload.get("price_inputs") or []),
            "indicators": len(payload.get("materialized_indicators") or []),
            "derived": len(payload.get("materialized_derived") or []),
        }
        total = sum(counts.values())
        timeframe = str(payload.get("timeframe", ""))
        if total:
            used_timeframes_by_role[str(role)] = timeframe
        roles[str(role)] = {
            "timeframe": timeframe,
            **counts,
            "total": total,
            "price_inputs": list(payload.get("price_inputs") or []),
            "materialized_indicators": list(payload.get("materialized_indicators") or []),
            "materialized_derived": list(payload.get("materialized_derived") or []),
        }
    row_counts: Counter[str] = Counter()
    for frame in frames:
        row_counts[frame.timeframe] += len(frame.frame)
    input_columns = loader_columns_for_search_specs((search_spec,))
    required_timeframes = _required_timeframes(search_spec, fallback=fallback_timeframe)
    return {
        "clock_profile": dict(getattr(search_spec, "clock_profile", {}) or {}),
        "execution_timeframe": _execution_timeframe(search_spec, fallback=fallback_timeframe),
        "required_timeframes": list(required_timeframes),
        "used_timeframes_by_role": used_timeframes_by_role,
        "roles": roles,
        "loaded_series_count": len(frames),
        "contract_group_count": contract_group_count,
        "loaded_rows_by_timeframe": dict(sorted(row_counts.items())),
        "projected_columns": {
            "price": list(input_columns.price_columns),
            "indicator": list(input_columns.indicator_columns),
            "derived": list(input_columns.derived_columns),
        },
    }


def _count_rows(report: dict[str, object]) -> dict[str, int]:
    return {
        key: len(value) if isinstance(value, list) else 0
        for key, value in report.items()
        if key.endswith("_rows")
    }


def _search_spec_row(search_spec: object) -> dict[str, object]:
    payload = search_spec.to_dict()
    return {
        "search_spec_id": search_spec_id(search_spec),
        "family_key": search_spec.family_key,
        "template_key": search_spec.template_key,
        "search_spec_version": search_spec.search_spec_version,
        "intent": search_spec.intent,
        "clock_profiles": list(search_spec.allowed_clock_profiles),
        "market_states": list(search_spec.allowed_market_states),
        "required_price_inputs_json": list(search_spec.required_price_inputs),
        "required_materialized_indicators_json": list(search_spec.required_materialized_indicators),
        "required_materialized_derived_json": list(search_spec.required_materialized_derived),
        "optional_indicator_plan_json": [item.to_dict() for item in search_spec.optional_indicator_plan],
        "signal_surface_key": search_spec.signal_surface_key,
        "signal_surface_mode": search_spec.signal_surface_mode,
        "parameter_mode": search_spec.parameter_mode,
        "parameter_space_json": payload["parameter_space"],
        "parameter_constraints_json": list(search_spec.parameter_constraints),
        "clock_profile_json": payload["clock_profile"],
        "required_inputs_by_clock_json": payload["required_inputs_by_clock"],
        "parameter_space_by_role_json": payload["parameter_space_by_role"],
        "parameter_clock_map_json": payload["parameter_clock_map"],
        "exit_parameter_space_json": payload["exit_parameter_space"],
        "risk_parameter_space_json": payload["risk_parameter_space"],
        "execution_assumptions_json": payload["execution_assumptions"],
        "created_at": _created_at(),
    }


def _coerce_json_object(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        payload = json.loads(value)
        if isinstance(payload, dict):
            return {str(key): item for key, item in payload.items()}
    return {}


def _slim_optimizer_trial(row: dict[str, object]) -> dict[str, object]:
    components = _coerce_json_object(row.get("objective_components_json", {}))
    return {
        "optimizer_trial_id": row.get("optimizer_trial_id", ""),
        "optimizer_study_id": row.get("optimizer_study_id", ""),
        "trial_number": row.get("trial_number", -1),
        "trial_kind": row.get("trial_kind", ""),
        "status": row.get("status", ""),
        "params_hash": row.get("param_hash", ""),
        "value": row.get("value", -1.0),
        "constraints_passed": bool(row.get("constraints_passed", False)),
        "constraint_names": components.get("constraint_names", ()),
        "constraint_values": components.get("constraint_values", ()),
        "selection_owner": components.get("selection_owner", ""),
    }


def _slim_optimizer_study(row: dict[str, object]) -> dict[str, object]:
    return {
        "optimizer_study_id": row.get("optimizer_study_id", ""),
        "status": row.get("status", ""),
        "best_trial_number": row.get("best_trial_number", -1),
        "best_param_hash": row.get("best_param_hash", ""),
        "best_value": row.get("best_value", -1.0),
    }


def _optimizer_selection_summary(
    *,
    optimizer_study_rows: list[dict[str, Any]],
    optimizer_trial_rows: list[dict[str, Any]],
    top_k: int,
) -> dict[str, object]:
    trial_rows = [dict(row) for row in optimizer_trial_rows]
    accepted_rows = [
        row
        for row in trial_rows
        if str(row.get("status", "")) in {"completed", "duplicate"} and bool(row.get("constraints_passed", False))
    ]
    top_rows = sorted(
        trial_rows,
        key=lambda row: (
            -int(bool(row.get("constraints_passed", False))),
            -float(row.get("value", -1.0) or -1.0),
            int(row.get("trial_number", 999_999) or 999_999),
        ),
    )[:top_k]
    return {
        "selection_owner": "optuna.study",
        "optimizer_study_count": len(optimizer_study_rows),
        "optimizer_trial_count": len(trial_rows),
        "accepted_candidate_count": len(accepted_rows),
        "rejected_candidate_count": len(trial_rows) - len(accepted_rows),
        "study_status_counts": dict(Counter(str(row.get("status", "")) for row in optimizer_study_rows)),
        "trial_status_counts": dict(Counter(str(row.get("status", "")) for row in trial_rows)),
        "detail_source": "delta_tables",
        "best_studies": [_slim_optimizer_study(row) for row in optimizer_study_rows[:top_k]],
        "top_trials": [_slim_optimizer_trial(row) for row in top_rows],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark one TA3000 vectorbt family from Delta/Arrow inputs.")
    parser.add_argument("--materialized-root", default="D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/optuna-signal-smoke-br")
    parser.add_argument("--dataset-version", default="moex_approved_subset_optuna_15m_br_smoke_v1")
    parser.add_argument("--indicator-set-version", default="indicators-v1")
    parser.add_argument("--derived-indicator-set-version", default="derived-v1")
    parser.add_argument("--instrument-id", default="FUT_BR")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--strategy-label", default="mean-reversion-v1")
    parser.add_argument("--trials", type=int, default=128)
    parser.add_argument("--max-contracts", type=int, default=0)
    parser.add_argument("--optimizer-top-k", type=int, default=20)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--output-json", default="")
    args = parser.parse_args()

    materialized_root = Path(args.materialized_root)
    run_stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else _default_output_dir(
            dataset_version=args.dataset_version,
            strategy_label=args.strategy_label,
            run_stamp=run_stamp,
        )
    )
    _validate_benchmark_output_dir(output_dir)
    battle_space = BATTLE_SPACES.get(args.strategy_label)
    if battle_space is None:
        raise ValueError(f"unsupported strategy label for benchmark: {args.strategy_label}")
    total_space = 1
    for values in battle_space.values():
        total_space *= len(values)

    registry = build_strategy_registry()
    base_spec = strategy_spec_to_search_spec(
        registry.get(args.strategy_label),
        template_key=TEMPLATE_KEYS.get(args.strategy_label),
        max_parameter_combinations=250_000,
    )
    search_spec = replace(
        base_spec,
        parameter_space=battle_space,
        max_parameter_combinations=250_000,
    )
    load_started = time.perf_counter()
    frames, load_timings = _load_contract_frames(
        materialized_root=materialized_root,
        dataset_version=args.dataset_version,
        indicator_set_version=args.indicator_set_version,
        derived_indicator_set_version=args.derived_indicator_set_version,
        instrument_id=args.instrument_id,
        timeframe=args.timeframe,
        search_spec=search_spec,
    )
    split_windows = _load_split_windows(materialized_root, dataset_version=args.dataset_version)
    load_total = time.perf_counter() - load_started
    contract_groups = _group_series_frames(
        frames,
        search_spec=search_spec,
        fallback_timeframe=args.timeframe,
        max_contracts=args.max_contracts,
    )

    config = BacktestEngineConfig(
        fees_bps=0.0,
        slippage_bps=5.0,
        allow_short=True,
        window_count=3,
    )
    optimizer_policy = {
        "engine": "optuna",
        "sampler": "tpe",
        "seed": 20260429,
        "n_trials": args.trials,
        "objective": "robust_oos_trial_v1",
        "direction": "maximize",
        "top_k": 8,
        "radius": 0,
        "max_neighborhood_trials": 0,
        "ranking_policy": {
            "policy_id": "robust_oos_v1",
            "metric_order": ["total_return", "profit_factor", "max_drawdown"],
            "require_out_of_sample_pass": True,
            "min_trade_count": 8,
            "max_drawdown_cap": 0.35,
            "min_positive_fold_ratio": 0.5,
            "stress_slippage_bps": 7.5,
            "min_parameter_stability": 0.35,
            "min_slippage_score": 0.45,
        },
    }

    print(
        json.dumps(
            {
                "event": "loaded_delta_inputs",
                "artifact_role": BENCHMARK_ARTIFACT_ROLE,
                "canonical_campaign_route": CANONICAL_CAMPAIGN_ROUTE,
                "contracts": len(contract_groups),
                "total_parameter_space": total_space,
                "trials_per_contract": args.trials,
                "expected_trial_rows": len(contract_groups) * args.trials,
                "split_windows": [window.get("window_id") for window in split_windows],
                "load_total_seconds": load_total,
                "input_summary": _input_summary(
                    search_spec,
                    frames,
                    fallback_timeframe=args.timeframe,
                    contract_group_count=len(contract_groups),
                ),
                **load_timings,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )

    benchmark_started = time.perf_counter()
    aggregate_counts: Counter[str] = Counter()
    aggregate_trial_statuses: Counter[str] = Counter()
    all_search_run_rows: list[dict[str, Any]] = []
    all_optimizer_study_rows: list[dict[str, Any]] = []
    all_optimizer_trial_rows: list[dict[str, Any]] = []
    all_param_result_rows: list[dict[str, Any]] = []
    all_gate_rows: list[dict[str, Any]] = []
    all_run_rows: list[dict[str, Any]] = []
    all_stat_rows: list[dict[str, Any]] = []
    all_trade_rows: list[dict[str, Any]] = []
    all_order_rows: list[dict[str, Any]] = []
    all_drawdown_rows: list[dict[str, Any]] = []
    for index, group in enumerate(contract_groups, start=1):
        contract_started = time.perf_counter()
        execution_frame = next(
            (
                frame
                for frame in group
                if frame.timeframe == _execution_timeframe(search_spec, fallback=args.timeframe)
            ),
            group[0],
        )
        report = run_vectorbt_family_search(
            series_frames=group,
            search_spec=search_spec,
            config=config,
            backtest_batch_id=BENCHMARK_BACKTEST_BATCH_ID,
            campaign_run_id=BENCHMARK_CAMPAIGN_RUN_ID,
            strategy_space_id=BENCHMARK_STRATEGY_SPACE_ID,
            dataset_version=args.dataset_version,
            indicator_set_version=args.indicator_set_version,
            derived_indicator_set_version=args.derived_indicator_set_version,
            split_windows=split_windows,
            param_batch_size=args.trials,
            optimizer_policy=optimizer_policy,
        )
        all_search_run_rows.extend(
            dict(row) for row in report.get("search_run_rows", []) if isinstance(row, dict)
        )
        all_optimizer_study_rows.extend(
            dict(row) for row in report.get("optimizer_study_rows", []) if isinstance(row, dict)
        )
        all_optimizer_trial_rows.extend(
            dict(row) for row in report.get("optimizer_trial_rows", []) if isinstance(row, dict)
        )
        all_param_result_rows.extend(
            dict(row) for row in report.get("param_result_rows", []) if isinstance(row, dict)
        )
        all_gate_rows.extend(dict(row) for row in report.get("gate_rows", []) if isinstance(row, dict))
        all_run_rows.extend(dict(row) for row in report.get("run_rows", []) if isinstance(row, dict))
        all_stat_rows.extend(dict(row) for row in report.get("stat_rows", []) if isinstance(row, dict))
        all_trade_rows.extend(dict(row) for row in report.get("trade_rows", []) if isinstance(row, dict))
        all_order_rows.extend(dict(row) for row in report.get("order_rows", []) if isinstance(row, dict))
        all_drawdown_rows.extend(
            dict(row) for row in report.get("drawdown_rows", []) if isinstance(row, dict)
        )
        elapsed = time.perf_counter() - contract_started
        counts = _count_rows(report)
        trial_statuses = Counter(
            str(row.get("status", ""))
            for row in report.get("optimizer_trial_rows", [])
            if isinstance(row, dict)
        )
        aggregate_counts.update(counts)
        aggregate_trial_statuses.update(trial_statuses)
        summary = {
            "event": "contract_done",
            "contract_index": index,
            "contract_count": len(contract_groups),
            "contract_id": execution_frame.contract_id,
            "timeframes": [frame.timeframe for frame in group],
            "rows_by_timeframe": {frame.timeframe: len(frame.frame) for frame in group},
            "seconds": elapsed,
            "trials_per_second": args.trials / elapsed if elapsed > 0 else 0.0,
            "counts": counts,
            "trial_status_counts": dict(trial_statuses),
        }
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        del report
        gc.collect()

    vectorbt_seconds = time.perf_counter() - benchmark_started
    batch_row = {
        "backtest_batch_id": BENCHMARK_BACKTEST_BATCH_ID,
        "campaign_run_id": BENCHMARK_CAMPAIGN_RUN_ID,
        "strategy_space_id": BENCHMARK_STRATEGY_SPACE_ID,
        "dataset_version": args.dataset_version,
        "indicator_set_version": args.indicator_set_version,
        "derived_indicator_set_version": args.derived_indicator_set_version,
        "engine_name": config.engine_name,
        "param_batch_size": args.trials,
        "series_batch_size": len(contract_groups),
        "combination_count": len(contract_groups) * args.trials,
        "series_count": len(frames),
        "cache_id": "",
        "cache_hit": 0,
        "created_at": all_stat_rows[0].get("created_at", _created_at()) if all_stat_rows else _created_at(),
    }
    output_paths = write_backtest_artifacts(
        output_dir=output_dir,
        batch_rows=[batch_row],
        search_spec_rows=[_search_spec_row(search_spec)],
        search_run_rows=all_search_run_rows,
        optimizer_study_rows=all_optimizer_study_rows,
        optimizer_trial_rows=all_optimizer_trial_rows,
        param_result_rows=all_param_result_rows,
        gate_event_rows=all_gate_rows,
        ephemeral_indicator_rows=[],
        promotion_event_rows=[],
        run_rows=all_run_rows,
        stat_rows=all_stat_rows,
        trade_rows=all_trade_rows,
        order_rows=all_order_rows,
        drawdown_rows=all_drawdown_rows,
    )
    optimizer_selection_summary = _optimizer_selection_summary(
        optimizer_study_rows=all_optimizer_study_rows,
        optimizer_trial_rows=all_optimizer_trial_rows,
        top_k=args.optimizer_top_k,
    )
    result = {
        "event": "benchmark_complete",
        "artifact_role": BENCHMARK_ARTIFACT_ROLE,
        "canonical_campaign_route": CANONICAL_CAMPAIGN_ROUTE,
        "strategy_label": args.strategy_label,
        "family_key": search_spec.family_key,
        "instrument_id": args.instrument_id,
        "timeframe": args.timeframe,
        "contracts": len(contract_groups),
        "total_parameter_space": total_space,
        "trials_per_contract": args.trials,
        "total_expected_trials": len(contract_groups) * args.trials,
        "delta_load_seconds": load_total,
        "vectorbt_seconds": vectorbt_seconds,
        "wall_clock_seconds": load_total + vectorbt_seconds,
        "trials_per_second_vectorbt": (len(contract_groups) * args.trials) / vectorbt_seconds if vectorbt_seconds > 0 else 0.0,
        "trials_per_second_wall": (len(contract_groups) * args.trials) / (load_total + vectorbt_seconds) if load_total + vectorbt_seconds > 0 else 0.0,
        "aggregate_counts": dict(aggregate_counts),
        "aggregate_trial_status_counts": dict(aggregate_trial_statuses),
        "output_dir": output_dir.as_posix(),
        "output_paths": output_paths,
        "summary_role": "benchmark_receipt_delta_paths_only",
        "optimizer_selection_summary": optimizer_selection_summary,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
