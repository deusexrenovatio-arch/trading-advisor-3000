from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from statistics import mean

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    CandidateProjectionRequest,
    RankingPolicy,
    build_ephemeral_strategy_space,
    backtest_store_contract,
    results_store_contract,
    project_runtime_candidates,
    rank_backtest_results,
    run_backtest_batch,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ResearchDatasetManifest,
    materialize_research_dataset,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.features import materialize_feature_frames, research_feature_store_contract
from trading_advisor_3000.product_plane.research.indicators import materialize_indicator_frames, indicator_store_contract
from trading_advisor_3000.product_plane.research.io import ResearchFrameCache
from trading_advisor_3000.product_plane.research.strategies import StrategyCatalog, StrategyRegistry, build_strategy_registry

def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _sorted_timeframes(bars: list[CanonicalBar]) -> tuple[str, ...]:
    order = {"1m": 1, "5m": 5, "10m": 10, "15m": 15, "1h": 60, "4h": 240, "1d": 1_440, "1w": 10_080}
    values = {bar.timeframe.value for bar in bars}
    return tuple(sorted(values, key=lambda item: (order.get(item, 99_999), item)))


def _normalized_bars(
    bars: list[CanonicalBar],
    instrument_by_contract: dict[str, str],
) -> list[CanonicalBar]:
    normalized: list[CanonicalBar] = []
    for bar in bars:
        instrument_id = instrument_by_contract.get(bar.contract_id, bar.instrument_id)
        normalized.append(
            CanonicalBar.from_dict(
                {
                    "contract_id": bar.contract_id,
                    "instrument_id": instrument_id,
                    "timeframe": bar.timeframe.value,
                    "ts": bar.ts,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "open_interest": bar.open_interest,
                }
            )
        )
    return normalized


def _inline_canonical_context(bars: list[CanonicalBar]) -> tuple[list[SessionCalendarEntry], list[RollMapEntry]]:
    calendar: dict[tuple[str, str, str], dict[str, str]] = {}
    roll_map: dict[tuple[str, str], tuple[str, int, str]] = {}

    for bar in sorted(bars, key=lambda item: (item.instrument_id, item.timeframe.value, item.ts, item.contract_id)):
        session_date = bar.ts[:10]
        calendar_key = (bar.instrument_id, bar.timeframe.value, session_date)
        current_calendar = calendar.get(calendar_key)
        if current_calendar is None:
            calendar[calendar_key] = {
                "session_open_ts": bar.ts,
                "session_close_ts": bar.ts,
            }
        else:
            if bar.ts < current_calendar["session_open_ts"]:
                current_calendar["session_open_ts"] = bar.ts
            if bar.ts > current_calendar["session_close_ts"]:
                current_calendar["session_close_ts"] = bar.ts

        roll_key = (bar.instrument_id, session_date)
        current_roll = roll_map.get(roll_key)
        if current_roll is None or bar.open_interest > current_roll[1] or (
            bar.open_interest == current_roll[1] and bar.ts > current_roll[2]
        ):
            roll_map[roll_key] = (bar.contract_id, bar.open_interest, bar.ts)

    session_calendar = [
        SessionCalendarEntry(
            instrument_id=key[0],
            timeframe=key[1],
            session_date=key[2],
            session_open_ts=value["session_open_ts"],
            session_close_ts=value["session_close_ts"],
        )
        for key, value in sorted(calendar.items())
    ]
    roll_entries = [
        RollMapEntry(
            instrument_id=key[0],
            session_date=key[1],
            active_contract_id=value[0],
            reason="inline_primary_entrypoint_max_open_interest",
        )
        for key, value in sorted(roll_map.items())
    ]
    return session_calendar, roll_entries


def _primary_strategy_registry(strategy_version_id: str) -> StrategyRegistry:
    base_registry = build_strategy_registry()
    base_spec = base_registry.get(strategy_version_id)
    primary_spec = replace(
        base_spec,
        version=strategy_version_id,
        description=base_spec.description,
    )
    return StrategyRegistry(
        catalog=StrategyCatalog(
            version=f"{base_registry.catalog_version()}-primary-entrypoint",
            strategies=(primary_spec,),
        )
    )


def _primary_ranking_policy(backtest_config: dict[str, object]) -> RankingPolicy:
    metric_order = tuple(
        str(item)
        for item in backtest_config.get(
            "ranking_metric_order",
            ("total_return", "profit_factor", "max_drawdown"),
        )
    )
    return RankingPolicy(
        policy_id=str(backtest_config.get("ranking_policy_id", "primary_entrypoint_v1")),
        metric_order=metric_order,
        require_out_of_sample_pass=bool(backtest_config.get("require_out_of_sample_pass", False)),
        min_trade_count=int(backtest_config.get("min_trade_count", 1)),
        max_drawdown_cap=float(backtest_config.get("max_drawdown_cap", 0.9)),
        min_positive_fold_ratio=float(backtest_config.get("min_positive_fold_ratio", 0.0)),
        stress_slippage_bps=float(backtest_config.get("stress_slippage_bps", 0.0)),
        min_parameter_stability=float(backtest_config.get("min_parameter_stability", 0.0)),
        min_slippage_score=float(backtest_config.get("min_slippage_score", 0.0)),
    )


def _compat_backtest_run(
    *,
    strategy_version_id: str,
    dataset_version: str,
    representative_run: dict[str, object] | None,
    candidate_count: int,
    walk_forward_windows: int,
    commission_total: float,
    slippage_total: float,
) -> dict[str, object]:
    if representative_run is not None:
        return {
            **dict(representative_run),
            "strategy_version_id": strategy_version_id,
            "dataset_version": dataset_version,
            "candidate_count": candidate_count,
            "walk_forward_windows": walk_forward_windows,
            "commission_total": commission_total,
            "slippage_total": slippage_total,
        }
    return {
        "backtest_run_id": "BTRUN-" + _stable_hash(f"{strategy_version_id}|{dataset_version}"),
        "strategy_version_id": strategy_version_id,
        "dataset_version": dataset_version,
        "candidate_count": candidate_count,
        "walk_forward_windows": walk_forward_windows,
        "commission_total": commission_total,
        "slippage_total": slippage_total,
        "status": "completed",
    }


def _compat_strategy_metrics(
    *,
    candidate_rows: list[dict[str, object]],
    walk_forward_windows: int,
    session_hours_utc: tuple[int, int] | None,
    commission_total: float,
    slippage_total: float,
) -> dict[str, object]:
    avg_score = mean(float(row.get("score", 0.0) or 0.0) for row in candidate_rows) if candidate_rows else 0.0
    avg_risk_reward = (
        mean(
            abs(float(row.get("target_ref", 0.0) or 0.0) - float(row.get("entry_ref", 0.0) or 0.0))
            / max(abs(float(row.get("entry_ref", 0.0) or 0.0) - float(row.get("stop_ref", 0.0) or 0.0)), 1e-9)
            for row in candidate_rows
        )
        if candidate_rows
        else 0.0
    )
    return {
        "walk_forward_windows": walk_forward_windows,
        "session_hours_utc": None if session_hours_utc is None else [session_hours_utc[0], session_hours_utc[1]],
        "candidate_count": len(candidate_rows),
        "avg_score": avg_score,
        "avg_risk_reward": avg_risk_reward,
        "commission_total": commission_total,
        "slippage_total": slippage_total,
    }
    return RankingPolicy(
        policy_id=str(backtest_config.get("ranking_policy_id", "primary_entrypoint_v1")),
        metric_order=metric_order,
        require_out_of_sample_pass=bool(backtest_config.get("require_out_of_sample_pass", False)),
        min_trade_count=int(backtest_config.get("min_trade_count", 1)),
        max_drawdown_cap=float(backtest_config.get("max_drawdown_cap", 0.9)),
        min_positive_fold_ratio=float(backtest_config.get("min_positive_fold_ratio", 0.0)),
        stress_slippage_bps=float(backtest_config.get("stress_slippage_bps", 0.0)),
        min_parameter_stability=float(backtest_config.get("min_parameter_stability", 0.0)),
        min_slippage_score=float(backtest_config.get("min_slippage_score", 0.0)),
    )


def run_research_from_bars(
    *,
    bars: list[CanonicalBar],
    instrument_by_contract: dict[str, str],
    strategy_version_id: str,
    dataset_version: str,
    output_dir: Path,
    backtest_config: dict[str, object] | None = None,
    scoring_profile_path: Path | None = None,
    repeatable: bool = True,
) -> dict[str, object]:
    """Primary public research entrypoint over the materialized research plane."""
    backtest_config = backtest_config or {}
    normalized_bars = _normalized_bars(bars, instrument_by_contract)
    timeframes = _sorted_timeframes(normalized_bars)
    if not timeframes:
        raise ValueError("bars must not be empty")

    materialized_dir = output_dir / "materialized"
    research_dir = output_dir / "research"
    session_calendar, roll_map = _inline_canonical_context(normalized_bars)
    base_timeframe = str(backtest_config.get("base_timeframe", timeframes[0]))
    indicator_set_version = str(backtest_config.get("indicator_set_version", "indicators-v1"))
    feature_set_version = str(backtest_config.get("feature_set_version", "features-v1"))
    indicator_profile_version = str(backtest_config.get("indicator_profile_version", "core_v1"))
    feature_profile_version = str(backtest_config.get("feature_profile_version", "core_v1"))
    session_hours_raw = backtest_config.get("session_hours_utc")
    session_hours_utc = (
        tuple(int(item) for item in session_hours_raw)
        if isinstance(session_hours_raw, (list, tuple)) and len(session_hours_raw) == 2
        else None
    )
    walk_forward_windows = int(backtest_config.get("walk_forward_windows", 1))
    commission_per_trade = float(backtest_config.get("commission_per_trade", 0.0))
    slippage_bps = float(backtest_config.get("slippage_bps", 0.0))
    allow_short = bool(backtest_config.get("allow_short", True))

    manifest_seed = ResearchDatasetManifest(
        dataset_version=dataset_version,
        dataset_name=str(backtest_config.get("dataset_name", "primary-public-research")),
        source_table="inline_canonical_bars",
        universe_id=str(backtest_config.get("universe_id", "inline-public-research")),
        timeframes=timeframes,
        base_timeframe=base_timeframe,
        start_ts=min(bar.ts for bar in normalized_bars),
        end_ts=max(bar.ts for bar in normalized_bars),
        split_method=str(backtest_config.get("split_method", "full")),
        warmup_bars=int(backtest_config.get("warmup_bars", 0)),
        code_version="materialized-research-primary-entrypoint",
    )
    materialize_research_dataset(
        manifest_seed=manifest_seed,
        bars=normalized_bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=materialized_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        profile_version=indicator_profile_version,
    )
    materialize_feature_frames(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
        profile_version=feature_profile_version,
    )

    strategy_registry = _primary_strategy_registry(strategy_version_id)
    all_combinations = len(strategy_registry.parameter_combinations(strategy_version_id))
    instances_per_strategy = int(backtest_config.get("combination_count", all_combinations))
    strategy_space = build_ephemeral_strategy_space(
        strategy_registry=strategy_registry,
        strategy_version_labels=(strategy_version_id,),
        instances_per_strategy=instances_per_strategy,
    )
    campaign_run_id = "crun_" + _stable_hash(f"{dataset_version}|{strategy_space.strategy_space_id}|{strategy_version_id}")
    request = BacktestBatchRequest(
        campaign_run_id=campaign_run_id,
        strategy_space_id=strategy_space.strategy_space_id,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
        strategy_instances=strategy_space.strategy_instances,
        combination_count=len(strategy_space.strategy_instances),
        param_batch_size=int(backtest_config.get("param_batch_size", min(max(len(strategy_space.strategy_instances), 1), 25))),
        series_batch_size=int(backtest_config.get("series_batch_size", 4)),
        timeframe=str(backtest_config.get("timeframe", base_timeframe)),
        contract_ids=tuple(str(item) for item in backtest_config.get("contract_ids", ())),
        instrument_ids=tuple(str(item) for item in backtest_config.get("instrument_ids", ())),
    )
    engine_config = BacktestEngineConfig(
        fees_bps=float(backtest_config.get("fees_bps", 0.0)),
        slippage_bps=slippage_bps,
        allow_short=allow_short,
        session_hours_utc=session_hours_utc,
        window_count=walk_forward_windows,
    )
    cache = ResearchFrameCache()
    batch_report = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        output_dir=research_dir,
        request=request,
        engine_config=engine_config,
        strategy_registry=strategy_registry,
        cache=cache,
    )
    ranking_policy = _primary_ranking_policy(backtest_config)
    ranking_report = rank_backtest_results(
        backtest_output_dir=research_dir,
        output_dir=research_dir,
        policy=ranking_policy,
        strategy_registry=strategy_registry,
    )
    projection_report = project_runtime_candidates(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        output_dir=research_dir,
        request=CandidateProjectionRequest(
            ranking_policy_id=ranking_policy.policy_id,
            selection_policy=str(backtest_config.get("selection_policy", "all_policy_pass")),
            max_candidates_per_partition=int(backtest_config.get("max_candidates_per_partition", 4)),
            min_robust_score=float(backtest_config.get("min_robust_score", 0.0)),
            decision_lag_bars_max=int(backtest_config.get("decision_lag_bars_max", max(1, len(normalized_bars)))),
        ),
        ranking_rows=ranking_report["ranking_rows"],
        strategy_registry=strategy_registry,
        config=engine_config,
    )
    candidate_contract_rows = [dict(payload) for payload in projection_report["candidate_contracts"]]
    candidate_rows = list(projection_report["candidate_rows"])
    commission_total = commission_per_trade * len(candidate_rows)
    slippage_total = sum(float(row["estimated_slippage"]) for row in candidate_rows)
    strategy_metrics = _compat_strategy_metrics(
        candidate_rows=candidate_rows,
        walk_forward_windows=walk_forward_windows,
        session_hours_utc=session_hours_utc,
        commission_total=commission_total,
        slippage_total=slippage_total,
    )
    representative_run = batch_report["run_rows"][0] if batch_report["run_rows"] else None
    backtest_run = _compat_backtest_run(
        strategy_version_id=strategy_version_id,
        dataset_version=dataset_version,
        representative_run=representative_run,
        candidate_count=len(candidate_rows),
        walk_forward_windows=walk_forward_windows,
        commission_total=commission_total,
        slippage_total=slippage_total,
    )
    output_paths = {
        "research_datasets": (materialized_dir / "research_datasets.delta").as_posix(),
        "research_bar_views": (materialized_dir / "research_bar_views.delta").as_posix(),
        "research_indicator_frames": (materialized_dir / "research_indicator_frames.delta").as_posix(),
        "research_feature_frames": (materialized_dir / "research_feature_frames.delta").as_posix(),
        "research_backtest_batches": str(batch_report["output_paths"]["research_backtest_batches"]),
        "research_backtest_runs": str(batch_report["output_paths"]["research_backtest_runs"]),
        "research_strategy_stats": str(batch_report["output_paths"]["research_strategy_stats"]),
        "research_trade_records": str(batch_report["output_paths"]["research_trade_records"]),
        "research_order_records": str(batch_report["output_paths"]["research_order_records"]),
        "research_drawdown_records": str(batch_report["output_paths"]["research_drawdown_records"]),
        "research_strategy_rankings": str(ranking_report["output_paths"]["research_strategy_rankings"]),
        "research_signal_candidates": str(projection_report["output_paths"]["research_signal_candidates"]),
        "backtest_runs": str(batch_report["output_paths"]["research_backtest_runs"]),
        "signal_candidates": str(projection_report["output_paths"]["research_signal_candidates"]),
    }

    return {
        "bars_processed": len(normalized_bars),
        "strategy_space_id": strategy_space.strategy_space_id,
        "strategy_instance_count": len(strategy_space.strategy_instances),
        "signal_contracts": len(candidate_contract_rows),
        "research_candidates": len(candidate_rows),
        "backtest_batch": batch_report["backtest_batch"],
        "backtest_run": backtest_run,
        "strategy_metrics": strategy_metrics,
        "delta_manifest": {
            "feature_snapshots": research_feature_store_contract()["feature_snapshots"],
            **research_dataset_store_contract(),
            **indicator_store_contract(),
            **research_feature_store_contract(),
            **backtest_store_contract(),
            **results_store_contract(),
        },
        "output_paths": output_paths,
        "signal_contract_rows": candidate_contract_rows,
        "primary_path": "materialized_research",
    }
