from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from statistics import mean

import pandas as pd

from trading_advisor_3000.product_plane.contracts import CanonicalBar, DecisionCandidate
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    CandidateProjectionRequest,
    RankingPolicy,
    phase5_backtest_store_contract,
    phase6_results_store_contract,
    project_runtime_candidates,
    rank_backtest_results,
    run_backtest_batch,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ResearchDatasetManifest,
    materialize_research_dataset,
    phase2_research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.features import materialize_feature_frames, phase2b_feature_store_contract
from trading_advisor_3000.product_plane.research.indicators import materialize_indicator_frames, phase3_indicator_store_contract
from trading_advisor_3000.product_plane.research.io import ResearchFrameCache, ResearchSliceRequest, load_backtest_frames
from trading_advisor_3000.product_plane.research.strategies import StrategyCatalog, StrategyRegistry, build_phase1_strategy_registry

_LEGACY_STRATEGY_ALIASES = {
    "trend-follow-v1": "ma-cross-v1",
    "mean-revert-v1": "mean-reversion-v1",
}


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


def _compat_strategy_registry(strategy_version_id: str) -> StrategyRegistry:
    base_registry = build_phase1_strategy_registry()
    primary_version = _LEGACY_STRATEGY_ALIASES.get(strategy_version_id, strategy_version_id)
    base_spec = base_registry.get(primary_version)
    compat_spec = replace(
        base_spec,
        version=strategy_version_id,
        description=f"{base_spec.description} Compatibility alias over the materialized research plane.",
    )
    return StrategyRegistry(
        catalog=StrategyCatalog(
            version=f"{base_registry.catalog_version()}-compat-primary-entrypoint",
            strategies=(compat_spec,),
        )
    )


def _compat_ranking_policy(backtest_config: dict[str, object]) -> RankingPolicy:
    metric_order = tuple(
        str(item)
        for item in backtest_config.get(
            "ranking_metric_order",
            ("total_return", "profit_factor", "max_drawdown"),
        )
    )
    return RankingPolicy(
        policy_id=str(backtest_config.get("ranking_policy_id", "compat_primary_v1")),
        metric_order=metric_order,
        require_out_of_sample_pass=bool(backtest_config.get("require_out_of_sample_pass", False)),
        min_trade_count=int(backtest_config.get("min_trade_count", 1)),
        max_drawdown_cap=float(backtest_config.get("max_drawdown_cap", 0.9)),
        min_positive_fold_ratio=float(backtest_config.get("min_positive_fold_ratio", 0.0)),
        stress_slippage_bps=float(backtest_config.get("stress_slippage_bps", 0.0)),
        min_parameter_stability=float(backtest_config.get("min_parameter_stability", 0.0)),
        min_slippage_score=float(backtest_config.get("min_slippage_score", 0.0)),
    )


def _compat_metrics(
    *,
    candidate_rows: list[dict[str, object]],
    walk_forward_windows: int,
    session_hours_utc: tuple[int, int] | None,
    commission_per_trade: float,
) -> dict[str, object]:
    avg_score = mean(float(row["score"]) for row in candidate_rows) if candidate_rows else 0.0
    avg_risk_reward = (
        mean(
            abs(float(row["target_ref"]) - float(row["entry_ref"]))
            / max(abs(float(row["entry_ref"]) - float(row["stop_ref"])), 1e-9)
            for row in candidate_rows
        )
        if candidate_rows
        else 0.0
    )
    trade_count = len(candidate_rows)
    return {
        "walk_forward_windows": walk_forward_windows,
        "session_hours_utc": None if session_hours_utc is None else [session_hours_utc[0], session_hours_utc[1]],
        "session_filtered_out": 0,
        "candidate_count": len(candidate_rows),
        "long_count": sum(1 for row in candidate_rows if str(row["side"]) == "long"),
        "short_count": sum(1 for row in candidate_rows if str(row["side"]) == "short"),
        "avg_score": avg_score,
        "avg_risk_reward": avg_risk_reward,
        "commission_total": commission_per_trade * trade_count,
        "slippage_total": sum(float(row["estimated_slippage"]) for row in candidate_rows),
    }


def _compat_backtest_run(
    *,
    strategy_version_id: str,
    dataset_version: str,
    candidate_rows: list[dict[str, object]],
    metrics: dict[str, object],
    representative_run: dict[str, object] | None,
) -> dict[str, object]:
    started_at = str(representative_run["started_at"]) if representative_run is not None else "1970-01-01T00:00:00Z"
    finished_at = str(representative_run["finished_at"]) if representative_run is not None else started_at
    params_hash = str(representative_run["params_hash"]) if representative_run is not None else "NONE"
    return {
        "backtest_run_id": str(representative_run["backtest_run_id"]) if representative_run is not None else f"BTRUN-{_stable_hash(strategy_version_id)}",
        "strategy_version_id": strategy_version_id,
        "dataset_version": dataset_version,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": "completed",
        "params_hash": params_hash,
        "candidate_count": len(candidate_rows),
        "walk_forward_windows": int(metrics["walk_forward_windows"]),
        "commission_total": float(metrics["commission_total"]),
        "slippage_total": float(metrics["slippage_total"]),
        "session_filtered_out": int(metrics["session_filtered_out"]),
    }


def _regime_label(code: object) -> str:
    if code is None or pd.isna(code):
        return "unknown"
    numeric = int(code)
    if numeric == 2:
        return "squeeze"
    if numeric > 0:
        return "trend_up"
    if numeric < 0:
        return "trend_down"
    return "neutral"


def _compat_snapshot_rows(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    feature_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    feature_set_version: str,
    timeframe: str,
) -> list[dict[str, object]]:
    frames, _, _ = load_backtest_frames(
        dataset_output_dir=dataset_output_dir,
        indicator_output_dir=indicator_output_dir,
        feature_output_dir=feature_output_dir,
        request=ResearchSliceRequest(
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            feature_set_version=feature_set_version,
            timeframe=timeframe,
            analysis_only=False,
        ),
    )
    rows: list[dict[str, object]] = []
    for series in frames:
        for payload in series.frame.sort_index().to_dict(orient="records"):
            required_values = (
                payload.get("atr_14"),
                payload.get("ema_10"),
                payload.get("ema_20"),
                payload.get("donchian_high_20"),
                payload.get("donchian_low_20"),
                payload.get("rvol_20"),
            )
            if any(item is None or pd.isna(item) for item in required_values):
                continue
            rows.append(
                {
                    "snapshot_id": "FS-" + _stable_hash(
                        "|".join(
                            (
                                str(payload["contract_id"]),
                                str(payload["timeframe"]),
                                str(payload["ts"]),
                                feature_set_version,
                            )
                        )
                    ),
                    "contract_id": str(payload["contract_id"]),
                    "instrument_id": str(payload["instrument_id"]),
                    "timeframe": str(payload["timeframe"]),
                    "ts": str(payload["ts"]),
                    "feature_set_version": feature_set_version,
                    "regime": _regime_label(payload.get("regime_state_code")),
                    "atr": float(payload["atr_14"]),
                    "ema_fast": float(payload["ema_10"]),
                    "ema_slow": float(payload["ema_20"]),
                    "donchian_high": float(payload["donchian_high_20"]),
                    "donchian_low": float(payload["donchian_low_20"]),
                    "rvol": float(payload["rvol_20"]),
                    "features_json": {
                        "last_close": float(payload["close"]),
                        "trend_state_fast_slow_code": (
                            int(payload["trend_state_fast_slow_code"])
                            if payload.get("trend_state_fast_slow_code") is not None and not pd.isna(payload.get("trend_state_fast_slow_code"))
                            else 0
                        ),
                        "breakout_ready_flag": (
                            int(payload["breakout_ready_flag"])
                            if payload.get("breakout_ready_flag") is not None and not pd.isna(payload.get("breakout_ready_flag"))
                            else 0
                        ),
                        "reversion_ready_flag": (
                            int(payload["reversion_ready_flag"])
                            if payload.get("reversion_ready_flag") is not None and not pd.isna(payload.get("reversion_ready_flag"))
                            else 0
                        ),
                    },
                }
            )
    return rows


def _compat_candidate_rows(
    *,
    candidate_contracts: list[dict[str, object]],
    backtest_run_id: str,
    commission_per_trade: float,
    slippage_bps: float,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for payload in candidate_contracts:
        contract = DecisionCandidate.from_dict(payload)
        estimated_slippage = abs(float(contract.entry_ref)) * (slippage_bps / 10_000.0)
        rows.append(
            {
                "candidate_id": "CAND-" + _stable_hash(
                    "|".join(
                        (
                            contract.strategy_version_id,
                            contract.contract_id,
                            contract.timeframe.value,
                            contract.ts_decision,
                        )
                    )
                ),
                "backtest_run_id": backtest_run_id,
                "strategy_version_id": contract.strategy_version_id,
                "contract_id": contract.contract_id,
                "timeframe": contract.timeframe.value,
                "ts_signal": contract.ts_decision,
                "side": contract.side.value,
                "entry_ref": contract.entry_ref,
                "stop_ref": contract.stop_ref,
                "target_ref": contract.target_ref,
                "score": contract.confidence,
                "window_id": "projection-01",
                "estimated_commission": commission_per_trade,
                "estimated_slippage": estimated_slippage,
            }
        )
    return rows


def _write_compatibility_artifacts(
    *,
    compat_output_dir: Path,
    snapshot_rows: list[dict[str, object]],
    backtest_run: dict[str, object],
    strategy_metrics: dict[str, object],
    candidate_rows: list[dict[str, object]],
) -> dict[str, str]:
    contract = phase2b_feature_store_contract()
    compat_output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = compat_output_dir / "feature.snapshots.delta"
    run_path = compat_output_dir / "research.backtest_runs.delta"
    candidate_path = compat_output_dir / "research.signal_candidates.delta"
    metrics_path = compat_output_dir / "research.strategy_metrics.delta"

    write_delta_table_rows(
        table_path=snapshot_path,
        rows=snapshot_rows,
        columns=contract["feature_snapshots"]["columns"],
    )
    write_delta_table_rows(
        table_path=run_path,
        rows=[backtest_run],
        columns=contract["research_backtest_runs"]["columns"],
    )
    write_delta_table_rows(
        table_path=candidate_path,
        rows=candidate_rows,
        columns=contract["research_signal_candidates"]["columns"],
    )
    write_delta_table_rows(
        table_path=metrics_path,
        rows=[strategy_metrics],
        columns=contract["research_strategy_metrics"]["columns"],
    )
    return {
        "feature_snapshots": snapshot_path.as_posix(),
        "backtest_runs": run_path.as_posix(),
        "signal_candidates": candidate_path.as_posix(),
        "strategy_metrics": metrics_path.as_posix(),
    }


def run_research_from_bars(
    *,
    bars: list[CanonicalBar],
    instrument_by_contract: dict[str, str],
    strategy_version_id: str,
    dataset_version: str,
    output_dir: Path,
    backtest_config: dict[str, object] | None = None,
) -> dict[str, object]:
    """Primary public research entrypoint over the materialized research plane."""
    backtest_config = backtest_config or {}
    normalized_bars = _normalized_bars(bars, instrument_by_contract)
    timeframes = _sorted_timeframes(normalized_bars)
    if not timeframes:
        raise ValueError("bars must not be empty")

    materialized_dir = output_dir / "materialized"
    research_dir = output_dir / "research"
    compat_dir = output_dir / "compat"
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
        code_version="phase2b-primary-entrypoint",
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

    strategy_registry = _compat_strategy_registry(strategy_version_id)
    all_combinations = len(strategy_registry.parameter_combinations(strategy_version_id))
    combination_count = int(backtest_config.get("combination_count", all_combinations))
    request = BacktestBatchRequest(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
        strategy_versions=(strategy_version_id,),
        combination_count=combination_count,
        param_batch_size=int(backtest_config.get("param_batch_size", min(max(combination_count, 1), 25))),
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
    ranking_policy = _compat_ranking_policy(backtest_config)
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

    representative_run = batch_report["run_rows"][0] if batch_report["run_rows"] else None
    candidate_contract_rows = [dict(payload) for payload in projection_report["candidate_contracts"]]
    compat_candidate_rows = _compat_candidate_rows(
        candidate_contracts=candidate_contract_rows,
        backtest_run_id=str(representative_run["backtest_run_id"]) if representative_run is not None else "compat-run",
        commission_per_trade=commission_per_trade,
        slippage_bps=slippage_bps,
    )
    strategy_metrics = _compat_metrics(
        candidate_rows=compat_candidate_rows,
        walk_forward_windows=walk_forward_windows,
        session_hours_utc=session_hours_utc,
        commission_per_trade=commission_per_trade,
    )
    backtest_run = _compat_backtest_run(
        strategy_version_id=strategy_version_id,
        dataset_version=dataset_version,
        candidate_rows=compat_candidate_rows,
        metrics=strategy_metrics,
        representative_run=representative_run,
    )
    snapshot_rows = _compat_snapshot_rows(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        feature_output_dir=materialized_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
        timeframe=request.timeframe,
    )
    compat_output_paths = _write_compatibility_artifacts(
        compat_output_dir=compat_dir,
        snapshot_rows=snapshot_rows,
        backtest_run=backtest_run,
        strategy_metrics=strategy_metrics,
        candidate_rows=compat_candidate_rows,
    )

    return {
        "bars_processed": len(normalized_bars),
        "feature_snapshots": len(snapshot_rows),
        "signal_contracts": len(candidate_contract_rows),
        "research_candidates": len(compat_candidate_rows),
        "backtest_run": backtest_run,
        "strategy_metrics": strategy_metrics,
        "delta_manifest": {
            **phase2b_feature_store_contract(),
            "materialized_research_datasets": phase2_research_dataset_store_contract()["research_datasets"],
            "materialized_research_bar_views": phase2_research_dataset_store_contract()["research_bar_views"],
            "materialized_research_indicator_frames": phase3_indicator_store_contract()["research_indicator_frames"],
            "materialized_research_feature_frames": phase2b_feature_store_contract()["research_feature_frames"],
            "materialized_research_backtest_batches": phase5_backtest_store_contract()["research_backtest_batches"],
            "materialized_research_backtest_runs": phase5_backtest_store_contract()["research_backtest_runs"],
            "materialized_research_strategy_stats": phase5_backtest_store_contract()["research_strategy_stats"],
            "materialized_research_trade_records": phase5_backtest_store_contract()["research_trade_records"],
            "materialized_research_order_records": phase5_backtest_store_contract()["research_order_records"],
            "materialized_research_drawdown_records": phase5_backtest_store_contract()["research_drawdown_records"],
            "materialized_research_strategy_rankings": phase6_results_store_contract()["research_strategy_rankings"],
            "materialized_research_signal_candidates": phase6_results_store_contract()["research_signal_candidates"],
        },
        "output_paths": {
            **compat_output_paths,
            "research_datasets": (materialized_dir / "research_datasets.delta").as_posix(),
            "research_bar_views": (materialized_dir / "research_bar_views.delta").as_posix(),
            "research_indicator_frames": (materialized_dir / "research_indicator_frames.delta").as_posix(),
            "research_feature_frames": (materialized_dir / "research_feature_frames.delta").as_posix(),
            "research_backtest_batches": str(batch_report["output_paths"]["research_backtest_batches"]),
            "research_backtest_runs_materialized": str(batch_report["output_paths"]["research_backtest_runs"]),
            "research_strategy_stats": str(batch_report["output_paths"]["research_strategy_stats"]),
            "research_trade_records": str(batch_report["output_paths"]["research_trade_records"]),
            "research_order_records": str(batch_report["output_paths"]["research_order_records"]),
            "research_drawdown_records": str(batch_report["output_paths"]["research_drawdown_records"]),
            "research_strategy_rankings": str(ranking_report["output_paths"]["research_strategy_rankings"]),
            "research_signal_candidates_materialized": str(projection_report["output_paths"]["research_signal_candidates"]),
        },
        "signal_contract_rows": candidate_contract_rows,
        "primary_path": "materialized_phase2b",
    }
