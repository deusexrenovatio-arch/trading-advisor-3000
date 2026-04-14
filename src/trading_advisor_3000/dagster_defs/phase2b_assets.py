from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from dagster import AssetSelection, Definitions, asset, define_asset_job, materialize

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    CandidateProjectionRequest,
    RankingPolicy,
    project_runtime_candidates,
    rank_backtest_results,
    run_backtest_batch,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    ResearchDatasetManifest,
    load_materialized_research_dataset,
    materialize_research_dataset,
    phase2_research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.features import (
    materialize_feature_frames,
    phase2b_feature_store_contract,
    reload_feature_frames,
)
from trading_advisor_3000.product_plane.research.indicators import (
    materialize_indicator_frames,
    phase3_indicator_store_contract,
    reload_indicator_frames,
)
from trading_advisor_3000.product_plane.research.strategies import build_phase1_strategy_registry

from .phase2a_assets import AssetSpec


PHASE2B_BOOTSTRAP_ASSETS = (
    "research_datasets",
    "research_bar_views",
    "research_indicator_frames",
    "research_feature_frames",
)

PHASE2B_BACKTEST_ASSETS = (
    "research_backtest_batches",
    "research_backtest_runs",
    "research_strategy_stats",
    "research_trade_records",
    "research_order_records",
    "research_drawdown_records",
    "research_strategy_rankings",
)

PHASE2B_PROJECTION_ASSETS = (
    "research_signal_candidates",
)

PHASE2B_ASSET_KEYS = (
    *PHASE2B_BOOTSTRAP_ASSETS,
    *PHASE2B_BACKTEST_ASSETS,
    *PHASE2B_PROJECTION_ASSETS,
)

PHASE2B_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "research_datasets": tuple(),
    "research_bar_views": ("research_datasets",),
    "research_indicator_frames": ("research_datasets", "research_bar_views"),
    "research_feature_frames": ("research_datasets", "research_bar_views", "research_indicator_frames"),
    "research_backtest_batches": ("research_datasets", "research_indicator_frames", "research_feature_frames"),
    "research_backtest_runs": ("research_backtest_batches",),
    "research_strategy_stats": ("research_backtest_batches",),
    "research_trade_records": ("research_backtest_batches",),
    "research_order_records": ("research_backtest_batches",),
    "research_drawdown_records": ("research_backtest_batches",),
    "research_strategy_rankings": ("research_backtest_batches", "research_strategy_stats", "research_trade_records"),
    "research_signal_candidates": ("research_datasets", "research_feature_frames", "research_strategy_rankings"),
}


def phase2b_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="research_datasets",
            description="Materialize versioned research dataset manifests from canonical bars/session calendar/roll map.",
            inputs=("canonical_bars_delta", "canonical_session_calendar_delta", "canonical_roll_map_delta"),
            outputs=("research_datasets_delta",),
        ),
        AssetSpec(
            key="research_bar_views",
            description="Materialize research-ready bar views for the selected dataset version.",
            inputs=("research_datasets_delta",),
            outputs=("research_bar_views_delta",),
        ),
        AssetSpec(
            key="research_indicator_frames",
            description="Bootstrap indicator partitions from materialized research bar views.",
            inputs=("research_datasets_delta", "research_bar_views_delta"),
            outputs=("research_indicator_frames_delta",),
        ),
        AssetSpec(
            key="research_feature_frames",
            description="Bootstrap derived feature partitions from research bar views and materialized indicators.",
            inputs=("research_datasets_delta", "research_bar_views_delta", "research_indicator_frames_delta"),
            outputs=("research_feature_frames_delta",),
        ),
        AssetSpec(
            key="research_backtest_batches",
            description="Run batched vectorbt backtests over the materialized research plane.",
            inputs=("research_datasets_delta", "research_indicator_frames_delta", "research_feature_frames_delta"),
            outputs=("research_backtest_batches_delta",),
        ),
        AssetSpec(
            key="research_backtest_runs",
            description="Expose persisted backtest run rows from the Stage 5/6 vectorbt path.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_backtest_runs_delta",),
        ),
        AssetSpec(
            key="research_strategy_stats",
            description="Expose fold/run-level strategy statistics from backtest batches.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_strategy_stats_delta",),
        ),
        AssetSpec(
            key="research_trade_records",
            description="Expose persisted trade records from vectorbt backtest batches.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_trade_records_delta",),
        ),
        AssetSpec(
            key="research_order_records",
            description="Expose persisted order records from vectorbt backtest batches.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_order_records_delta",),
        ),
        AssetSpec(
            key="research_drawdown_records",
            description="Expose persisted drawdown records from vectorbt backtest batches.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_drawdown_records_delta",),
        ),
        AssetSpec(
            key="research_strategy_rankings",
            description="Rank strategy variants over materialized backtest results using Stage 6 robustness policy.",
            inputs=("research_backtest_batches_delta", "research_strategy_stats_delta", "research_trade_records_delta"),
            outputs=("research_strategy_rankings_delta",),
        ),
        AssetSpec(
            key="research_signal_candidates",
            description="Project runtime-compatible candidates from ranked research results.",
            inputs=("research_datasets_delta", "research_feature_frames_delta", "research_strategy_rankings_delta"),
            outputs=("research_signal_candidates_delta",),
        ),
    ]


def _default_strategy_versions() -> tuple[str, ...]:
    return build_phase1_strategy_registry().strategy_versions()


def _phase2b_config_schema() -> dict[str, object]:
    return {
        "canonical_output_dir": str,
        "research_output_dir": str,
        "dataset_version": str,
        "dataset_name": str,
        "universe_id": str,
        "timeframes": [str],
        "base_timeframe": str,
        "start_ts": str,
        "end_ts": str,
        "warmup_bars": int,
        "split_method": str,
        "series_mode": str,
        "indicator_set_version": str,
        "indicator_profile_version": str,
        "feature_set_version": str,
        "feature_profile_version": str,
        "code_version": str,
        "strategy_versions": [str],
        "combination_count": int,
        "param_batch_size": int,
        "series_batch_size": int,
        "backtest_timeframe": str,
        "backtest_contract_ids": [str],
        "backtest_instrument_ids": [str],
        "fees_bps": float,
        "slippage_bps": float,
        "allow_short": bool,
        "window_count": int,
        "ranking_policy_id": str,
        "ranking_metric_order": [str],
        "require_out_of_sample_pass": bool,
        "min_trade_count": int,
        "max_drawdown_cap": float,
        "min_positive_fold_ratio": float,
        "stress_slippage_bps": float,
        "min_parameter_stability": float,
        "min_slippage_score": float,
        "selection_policy": str,
        "max_candidates_per_partition": int,
        "min_robust_score": float,
        "decision_lag_bars_max": int,
    }


def _config_value(config: dict[str, object], key: str, default: object | None = None) -> object:
    value = config.get(key, default)
    if value is None:
        raise KeyError(f"missing phase2b config value: {key}")
    return value


def _canonical_table_path(config: dict[str, object], table_name: str) -> Path:
    return Path(str(_config_value(config, "canonical_output_dir"))).resolve() / f"{table_name}.delta"


def _phase2b_output_paths(research_output_dir: Path) -> dict[str, str]:
    resolved = research_output_dir.resolve()
    return {
        "research_datasets": (resolved / "research_datasets.delta").as_posix(),
        "research_bar_views": (resolved / "research_bar_views.delta").as_posix(),
        "research_indicator_frames": (resolved / "research_indicator_frames.delta").as_posix(),
        "research_feature_frames": (resolved / "research_feature_frames.delta").as_posix(),
        "research_backtest_batches": (resolved / "research_backtest_batches.delta").as_posix(),
        "research_backtest_runs": (resolved / "research_backtest_runs.delta").as_posix(),
        "research_strategy_stats": (resolved / "research_strategy_stats.delta").as_posix(),
        "research_trade_records": (resolved / "research_trade_records.delta").as_posix(),
        "research_order_records": (resolved / "research_order_records.delta").as_posix(),
        "research_drawdown_records": (resolved / "research_drawdown_records.delta").as_posix(),
        "research_strategy_rankings": (resolved / "research_strategy_rankings.delta").as_posix(),
        "research_signal_candidates": (resolved / "research_signal_candidates.delta").as_posix(),
    }


def _load_canonical_context(config: dict[str, object]) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    bars_path = _canonical_table_path(config, "canonical_bars")
    calendar_path = _canonical_table_path(config, "canonical_session_calendar")
    roll_map_path = _canonical_table_path(config, "canonical_roll_map")
    for path in (bars_path, calendar_path, roll_map_path):
        if not has_delta_log(path):
            raise RuntimeError(f"missing canonical delta table: {path.as_posix()}")

    bars = [CanonicalBar.from_dict(row) for row in read_delta_table_rows(bars_path)]
    session_calendar = [
        SessionCalendarEntry(
            instrument_id=str(row["instrument_id"]),
            timeframe=str(row["timeframe"]),
            session_date=str(row["session_date"]),
            session_open_ts=str(row["session_open_ts"]),
            session_close_ts=str(row["session_close_ts"]),
        )
        for row in read_delta_table_rows(calendar_path)
    ]
    roll_map = [
        RollMapEntry(
            instrument_id=str(row["instrument_id"]),
            session_date=str(row["session_date"]),
            active_contract_id=str(row["active_contract_id"]),
            reason=str(row["reason"]),
        )
        for row in read_delta_table_rows(roll_map_path)
    ]
    return bars, session_calendar, roll_map


def _seed_manifest(config: dict[str, object]) -> ResearchDatasetManifest:
    return ResearchDatasetManifest(
        dataset_version=str(_config_value(config, "dataset_version")),
        dataset_name=str(_config_value(config, "dataset_name", "research-bootstrap")),
        source_table="canonical_bars",
        universe_id=str(_config_value(config, "universe_id", "moex-futures")),
        timeframes=tuple(str(item) for item in _config_value(config, "timeframes")),
        base_timeframe=str(_config_value(config, "base_timeframe", "15m")),
        start_ts=str(_config_value(config, "start_ts", "")) or None,
        end_ts=str(_config_value(config, "end_ts", "")) or None,
        series_mode=str(_config_value(config, "series_mode", "contract")),  # type: ignore[arg-type]
        split_method=str(_config_value(config, "split_method", "holdout")),  # type: ignore[arg-type]
        warmup_bars=int(_config_value(config, "warmup_bars", 200)),
        continuous_front_policy=ContinuousFrontPolicy() if str(_config_value(config, "series_mode", "contract")) == "continuous_front" else None,
        code_version=str(_config_value(config, "code_version", "dagster-phase2b-bootstrap")),
    )


@asset(group_name="phase2b", config_schema=_phase2b_config_schema())
def research_datasets(context) -> dict[str, object]:
    config = dict(context.op_execution_context.op_config)
    research_output_dir = Path(str(_config_value(config, "research_output_dir"))).resolve()
    bars, session_calendar, roll_map = _load_canonical_context(config)
    report = materialize_research_dataset(
        manifest_seed=_seed_manifest(config),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=research_output_dir,
    )
    strategy_versions = tuple(str(item) for item in _config_value(config, "strategy_versions", _default_strategy_versions()))
    return {
        "canonical_output_dir": Path(str(_config_value(config, "canonical_output_dir"))).resolve().as_posix(),
        "research_output_dir": research_output_dir.as_posix(),
        "dataset_version": str(_config_value(config, "dataset_version")),
        "indicator_set_version": str(_config_value(config, "indicator_set_version", "indicators-v1")),
        "indicator_profile_version": str(_config_value(config, "indicator_profile_version", "core_v1")),
        "feature_set_version": str(_config_value(config, "feature_set_version", "features-v1")),
        "feature_profile_version": str(_config_value(config, "feature_profile_version", "core_v1")),
        "backtest_request": {
            "strategy_versions": strategy_versions,
            "combination_count": int(_config_value(config, "combination_count", 1)),
            "param_batch_size": int(_config_value(config, "param_batch_size", 25)),
            "series_batch_size": int(_config_value(config, "series_batch_size", 4)),
            "timeframe": str(_config_value(config, "backtest_timeframe", str(_config_value(config, "base_timeframe", "15m")))),
            "contract_ids": tuple(str(item) for item in _config_value(config, "backtest_contract_ids", [])),
            "instrument_ids": tuple(str(item) for item in _config_value(config, "backtest_instrument_ids", [])),
        },
        "engine_config": {
            "fees_bps": float(_config_value(config, "fees_bps", 0.0)),
            "slippage_bps": float(_config_value(config, "slippage_bps", 0.0)),
            "allow_short": bool(_config_value(config, "allow_short", True)),
            "window_count": int(_config_value(config, "window_count", 1)),
        },
        "ranking_policy": {
            "policy_id": str(_config_value(config, "ranking_policy_id", "robust_oos_v1")),
            "metric_order": tuple(str(item) for item in _config_value(config, "ranking_metric_order", ("total_return", "profit_factor", "max_drawdown"))),
            "require_out_of_sample_pass": bool(_config_value(config, "require_out_of_sample_pass", True)),
            "min_trade_count": int(_config_value(config, "min_trade_count", 4)),
            "max_drawdown_cap": float(_config_value(config, "max_drawdown_cap", 0.35)),
            "min_positive_fold_ratio": float(_config_value(config, "min_positive_fold_ratio", 0.5)),
            "stress_slippage_bps": float(_config_value(config, "stress_slippage_bps", 7.5)),
            "min_parameter_stability": float(_config_value(config, "min_parameter_stability", 0.35)),
            "min_slippage_score": float(_config_value(config, "min_slippage_score", 0.45)),
        },
        "projection_request": {
            "selection_policy": str(_config_value(config, "selection_policy", "top_robust_per_series")),
            "max_candidates_per_partition": int(_config_value(config, "max_candidates_per_partition", 1)),
            "min_robust_score": float(_config_value(config, "min_robust_score", 0.55)),
            "decision_lag_bars_max": int(_config_value(config, "decision_lag_bars_max", 1)),
        },
        "dataset_manifest": report["dataset_manifest"],
        "output_paths": {
            **_phase2b_output_paths(research_output_dir),
            **report["output_paths"],
        },
        "delta_manifest": {
            **phase2_research_dataset_store_contract(),
            **phase3_indicator_store_contract(),
            **phase2b_feature_store_contract(),
        },
    }


@asset(group_name="phase2b")
def research_bar_views(research_datasets: dict[str, object]) -> list[dict[str, object]]:
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    loaded = load_materialized_research_dataset(
        output_dir=research_output_dir,
        dataset_version=str(research_datasets["dataset_version"]),
    )
    return [row.to_dict() for row in loaded["bar_views"]]


@asset(group_name="phase2b")
def research_indicator_frames(
    research_datasets: dict[str, object],
    research_bar_views: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_bar_views
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    dataset_version = str(research_datasets["dataset_version"])
    indicator_set_version = str(research_datasets["indicator_set_version"])
    profile_version = str(research_datasets["indicator_profile_version"])
    materialize_indicator_frames(
        dataset_output_dir=research_output_dir,
        indicator_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        profile_version=profile_version,
    )
    rows = reload_indicator_frames(
        indicator_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    )
    return [row.to_dict() for row in rows]


@asset(group_name="phase2b")
def research_feature_frames(
    research_datasets: dict[str, object],
    research_bar_views: list[dict[str, object]],
    research_indicator_frames: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_bar_views
    del research_indicator_frames
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    dataset_version = str(research_datasets["dataset_version"])
    indicator_set_version = str(research_datasets["indicator_set_version"])
    feature_set_version = str(research_datasets["feature_set_version"])
    profile_version = str(research_datasets["feature_profile_version"])
    materialize_feature_frames(
        dataset_output_dir=research_output_dir,
        indicator_output_dir=research_output_dir,
        feature_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
        profile_version=profile_version,
    )
    rows = reload_feature_frames(
        feature_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
    )
    return [row.to_dict() for row in rows]


def _backtest_request_config(research_datasets: dict[str, object]) -> BacktestBatchRequest:
    payload = dict(research_datasets["backtest_request"])
    return BacktestBatchRequest(
        dataset_version=str(research_datasets["dataset_version"]),
        indicator_set_version=str(research_datasets["indicator_set_version"]),
        feature_set_version=str(research_datasets["feature_set_version"]),
        strategy_versions=tuple(str(item) for item in payload["strategy_versions"]),
        combination_count=int(payload["combination_count"]),
        param_batch_size=int(payload["param_batch_size"]),
        series_batch_size=int(payload["series_batch_size"]),
        timeframe=str(payload["timeframe"]),
        contract_ids=tuple(str(item) for item in payload["contract_ids"]),
        instrument_ids=tuple(str(item) for item in payload["instrument_ids"]),
    )


def _engine_config(research_datasets: dict[str, object]) -> BacktestEngineConfig:
    payload = dict(research_datasets["engine_config"])
    return BacktestEngineConfig(
        fees_bps=float(payload["fees_bps"]),
        slippage_bps=float(payload["slippage_bps"]),
        allow_short=bool(payload["allow_short"]),
        window_count=int(payload["window_count"]),
    )


def _ranking_policy(research_datasets: dict[str, object]) -> RankingPolicy:
    payload = dict(research_datasets["ranking_policy"])
    return RankingPolicy(
        policy_id=str(payload["policy_id"]),
        metric_order=tuple(str(item) for item in payload["metric_order"]),
        require_out_of_sample_pass=bool(payload["require_out_of_sample_pass"]),
        min_trade_count=int(payload["min_trade_count"]),
        max_drawdown_cap=float(payload["max_drawdown_cap"]),
        min_positive_fold_ratio=float(payload["min_positive_fold_ratio"]),
        stress_slippage_bps=float(payload["stress_slippage_bps"]),
        min_parameter_stability=float(payload["min_parameter_stability"]),
        min_slippage_score=float(payload["min_slippage_score"]),
    )


def _projection_request(research_datasets: dict[str, object]) -> CandidateProjectionRequest:
    payload = dict(research_datasets["projection_request"])
    return CandidateProjectionRequest(
        ranking_policy_id=str(research_datasets["ranking_policy"]["policy_id"]),
        selection_policy=str(payload["selection_policy"]),
        max_candidates_per_partition=int(payload["max_candidates_per_partition"]),
        min_robust_score=float(payload["min_robust_score"]),
        decision_lag_bars_max=int(payload["decision_lag_bars_max"]),
    )


@asset(group_name="phase2b")
def research_backtest_batches(
    research_datasets: dict[str, object],
    research_indicator_frames: list[dict[str, object]],
    research_feature_frames: list[dict[str, object]],
) -> dict[str, object]:
    del research_indicator_frames
    del research_feature_frames
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    report = run_backtest_batch(
        dataset_output_dir=research_output_dir,
        indicator_output_dir=research_output_dir,
        feature_output_dir=research_output_dir,
        output_dir=research_output_dir,
        request=_backtest_request_config(research_datasets),
        engine_config=_engine_config(research_datasets),
    )
    return {
        **report,
        "research_output_dir": research_output_dir.as_posix(),
    }


@asset(group_name="phase2b")
def research_backtest_runs(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["run_rows"]]


@asset(group_name="phase2b")
def research_strategy_stats(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["stat_rows"]]


@asset(group_name="phase2b")
def research_trade_records(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["trade_rows"]]


@asset(group_name="phase2b")
def research_order_records(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["order_rows"]]


@asset(group_name="phase2b")
def research_drawdown_records(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["drawdown_rows"]]


@asset(group_name="phase2b")
def research_strategy_rankings(
    research_datasets: dict[str, object],
    research_backtest_batches: dict[str, object],
    research_strategy_stats: list[dict[str, object]],
    research_trade_records: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_backtest_batches
    del research_strategy_stats
    del research_trade_records
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    report = rank_backtest_results(
        backtest_output_dir=research_output_dir,
        output_dir=research_output_dir,
        policy=_ranking_policy(research_datasets),
    )
    return [dict(row) for row in report["ranking_rows"]]


@asset(group_name="phase2b")
def research_signal_candidates(
    research_datasets: dict[str, object],
    research_feature_frames: list[dict[str, object]],
    research_strategy_rankings: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_feature_frames
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    report = project_runtime_candidates(
        dataset_output_dir=research_output_dir,
        indicator_output_dir=research_output_dir,
        feature_output_dir=research_output_dir,
        output_dir=research_output_dir,
        request=_projection_request(research_datasets),
        ranking_rows=[dict(row) for row in research_strategy_rankings],
        config=_engine_config(research_datasets),
    )
    return [dict(row) for row in report["candidate_rows"]]


PHASE2B_ASSETS = (
    research_datasets,
    research_bar_views,
    research_indicator_frames,
    research_feature_frames,
    research_backtest_batches,
    research_backtest_runs,
    research_strategy_stats,
    research_trade_records,
    research_order_records,
    research_drawdown_records,
    research_strategy_rankings,
    research_signal_candidates,
)

phase2b_bootstrap_job = define_asset_job(
    name="phase2b_bootstrap_job",
    selection=AssetSelection.assets(
        research_datasets,
        research_bar_views,
        research_indicator_frames,
        research_feature_frames,
    ),
)

phase2b_backtest_job = define_asset_job(
    name="phase2b_backtest_job",
    selection=AssetSelection.assets(
        research_backtest_batches,
        research_backtest_runs,
        research_strategy_stats,
        research_trade_records,
        research_order_records,
        research_drawdown_records,
        research_strategy_rankings,
    ),
)

phase2b_projection_job = define_asset_job(
    name="phase2b_projection_job",
    selection=AssetSelection.assets(research_signal_candidates),
)

phase2b_definitions = Definitions(
    assets=list(PHASE2B_ASSETS),
    jobs=[phase2b_bootstrap_job, phase2b_backtest_job, phase2b_projection_job],
)


def assert_phase2b_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or phase2b_definitions
    repository = defs.get_repository_def()
    expected_by_job = {
        "phase2b_bootstrap_job": set(PHASE2B_BOOTSTRAP_ASSETS),
        "phase2b_backtest_job": set(PHASE2B_BACKTEST_ASSETS),
        "phase2b_projection_job": set(PHASE2B_PROJECTION_ASSETS),
    }
    for job_name, expected_assets in expected_by_job.items():
        job = repository.get_job(job_name)
        actual_nodes = set(job.graph.node_dict.keys())
        missing_nodes = sorted(expected_assets - actual_nodes)
        if missing_nodes:
            raise RuntimeError(
                "phase2b Dagster definitions are metadata-only or incomplete: "
                f"job `{job_name}` missing executable asset nodes: {', '.join(missing_nodes)}"
            )


def build_phase2b_definitions() -> Definitions:
    assert_phase2b_definitions_executable(phase2b_definitions)
    return phase2b_definitions


def _resolve_selected_assets(
    selection: Sequence[str] | None,
    *,
    default_assets: Sequence[str],
    allowed_assets: Sequence[str],
) -> list[str]:
    if selection is None:
        return list(default_assets)
    normalized = sorted({item.strip() for item in selection if item.strip()})
    if not normalized:
        raise ValueError("selection must include at least one asset key")
    unknown = [item for item in normalized if item not in allowed_assets]
    if unknown:
        raise ValueError(f"unknown phase2b asset selection: {', '.join(sorted(unknown))}")
    return normalized


def _resolve_expected_materialization(selection: Sequence[str]) -> list[str]:
    resolved: set[str] = set()

    def _visit(asset_name: str) -> None:
        if asset_name in resolved:
            return
        for dependency in PHASE2B_DEPENDENCIES.get(asset_name, tuple()):
            _visit(dependency)
        resolved.add(asset_name)

    for asset_name in selection:
        _visit(asset_name)
    return [name for name in PHASE2B_ASSET_KEYS if name in resolved]


def _phase2b_run_config(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: Sequence[str],
    indicator_set_version: str,
    indicator_profile_version: str,
    feature_set_version: str,
    feature_profile_version: str,
    strategy_versions: Sequence[str] | None = None,
    combination_count: int = 1,
    param_batch_size: int = 25,
    series_batch_size: int = 4,
    backtest_timeframe: str = "",
    backtest_contract_ids: Sequence[str] = (),
    backtest_instrument_ids: Sequence[str] = (),
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
    allow_short: bool = True,
    window_count: int = 1,
    ranking_policy_id: str = "robust_oos_v1",
    ranking_metric_order: Sequence[str] = ("total_return", "profit_factor", "max_drawdown"),
    require_out_of_sample_pass: bool = True,
    min_trade_count: int = 4,
    max_drawdown_cap: float = 0.35,
    min_positive_fold_ratio: float = 0.5,
    stress_slippage_bps: float = 7.5,
    min_parameter_stability: float = 0.35,
    min_slippage_score: float = 0.45,
    selection_policy: str = "top_robust_per_series",
    max_candidates_per_partition: int = 1,
    min_robust_score: float = 0.55,
    decision_lag_bars_max: int = 1,
) -> dict[str, object]:
    resolved_strategy_versions = tuple(str(item) for item in (strategy_versions or _default_strategy_versions()))
    resolved_timeframes = [str(item) for item in timeframes]
    return {
        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
        "research_output_dir": research_output_dir.resolve().as_posix(),
        "dataset_version": dataset_version,
        "dataset_name": "research-bootstrap",
        "universe_id": "moex-futures",
        "timeframes": resolved_timeframes,
        "base_timeframe": resolved_timeframes[0] if resolved_timeframes else "15m",
        "start_ts": "",
        "end_ts": "",
        "warmup_bars": 200,
        "split_method": "holdout",
        "series_mode": "contract",
        "indicator_set_version": indicator_set_version,
        "indicator_profile_version": indicator_profile_version,
        "feature_set_version": feature_set_version,
        "feature_profile_version": feature_profile_version,
        "code_version": "dagster-phase2b-orchestration",
        "strategy_versions": list(resolved_strategy_versions),
        "combination_count": combination_count,
        "param_batch_size": param_batch_size,
        "series_batch_size": series_batch_size,
        "backtest_timeframe": backtest_timeframe,
        "backtest_contract_ids": [str(item) for item in backtest_contract_ids],
        "backtest_instrument_ids": [str(item) for item in backtest_instrument_ids],
        "fees_bps": fees_bps,
        "slippage_bps": slippage_bps,
        "allow_short": allow_short,
        "window_count": window_count,
        "ranking_policy_id": ranking_policy_id,
        "ranking_metric_order": [str(item) for item in ranking_metric_order],
        "require_out_of_sample_pass": require_out_of_sample_pass,
        "min_trade_count": min_trade_count,
        "max_drawdown_cap": max_drawdown_cap,
        "min_positive_fold_ratio": min_positive_fold_ratio,
        "stress_slippage_bps": stress_slippage_bps,
        "min_parameter_stability": min_parameter_stability,
        "min_slippage_score": min_slippage_score,
        "selection_policy": selection_policy,
        "max_candidates_per_partition": max_candidates_per_partition,
        "min_robust_score": min_robust_score,
        "decision_lag_bars_max": decision_lag_bars_max,
    }


def _materialize_phase2b_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: Sequence[str],
    default_assets: Sequence[str],
    allowed_assets: Sequence[str],
    selection: Sequence[str] | None = None,
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    strategy_versions: Sequence[str] | None = None,
    combination_count: int = 1,
    param_batch_size: int = 25,
    series_batch_size: int = 4,
    backtest_timeframe: str = "",
    backtest_contract_ids: Sequence[str] = (),
    backtest_instrument_ids: Sequence[str] = (),
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
    allow_short: bool = True,
    window_count: int = 1,
    ranking_policy_id: str = "robust_oos_v1",
    ranking_metric_order: Sequence[str] = ("total_return", "profit_factor", "max_drawdown"),
    require_out_of_sample_pass: bool = True,
    min_trade_count: int = 4,
    max_drawdown_cap: float = 0.35,
    min_positive_fold_ratio: float = 0.5,
    stress_slippage_bps: float = 7.5,
    min_parameter_stability: float = 0.35,
    min_slippage_score: float = 0.45,
    selection_policy: str = "top_robust_per_series",
    max_candidates_per_partition: int = 1,
    min_robust_score: float = 0.55,
    decision_lag_bars_max: int = 1,
    raise_on_error: bool = True,
) -> dict[str, object]:
    assert_phase2b_definitions_executable()
    selected_assets = _resolve_selected_assets(selection, default_assets=default_assets, allowed_assets=allowed_assets)
    expected_materialized_assets = _resolve_expected_materialization(selected_assets)
    research_output_dir = research_output_dir.resolve()
    output_paths = _phase2b_output_paths(research_output_dir)

    result = materialize(
        assets=list(PHASE2B_ASSETS),
        selection=expected_materialized_assets,
        run_config={
            "ops": {
                "research_datasets": {
                    "config": _phase2b_run_config(
                        canonical_output_dir=canonical_output_dir,
                        research_output_dir=research_output_dir,
                        dataset_version=dataset_version,
                        timeframes=timeframes,
                        indicator_set_version=indicator_set_version,
                        indicator_profile_version=indicator_profile_version,
                        feature_set_version=feature_set_version,
                        feature_profile_version=feature_profile_version,
                        strategy_versions=strategy_versions,
                        combination_count=combination_count,
                        param_batch_size=param_batch_size,
                        series_batch_size=series_batch_size,
                        backtest_timeframe=backtest_timeframe,
                        backtest_contract_ids=backtest_contract_ids,
                        backtest_instrument_ids=backtest_instrument_ids,
                        fees_bps=fees_bps,
                        slippage_bps=slippage_bps,
                        allow_short=allow_short,
                        window_count=window_count,
                        ranking_policy_id=ranking_policy_id,
                        ranking_metric_order=ranking_metric_order,
                        require_out_of_sample_pass=require_out_of_sample_pass,
                        min_trade_count=min_trade_count,
                        max_drawdown_cap=max_drawdown_cap,
                        min_positive_fold_ratio=min_positive_fold_ratio,
                        stress_slippage_bps=stress_slippage_bps,
                        min_parameter_stability=min_parameter_stability,
                        min_slippage_score=min_slippage_score,
                        selection_policy=selection_policy,
                        max_candidates_per_partition=max_candidates_per_partition,
                        min_robust_score=min_robust_score,
                        decision_lag_bars_max=decision_lag_bars_max,
                    )
                }
            }
        },
        raise_on_error=raise_on_error,
    )

    report: dict[str, object] = {
        "success": bool(result.success),
        "selected_assets": selected_assets,
        "materialized_assets": expected_materialized_assets,
        "output_paths": output_paths,
    }
    if not result.success:
        report["rows_by_table"] = {}
        return report

    rows_by_table: dict[str, int] = {}
    for asset_name in expected_materialized_assets:
        table_path = Path(output_paths[asset_name])
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing `_delta_log` for `{asset_name}` at {table_path.as_posix()}")
        rows_by_table[asset_name] = len(read_delta_table_rows(table_path))
    report["rows_by_table"] = rows_by_table
    return report


def materialize_phase2b_bootstrap_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: Sequence[str],
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    return _materialize_phase2b_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        dataset_version=dataset_version,
        timeframes=timeframes,
        default_assets=PHASE2B_BOOTSTRAP_ASSETS,
        allowed_assets=PHASE2B_BOOTSTRAP_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        raise_on_error=raise_on_error,
    )


def materialize_phase2b_backtest_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: Sequence[str],
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    strategy_versions: Sequence[str] | None = None,
    combination_count: int = 1,
    param_batch_size: int = 25,
    series_batch_size: int = 4,
    backtest_timeframe: str = "",
    require_out_of_sample_pass: bool = True,
    min_trade_count: int = 4,
    max_drawdown_cap: float = 0.35,
    min_positive_fold_ratio: float = 0.5,
    stress_slippage_bps: float = 7.5,
    min_parameter_stability: float = 0.35,
    min_slippage_score: float = 0.45,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    return _materialize_phase2b_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        dataset_version=dataset_version,
        timeframes=timeframes,
        default_assets=PHASE2B_BACKTEST_ASSETS,
        allowed_assets=PHASE2B_BACKTEST_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        strategy_versions=strategy_versions,
        combination_count=combination_count,
        param_batch_size=param_batch_size,
        series_batch_size=series_batch_size,
        backtest_timeframe=backtest_timeframe,
        require_out_of_sample_pass=require_out_of_sample_pass,
        min_trade_count=min_trade_count,
        max_drawdown_cap=max_drawdown_cap,
        min_positive_fold_ratio=min_positive_fold_ratio,
        stress_slippage_bps=stress_slippage_bps,
        min_parameter_stability=min_parameter_stability,
        min_slippage_score=min_slippage_score,
        raise_on_error=raise_on_error,
    )


def materialize_phase2b_projection_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: Sequence[str],
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    strategy_versions: Sequence[str] | None = None,
    combination_count: int = 1,
    param_batch_size: int = 25,
    series_batch_size: int = 4,
    backtest_timeframe: str = "",
    selection_policy: str = "top_robust_per_series",
    max_candidates_per_partition: int = 1,
    min_robust_score: float = 0.55,
    decision_lag_bars_max: int = 1,
    require_out_of_sample_pass: bool = True,
    min_trade_count: int = 4,
    max_drawdown_cap: float = 0.35,
    min_positive_fold_ratio: float = 0.5,
    stress_slippage_bps: float = 7.5,
    min_parameter_stability: float = 0.35,
    min_slippage_score: float = 0.45,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    return _materialize_phase2b_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        dataset_version=dataset_version,
        timeframes=timeframes,
        default_assets=PHASE2B_PROJECTION_ASSETS,
        allowed_assets=PHASE2B_PROJECTION_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        strategy_versions=strategy_versions,
        combination_count=combination_count,
        param_batch_size=param_batch_size,
        series_batch_size=series_batch_size,
        backtest_timeframe=backtest_timeframe,
        selection_policy=selection_policy,
        max_candidates_per_partition=max_candidates_per_partition,
        min_robust_score=min_robust_score,
        decision_lag_bars_max=decision_lag_bars_max,
        require_out_of_sample_pass=require_out_of_sample_pass,
        min_trade_count=min_trade_count,
        max_drawdown_cap=max_drawdown_cap,
        min_positive_fold_ratio=min_positive_fold_ratio,
        stress_slippage_bps=stress_slippage_bps,
        min_parameter_stability=min_parameter_stability,
        min_slippage_score=min_slippage_score,
        raise_on_error=raise_on_error,
    )
