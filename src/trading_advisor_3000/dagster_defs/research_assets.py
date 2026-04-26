from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    asset,
    define_asset_job,
    materialize,
    run_status_sensor,
)

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
    MOEX_HISTORICAL_DATA_ROOT_ENV,
)
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    BacktestStrategyInstance,
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
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.derived_indicators import (
    DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    materialize_derived_indicator_frames,
    reload_derived_indicator_frames,
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import (
    materialize_indicator_frames,
    indicator_store_contract,
    reload_indicator_frames,
)
from trading_advisor_3000.product_plane.research.registry_store import registry_output_paths, research_registry_root
from trading_advisor_3000.product_plane.research.strategy_space import prepare_strategy_space
from trading_advisor_3000.product_plane.research.strategies.families import phase_stg02_family_adapters

from .moex_historical_assets import MOEX_BASELINE_UPDATE_JOB_NAME, moex_baseline_update_job
from .historical_data_proof_assets import AssetSpec


RESEARCH_DATA_PREP_JOB_NAME = "research_data_prep_job"
STRATEGY_REGISTRY_REFRESH_JOB_NAME = "strategy_registry_refresh_job"
RESEARCH_BACKTEST_JOB_NAME = "research_backtest_job"
RESEARCH_PROJECTION_JOB_NAME = "research_projection_job"
RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME = "research_data_prep_after_moex_sensor"

RESEARCH_DATA_PREP_CANONICAL_OUTPUT_DIR_ENV = "TA3000_RESEARCH_DATA_PREP_CANONICAL_OUTPUT_DIR"
RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR_ENV = "TA3000_RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR"
RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR_ENV = "TA3000_RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR"
RESEARCH_DATA_PREP_DATASET_VERSION_ENV = "TA3000_RESEARCH_DATA_PREP_DATASET_VERSION"
RESEARCH_DATA_PREP_TIMEFRAMES_ENV = "TA3000_RESEARCH_DATA_PREP_TIMEFRAMES"

DEFAULT_MOEX_HISTORICAL_DATA_ROOT = Path("D:/TA3000-data/trading-advisor-3000-nightly")
DEFAULT_RESEARCH_DATA_PREP_CANONICAL_OUTPUT_DIR = (
    DEFAULT_MOEX_HISTORICAL_DATA_ROOT / CANONICAL_BASELINE_ROOT_RELATIVE_PATH
)
DEFAULT_RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR = (
    DEFAULT_MOEX_HISTORICAL_DATA_ROOT / "research" / "gold" / "current"
)
DEFAULT_RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR = (
    DEFAULT_MOEX_HISTORICAL_DATA_ROOT / "research" / "runs" / "data-prep"
)
DEFAULT_RESEARCH_DATA_PREP_DATASET_VERSION = "moex_research_current"
DEFAULT_RESEARCH_DATA_PREP_DATASET_NAME = "moex-research-current"
DEFAULT_RESEARCH_DATA_PREP_TIMEFRAMES = ("15m", "1h", "4h", "1d")
DEFAULT_RESEARCH_DATA_PREP_BASE_TIMEFRAME = "15m"
DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS = 300

RESEARCH_DATA_PREP_ASSETS = (
    "research_datasets",
    "research_instrument_tree",
    "research_bar_views",
    "research_indicator_frames",
    "research_derived_indicator_frames",
)

STRATEGY_REGISTRY_REFRESH_ASSETS = (
    "research_strategy_families",
    "research_strategy_templates",
    "research_strategy_template_modules",
    "research_strategy_instances",
    "research_strategy_instance_modules",
)

STRATEGY_REGISTRY_REFRESH_EXECUTION_ASSETS = (
    "research_datasets",
    *STRATEGY_REGISTRY_REFRESH_ASSETS,
)

RESEARCH_BACKTEST_ASSETS = (
    "research_backtest_batches",
    "research_backtest_runs",
    "research_strategy_stats",
    "research_trade_records",
    "research_order_records",
    "research_drawdown_records",
    "research_strategy_rankings",
)

RESEARCH_PROJECTION_ASSETS = (
    "research_signal_candidates",
)

RESEARCH_ASSET_KEYS = (
    *RESEARCH_DATA_PREP_ASSETS,
    *STRATEGY_REGISTRY_REFRESH_ASSETS,
    *RESEARCH_BACKTEST_ASSETS,
    *RESEARCH_PROJECTION_ASSETS,
)

RESEARCH_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "research_datasets": tuple(),
    "research_instrument_tree": ("research_datasets",),
    "research_bar_views": ("research_datasets",),
    "research_indicator_frames": ("research_datasets", "research_bar_views"),
    "research_derived_indicator_frames": ("research_datasets", "research_bar_views", "research_indicator_frames"),
    "research_strategy_families": ("research_datasets",),
    "research_strategy_templates": ("research_strategy_families",),
    "research_strategy_template_modules": ("research_strategy_templates",),
    "research_strategy_instances": ("research_strategy_template_modules",),
    "research_strategy_instance_modules": ("research_strategy_instances",),
    "research_backtest_batches": (
        "research_datasets",
        "research_indicator_frames",
        "research_derived_indicator_frames",
        "research_strategy_instance_modules",
    ),
    "research_backtest_runs": ("research_backtest_batches",),
    "research_strategy_stats": ("research_backtest_batches",),
    "research_trade_records": ("research_backtest_batches",),
    "research_order_records": ("research_backtest_batches",),
    "research_drawdown_records": ("research_backtest_batches",),
    "research_strategy_rankings": ("research_backtest_batches", "research_strategy_stats", "research_trade_records"),
    "research_signal_candidates": ("research_datasets", "research_derived_indicator_frames", "research_strategy_rankings"),
}


def research_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="research_datasets",
            description="Materialize versioned research dataset manifests from canonical bars/session calendar/roll map.",
            inputs=("canonical_bars_delta", "canonical_session_calendar_delta", "canonical_roll_map_delta"),
            outputs=("research_datasets_delta",),
        ),
        AssetSpec(
            key="research_instrument_tree",
            description="Materialize the dataset-scoped instrument tree with instrument, contract, timeframe, and lineage coverage.",
            inputs=("research_datasets_delta",),
            outputs=("research_instrument_tree_delta",),
        ),
        AssetSpec(
            key="research_bar_views",
            description="Materialize research-ready bar views for the selected dataset version.",
            inputs=("research_datasets_delta",),
            outputs=("research_bar_views_delta",),
        ),
        AssetSpec(
            key="research_indicator_frames",
            description="Materialize indicator partitions from materialized research bar views.",
            inputs=("research_datasets_delta", "research_bar_views_delta"),
            outputs=("research_indicator_frames_delta",),
        ),
        AssetSpec(
            key="research_derived_indicator_frames",
            description="Materialize causal technical relationships from research bars and base indicators.",
            inputs=("research_datasets_delta", "research_bar_views_delta", "research_indicator_frames_delta"),
            outputs=("research_derived_indicator_frames_delta",),
        ),
        AssetSpec(
            key="research_strategy_families",
            description="Materialize Stage 2 strategy family registry rows from the frozen adapter inventory.",
            inputs=("research_datasets_delta",),
            outputs=("research_strategy_families_delta",),
        ),
        AssetSpec(
            key="research_strategy_templates",
            description="Materialize Stage 2 strategy template registry rows emitted by compiler adapters.",
            inputs=("research_strategy_families_delta",),
            outputs=("research_strategy_templates_delta",),
        ),
        AssetSpec(
            key="research_strategy_template_modules",
            description="Materialize Stage 2 strategy template-module rows with deterministic adapter provenance.",
            inputs=("research_strategy_templates_delta",),
            outputs=("research_strategy_template_modules_delta",),
        ),
        AssetSpec(
            key="research_strategy_instances",
            description="Expose concrete strategy instances materialized from strategy_space into the research registry root.",
            inputs=("research_strategy_template_modules_delta",),
            outputs=("research_strategy_instances_delta",),
        ),
        AssetSpec(
            key="research_strategy_instance_modules",
            description="Expose flattened module graphs for concrete strategy instances.",
            inputs=("research_strategy_instances_delta",),
            outputs=("research_strategy_instance_modules_delta",),
        ),
        AssetSpec(
            key="research_backtest_batches",
            description="Run batched vectorbt backtests over the materialized research plane.",
            inputs=(
                "research_datasets_delta",
                "research_indicator_frames_delta",
                "research_derived_indicator_frames_delta",
                "research_strategy_instance_modules_delta",
            ),
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
            inputs=("research_datasets_delta", "research_derived_indicator_frames_delta", "research_strategy_rankings_delta"),
            outputs=("research_signal_candidates_delta",),
        ),
    ]


def _default_strategy_space() -> dict[str, object]:
    return {
        "family_keys": [adapter.family_manifest.family_key for adapter in phase_stg02_family_adapters()],
        "template_ids": [],
        "include_instance_ids": [],
        "exclude_manifest_hashes": [],
        "materialize_instances": True,
        "max_instance_count": 5000,
        "search_space_overrides": {},
    }


def _research_config_schema() -> dict[str, object]:
    return {
        "canonical_output_dir": str,
        "registry_root": str,
        "materialized_output_dir": str,
        "results_output_dir": str,
        "reuse_existing_materialization": bool,
        "campaign_run_id": str,
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
        "dataset_contract_ids": [str],
        "dataset_instrument_ids": [str],
        "indicator_set_version": str,
        "indicator_profile_version": str,
        "derived_indicator_set_version": str,
        "derived_indicator_profile_version": str,
        "feature_set_version": str,
        "feature_profile_version": str,
        "code_version": str,
        "strategy_space": dict,
        "strategy_space_id": str,
        "strategy_instances": [dict],
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
        raise KeyError(f"missing research config value: {key}")
    return value


def _canonical_table_path(config: dict[str, object], table_name: str) -> Path:
    return Path(str(_config_value(config, "canonical_output_dir"))).resolve() / f"{table_name}.delta"


def _resolve_research_output_dirs(
    *,
    research_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
) -> tuple[Path, Path]:
    if research_output_dir is not None:
        if materialized_output_dir is not None or results_output_dir is not None:
            raise ValueError(
                "research_output_dir cannot be combined with materialized_output_dir/results_output_dir"
            )
        resolved_root = research_output_dir.resolve()
        return resolved_root, resolved_root
    if materialized_output_dir is None or results_output_dir is None:
        raise ValueError(
            "materialized_output_dir and results_output_dir are both required when research_output_dir is omitted"
        )
    return materialized_output_dir.resolve(), results_output_dir.resolve()


def _research_output_paths(*, registry_root: Path, materialized_output_dir: Path, results_output_dir: Path) -> dict[str, str]:
    resolved_materialized = materialized_output_dir.resolve()
    resolved_results = results_output_dir.resolve()
    return {
        **{
            table_name: path
            for table_name, path in registry_output_paths(registry_root=registry_root).items()
            if table_name.startswith("research_strategy_")
        },
        "research_datasets": (resolved_materialized / "research_datasets.delta").as_posix(),
        "research_instrument_tree": (resolved_materialized / "research_instrument_tree.delta").as_posix(),
        "research_bar_views": (resolved_materialized / "research_bar_views.delta").as_posix(),
        "research_indicator_frames": (resolved_materialized / "research_indicator_frames.delta").as_posix(),
        "research_derived_indicator_frames": (resolved_materialized / "research_derived_indicator_frames.delta").as_posix(),
        "research_backtest_batches": (resolved_results / "research_backtest_batches.delta").as_posix(),
        "research_backtest_runs": (resolved_results / "research_backtest_runs.delta").as_posix(),
        "research_strategy_stats": (resolved_results / "research_strategy_stats.delta").as_posix(),
        "research_trade_records": (resolved_results / "research_trade_records.delta").as_posix(),
        "research_order_records": (resolved_results / "research_order_records.delta").as_posix(),
        "research_drawdown_records": (resolved_results / "research_drawdown_records.delta").as_posix(),
        "research_strategy_rankings": (resolved_results / "research_strategy_rankings.delta").as_posix(),
        "research_signal_candidates": (resolved_results / "research_signal_candidates.delta").as_posix(),
        "research_run_findings": (resolved_results / "research_run_findings.delta").as_posix(),
    }


def _reuse_existing_materialization(config: dict[str, object]) -> bool:
    return bool(_config_value(config, "reuse_existing_materialization", False))


def _materialized_output_dir(config: dict[str, object]) -> Path:
    return Path(str(_config_value(config, "materialized_output_dir"))).resolve()


def _results_output_dir(config: dict[str, object]) -> Path:
    return Path(str(_config_value(config, "results_output_dir"))).resolve()


def _require_existing_data_prep(
    *,
    materialized_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
) -> None:
    dataset_path = materialized_output_dir / "research_datasets.delta"
    instrument_tree_path = materialized_output_dir / "research_instrument_tree.delta"
    bar_views_path = materialized_output_dir / "research_bar_views.delta"
    indicator_path = materialized_output_dir / "research_indicator_frames.delta"
    derived_indicator_path = materialized_output_dir / "research_derived_indicator_frames.delta"
    for path in (
        dataset_path,
        instrument_tree_path,
        bar_views_path,
        indicator_path,
        derived_indicator_path,
    ):
        if not has_delta_log(path):
            raise RuntimeError(f"missing reusable research data prep table: {path.as_posix()}")

    load_materialized_research_dataset(output_dir=materialized_output_dir, dataset_version=dataset_version)
    reload_indicator_frames(
        indicator_output_dir=materialized_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    )
    reload_derived_indicator_frames(
        derived_indicator_output_dir=materialized_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
    )


def _load_canonical_context(config: dict[str, object]) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    bars_path = _canonical_table_path(config, "canonical_bars")
    calendar_path = _canonical_table_path(config, "canonical_session_calendar")
    roll_map_path = _canonical_table_path(config, "canonical_roll_map")
    for path in (bars_path, calendar_path, roll_map_path):
        if not has_delta_log(path):
            raise RuntimeError(f"missing canonical delta table: {path.as_posix()}")

    contract_ids = {str(item) for item in _config_value(config, "dataset_contract_ids", [])}
    instrument_ids = {str(item) for item in _config_value(config, "dataset_instrument_ids", [])}

    bars = [
        CanonicalBar.from_dict(row)
        for row in read_delta_table_rows(bars_path)
        if (
            (not contract_ids or str(row.get("contract_id")) in contract_ids)
            and (not instrument_ids or str(row.get("instrument_id")) in instrument_ids)
        )
    ]
    session_calendar = [
        SessionCalendarEntry(
            instrument_id=str(row["instrument_id"]),
            timeframe=str(row["timeframe"]),
            session_date=str(row["session_date"]),
            session_open_ts=str(row["session_open_ts"]),
            session_close_ts=str(row["session_close_ts"]),
        )
        for row in read_delta_table_rows(calendar_path)
        if not instrument_ids or str(row.get("instrument_id")) in instrument_ids
    ]
    roll_map = [
        RollMapEntry(
            instrument_id=str(row["instrument_id"]),
            session_date=str(row["session_date"]),
            active_contract_id=str(row["active_contract_id"]),
            reason=str(row["reason"]),
        )
        for row in read_delta_table_rows(roll_map_path)
        if not instrument_ids or str(row.get("instrument_id")) in instrument_ids
    ]
    return bars, session_calendar, roll_map


def _seed_manifest(config: dict[str, object]) -> ResearchDatasetManifest:
    return ResearchDatasetManifest(
        dataset_version=str(_config_value(config, "dataset_version")),
        dataset_name=str(_config_value(config, "dataset_name", "research-materialized")),
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
        code_version=str(_config_value(config, "code_version", "research-data-prep")),
    )


@asset(group_name="research", config_schema=_research_config_schema())
def research_datasets(context) -> dict[str, object]:
    config = dict(context.op_execution_context.op_config)
    registry_root = Path(str(_config_value(config, "registry_root"))).resolve()
    materialized_output_dir = _materialized_output_dir(config)
    results_output_dir = _results_output_dir(config)
    dataset_version = str(_config_value(config, "dataset_version"))
    indicator_set_version = str(_config_value(config, "indicator_set_version", "indicators-v1"))
    derived_indicator_set_version = str(
        _config_value(config, "derived_indicator_set_version", DEFAULT_DERIVED_INDICATOR_SET_VERSION)
    )
    feature_set_version = str(_config_value(config, "feature_set_version", "features-v1"))

    if _reuse_existing_materialization(config):
        _require_existing_data_prep(
            materialized_output_dir=materialized_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
        )
        loaded = load_materialized_research_dataset(
            output_dir=materialized_output_dir,
            dataset_version=dataset_version,
        )
        report = {
            "dataset_manifest": dict(loaded["dataset_manifest"]),
            "instrument_tree_count": len(loaded.get("instrument_tree", [])),
            "bar_view_count": len(loaded["bar_views"]),
            "output_paths": {
                "research_datasets": (materialized_output_dir / "research_datasets.delta").as_posix(),
                "research_instrument_tree": (materialized_output_dir / "research_instrument_tree.delta").as_posix(),
                "research_bar_views": (materialized_output_dir / "research_bar_views.delta").as_posix(),
            },
        }
    else:
        bars, session_calendar, roll_map = _load_canonical_context(config)
        report = materialize_research_dataset(
            manifest_seed=_seed_manifest(config),
            bars=bars,
            session_calendar=session_calendar,
            roll_map=roll_map,
            output_dir=materialized_output_dir,
        )
    return {
        "canonical_output_dir": Path(str(_config_value(config, "canonical_output_dir"))).resolve().as_posix(),
        "registry_root": registry_root.as_posix(),
        "materialized_output_dir": materialized_output_dir.as_posix(),
        "results_output_dir": results_output_dir.as_posix(),
        "reuse_existing_materialization": _reuse_existing_materialization(config),
        "campaign_run_id": str(_config_value(config, "campaign_run_id")),
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "indicator_profile_version": str(_config_value(config, "indicator_profile_version", "core_v1")),
        "derived_indicator_set_version": derived_indicator_set_version,
        "derived_indicator_profile_version": str(_config_value(config, "derived_indicator_profile_version", "core_v1")),
        "feature_set_version": feature_set_version,
        "feature_profile_version": str(_config_value(config, "feature_profile_version", "core_v1")),
        "backtest_request": {
            "strategy_space_id": str(_config_value(config, "strategy_space_id")),
            "strategy_instances": [dict(item) for item in _config_value(config, "strategy_instances", [])],
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
            **_research_output_paths(
                registry_root=registry_root,
                materialized_output_dir=materialized_output_dir,
                results_output_dir=results_output_dir,
            ),
            **report["output_paths"],
        },
        "delta_manifest": {
            **research_dataset_store_contract(),
            **indicator_store_contract(),
            **research_derived_indicator_store_contract(),
        },
    }


@asset(group_name="research")
def research_instrument_tree(research_datasets: dict[str, object]) -> list[dict[str, object]]:
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    dataset_version = str(research_datasets["dataset_version"])
    rows = read_delta_table_rows(materialized_output_dir / "research_instrument_tree.delta")
    return [dict(row) for row in rows if row.get("dataset_version") == dataset_version]


@asset(group_name="research")
def research_bar_views(research_datasets: dict[str, object]) -> list[dict[str, object]]:
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    loaded = load_materialized_research_dataset(
        output_dir=materialized_output_dir,
        dataset_version=str(research_datasets["dataset_version"]),
    )
    return [row.to_dict() for row in loaded["bar_views"]]


@asset(group_name="research")
def research_indicator_frames(
    research_datasets: dict[str, object],
    research_bar_views: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_bar_views
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    dataset_version = str(research_datasets["dataset_version"])
    indicator_set_version = str(research_datasets["indicator_set_version"])
    profile_version = str(research_datasets["indicator_profile_version"])
    if not bool(research_datasets.get("reuse_existing_materialization")):
        materialize_indicator_frames(
            dataset_output_dir=materialized_output_dir,
            indicator_output_dir=materialized_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            profile_version=profile_version,
        )
    rows = reload_indicator_frames(
        indicator_output_dir=materialized_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    )
    return [row.to_dict() for row in rows]


@asset(group_name="research")
def research_derived_indicator_frames(
    research_datasets: dict[str, object],
    research_bar_views: list[dict[str, object]],
    research_indicator_frames: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_bar_views
    del research_indicator_frames
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    dataset_version = str(research_datasets["dataset_version"])
    indicator_set_version = str(research_datasets["indicator_set_version"])
    derived_indicator_set_version = str(research_datasets["derived_indicator_set_version"])
    profile_version = str(research_datasets["derived_indicator_profile_version"])
    if not bool(research_datasets.get("reuse_existing_materialization")):
        materialize_derived_indicator_frames(
            dataset_output_dir=materialized_output_dir,
            indicator_output_dir=materialized_output_dir,
            derived_indicator_output_dir=materialized_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            profile_version=profile_version,
        )
    rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=materialized_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
    )
    return [row.to_dict() for row in rows]


def _validate_strategy_seed_inventory(*, registry_root: Path) -> None:
    paths = {
        "research_strategy_families": registry_root / "research_strategy_families.delta",
        "research_strategy_templates": registry_root / "research_strategy_templates.delta",
        "research_strategy_template_modules": registry_root / "research_strategy_template_modules.delta",
        "research_strategy_instances": registry_root / "research_strategy_instances.delta",
        "research_strategy_instance_modules": registry_root / "research_strategy_instance_modules.delta",
    }
    for table_name, table_path in paths.items():
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing strategy registry table: {table_name} at {table_path.as_posix()}")

    family_rows = read_delta_table_rows(paths["research_strategy_families"])
    template_rows = read_delta_table_rows(paths["research_strategy_templates"])
    module_rows = read_delta_table_rows(paths["research_strategy_template_modules"])
    instance_rows = read_delta_table_rows(paths["research_strategy_instances"])
    instance_module_rows = read_delta_table_rows(paths["research_strategy_instance_modules"])

    expected_adapter_keys = {adapter.family_manifest.family_key for adapter in phase_stg02_family_adapters()}
    actual_adapter_keys = {str(row.get("family_key", "")).strip() for row in family_rows}
    if actual_adapter_keys != expected_adapter_keys:
        raise RuntimeError(
            "strategy adapter inventory mismatch on supported route: "
            f"expected={sorted(expected_adapter_keys)}, actual={sorted(actual_adapter_keys)}"
        )
    if len(template_rows) < len(expected_adapter_keys):
        raise RuntimeError(
            "strategy template registry missing required rows: "
            f"expected_at_least={len(expected_adapter_keys)}, actual={len(template_rows)}"
        )
    if len(module_rows) < len(expected_adapter_keys):
        raise RuntimeError(
            "strategy template-module registry missing required rows: "
            f"expected_at_least={len(expected_adapter_keys)}, actual={len(module_rows)}"
        )
    if not instance_rows:
        raise RuntimeError("strategy_space resolved to 0 materialized instances on supported route")
    if not instance_module_rows:
        raise RuntimeError("strategy instance-module registry missing required rows on supported route")


@asset(group_name="research")
def research_strategy_families(
    research_datasets: dict[str, object],
) -> list[dict[str, object]]:
    registry_root = Path(str(research_datasets["registry_root"]))
    _validate_strategy_seed_inventory(registry_root=registry_root)
    return [dict(row) for row in read_delta_table_rows(registry_root / "research_strategy_families.delta")]


@asset(group_name="research")
def research_strategy_templates(
    research_strategy_families: list[dict[str, object]],
    research_datasets: dict[str, object],
) -> list[dict[str, object]]:
    del research_strategy_families
    registry_root = Path(str(research_datasets["registry_root"]))
    return [dict(row) for row in read_delta_table_rows(registry_root / "research_strategy_templates.delta")]


@asset(group_name="research")
def research_strategy_template_modules(
    research_strategy_templates: list[dict[str, object]],
    research_datasets: dict[str, object],
) -> list[dict[str, object]]:
    del research_strategy_templates
    registry_root = Path(str(research_datasets["registry_root"]))
    return [dict(row) for row in read_delta_table_rows(registry_root / "research_strategy_template_modules.delta")]


@asset(group_name="research")
def research_strategy_instances(
    research_strategy_template_modules: list[dict[str, object]],
    research_datasets: dict[str, object],
) -> list[dict[str, object]]:
    del research_strategy_template_modules
    registry_root = Path(str(research_datasets["registry_root"]))
    return [dict(row) for row in read_delta_table_rows(registry_root / "research_strategy_instances.delta")]


@asset(group_name="research")
def research_strategy_instance_modules(
    research_strategy_instances: list[dict[str, object]],
    research_datasets: dict[str, object],
) -> list[dict[str, object]]:
    del research_strategy_instances
    registry_root = Path(str(research_datasets["registry_root"]))
    return [dict(row) for row in read_delta_table_rows(registry_root / "research_strategy_instance_modules.delta")]


def _backtest_request_config(research_datasets: dict[str, object]) -> BacktestBatchRequest:
    payload = dict(research_datasets["backtest_request"])
    return BacktestBatchRequest(
        campaign_run_id=str(research_datasets["campaign_run_id"]),
        strategy_space_id=str(payload["strategy_space_id"]),
        dataset_version=str(research_datasets["dataset_version"]),
        indicator_set_version=str(research_datasets["indicator_set_version"]),
        feature_set_version=str(research_datasets["feature_set_version"]),
        derived_indicator_set_version=str(research_datasets["derived_indicator_set_version"]),
        strategy_instances=tuple(
            BacktestStrategyInstance(
                strategy_instance_id=str(item["strategy_instance_id"]),
                strategy_template_id=str(item["strategy_template_id"]),
                family_id=str(item["family_id"]),
                family_key=str(item["family_key"]),
                strategy_version_label=str(item["strategy_version_label"]),
                execution_mode=str(item["execution_mode"]),
                parameter_values=dict(item.get("parameter_values", {})),
                manifest_hash=str(item["manifest_hash"]),
            )
            for item in payload["strategy_instances"]
        ),
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


@asset(group_name="research")
def research_backtest_batches(
    research_datasets: dict[str, object],
    research_indicator_frames: list[dict[str, object]],
    research_derived_indicator_frames: list[dict[str, object]],
    research_strategy_instance_modules: list[dict[str, object]],
) -> dict[str, object]:
    del research_indicator_frames
    del research_derived_indicator_frames
    del research_strategy_instance_modules
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    results_output_dir = Path(str(research_datasets["results_output_dir"]))
    report = run_backtest_batch(
        dataset_output_dir=materialized_output_dir,
        indicator_output_dir=materialized_output_dir,
        feature_output_dir=materialized_output_dir,
        output_dir=results_output_dir,
        request=_backtest_request_config(research_datasets),
        engine_config=_engine_config(research_datasets),
    )
    return {
        **report,
        "results_output_dir": results_output_dir.as_posix(),
    }


@asset(group_name="research")
def research_backtest_runs(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["run_rows"]]


@asset(group_name="research")
def research_strategy_stats(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["stat_rows"]]


@asset(group_name="research")
def research_trade_records(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["trade_rows"]]


@asset(group_name="research")
def research_order_records(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["order_rows"]]


@asset(group_name="research")
def research_drawdown_records(research_backtest_batches: dict[str, object]) -> list[dict[str, object]]:
    return [dict(row) for row in research_backtest_batches["drawdown_rows"]]


@asset(group_name="research")
def research_strategy_rankings(
    research_datasets: dict[str, object],
    research_backtest_batches: dict[str, object],
    research_strategy_stats: list[dict[str, object]],
    research_trade_records: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_backtest_batches
    del research_strategy_stats
    del research_trade_records
    results_output_dir = Path(str(research_datasets["results_output_dir"]))
    report = rank_backtest_results(
        backtest_output_dir=results_output_dir,
        output_dir=results_output_dir,
        policy=_ranking_policy(research_datasets),
    )
    return [dict(row) for row in report["ranking_rows"]]


@asset(group_name="research")
def research_signal_candidates(
    research_datasets: dict[str, object],
    research_derived_indicator_frames: list[dict[str, object]],
    research_strategy_rankings: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_derived_indicator_frames
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    results_output_dir = Path(str(research_datasets["results_output_dir"]))
    report = project_runtime_candidates(
        dataset_output_dir=materialized_output_dir,
        indicator_output_dir=materialized_output_dir,
        feature_output_dir=materialized_output_dir,
        output_dir=results_output_dir,
        request=_projection_request(research_datasets),
        ranking_rows=[dict(row) for row in research_strategy_rankings],
        config=_engine_config(research_datasets),
    )
    return [dict(row) for row in report["candidate_rows"]]


RESEARCH_ASSETS = (
    research_datasets,
    research_instrument_tree,
    research_bar_views,
    research_indicator_frames,
    research_derived_indicator_frames,
    research_strategy_families,
    research_strategy_templates,
    research_strategy_template_modules,
    research_strategy_instances,
    research_strategy_instance_modules,
    research_backtest_batches,
    research_backtest_runs,
    research_strategy_stats,
    research_trade_records,
    research_order_records,
    research_drawdown_records,
    research_strategy_rankings,
    research_signal_candidates,
)

research_data_prep_job = define_asset_job(
    name=RESEARCH_DATA_PREP_JOB_NAME,
    selection=AssetSelection.assets(
        research_datasets,
        research_instrument_tree,
        research_bar_views,
        research_indicator_frames,
        research_derived_indicator_frames,
    ),
)

strategy_registry_refresh_job = define_asset_job(
    name=STRATEGY_REGISTRY_REFRESH_JOB_NAME,
    selection=AssetSelection.assets(
        research_datasets,
        research_strategy_families,
        research_strategy_templates,
        research_strategy_template_modules,
        research_strategy_instances,
        research_strategy_instance_modules,
    ),
)

research_backtest_job = define_asset_job(
    name=RESEARCH_BACKTEST_JOB_NAME,
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

research_projection_job = define_asset_job(
    name=RESEARCH_PROJECTION_JOB_NAME,
    selection=AssetSelection.assets(research_signal_candidates),
)


def _split_env_csv(raw: str, *, default: Sequence[str]) -> tuple[str, ...]:
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    return values or tuple(default)


def _env_path(env_name: str, default: Path) -> Path:
    raw = os.environ.get(env_name, "").strip()
    return Path(raw).resolve() if raw else default.resolve()


def _default_moex_historical_data_root() -> Path:
    raw = os.environ.get(MOEX_HISTORICAL_DATA_ROOT_ENV, "").strip()
    return Path(raw).resolve() if raw else DEFAULT_MOEX_HISTORICAL_DATA_ROOT.resolve()


def _default_research_canonical_output_dir() -> Path:
    return _default_moex_historical_data_root() / CANONICAL_BASELINE_ROOT_RELATIVE_PATH


def _default_research_materialized_output_dir() -> Path:
    return _default_moex_historical_data_root() / "research" / "gold" / "current"


def _default_research_results_output_dir() -> Path:
    return _default_moex_historical_data_root() / "research" / "runs" / "data-prep"


def _env_text(env_name: str, default: str) -> str:
    return os.environ.get(env_name, "").strip() or default


def _moex_canonical_output_dir_from_run_config(run_config: object) -> Path | None:
    if not isinstance(run_config, dict):
        return None
    ops = run_config.get("ops")
    if not isinstance(ops, dict):
        return None
    step = ops.get("moex_baseline_update")
    if not isinstance(step, dict):
        return None
    config = step.get("config")
    if not isinstance(config, dict):
        return None

    canonical_output_dir = str(config.get("canonical_output_dir", "")).strip()
    if canonical_output_dir:
        return Path(canonical_output_dir).resolve()

    canonical_bars_path = str(config.get("canonical_bars_path", "")).strip()
    if canonical_bars_path:
        return Path(canonical_bars_path).resolve().parent
    return None


def build_research_data_prep_run_config(
    *,
    canonical_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
    dataset_version: str | None = None,
    dataset_name: str = DEFAULT_RESEARCH_DATA_PREP_DATASET_NAME,
    universe_id: str = "moex-futures",
    timeframes: Sequence[str] | None = None,
    base_timeframe: str = DEFAULT_RESEARCH_DATA_PREP_BASE_TIMEFRAME,
    campaign_run_id: str = "research_data_prep_scheduled",
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
) -> dict[str, object]:
    resolved_canonical_output_dir = (
        canonical_output_dir.resolve()
        if canonical_output_dir is not None
        else _env_path(RESEARCH_DATA_PREP_CANONICAL_OUTPUT_DIR_ENV, _default_research_canonical_output_dir())
    )
    resolved_materialized_output_dir = (
        materialized_output_dir.resolve()
        if materialized_output_dir is not None
        else _env_path(
            RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR_ENV,
            _default_research_materialized_output_dir(),
        )
    )
    resolved_results_output_dir = (
        results_output_dir.resolve()
        if results_output_dir is not None
        else _env_path(RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR_ENV, _default_research_results_output_dir())
    )
    resolved_timeframes = tuple(timeframes) if timeframes is not None else _split_env_csv(
        os.environ.get(RESEARCH_DATA_PREP_TIMEFRAMES_ENV, ""),
        default=DEFAULT_RESEARCH_DATA_PREP_TIMEFRAMES,
    )
    resolved_dataset_version = dataset_version or _env_text(
        RESEARCH_DATA_PREP_DATASET_VERSION_ENV,
        DEFAULT_RESEARCH_DATA_PREP_DATASET_VERSION,
    )
    registry_root = research_registry_root(materialized_root=resolved_materialized_output_dir)
    return {
        "ops": {
            "research_datasets": {
                "config": _research_run_config(
                    canonical_output_dir=resolved_canonical_output_dir,
                    registry_root=registry_root,
                    materialized_output_dir=resolved_materialized_output_dir,
                    results_output_dir=resolved_results_output_dir,
                    campaign_run_id=campaign_run_id,
                    dataset_version=resolved_dataset_version,
                    dataset_name=dataset_name,
                    universe_id=universe_id,
                    timeframes=resolved_timeframes,
                    base_timeframe=base_timeframe,
                    indicator_set_version=indicator_set_version,
                    indicator_profile_version=indicator_profile_version,
                    derived_indicator_set_version=derived_indicator_set_version,
                    derived_indicator_profile_version=derived_indicator_profile_version,
                    feature_set_version=feature_set_version,
                    feature_profile_version=feature_profile_version,
                    code_version="research-data-prep-after-moex",
                    strategy_space_id="",
                    strategy_instances=(),
                    combination_count=0,
                )
            }
        }
    }


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name=RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME,
    monitored_jobs=[moex_baseline_update_job],
    request_job=research_data_prep_job,
    default_status=DefaultSensorStatus.RUNNING,
    description="Start research_data_prep_job after the canonical MOEX baseline update succeeds.",
)
def research_data_prep_after_moex_sensor(context):
    canonical_output_dir = _moex_canonical_output_dir_from_run_config(context.dagster_run.run_config)
    if canonical_output_dir is None:
        canonical_output_dir = _env_path(
            RESEARCH_DATA_PREP_CANONICAL_OUTPUT_DIR_ENV,
            _default_research_canonical_output_dir(),
        )
    upstream_run_id = str(context.dagster_run.run_id)
    dataset_version = _env_text(
        RESEARCH_DATA_PREP_DATASET_VERSION_ENV,
        DEFAULT_RESEARCH_DATA_PREP_DATASET_VERSION,
    )
    return RunRequest(
        run_key=f"{RESEARCH_DATA_PREP_JOB_NAME}:{upstream_run_id}",
        run_config=build_research_data_prep_run_config(
            canonical_output_dir=canonical_output_dir,
            campaign_run_id=f"research_data_prep_after_{upstream_run_id}",
            dataset_version=dataset_version,
        ),
        tags={
            "ta3000/upstream_job": MOEX_BASELINE_UPDATE_JOB_NAME,
            "ta3000/upstream_run_id": upstream_run_id,
            "ta3000/dataset_version": dataset_version,
        },
    )


research_definitions = Definitions(
    assets=list(RESEARCH_ASSETS),
    jobs=[
        research_data_prep_job,
        strategy_registry_refresh_job,
        research_backtest_job,
        research_projection_job,
    ],
    sensors=[research_data_prep_after_moex_sensor],
)


def assert_research_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or research_definitions
    repository = defs.get_repository_def()
    expected_by_job = {
        RESEARCH_DATA_PREP_JOB_NAME: set(RESEARCH_DATA_PREP_ASSETS),
        STRATEGY_REGISTRY_REFRESH_JOB_NAME: set(STRATEGY_REGISTRY_REFRESH_EXECUTION_ASSETS),
        RESEARCH_BACKTEST_JOB_NAME: set(RESEARCH_BACKTEST_ASSETS),
        RESEARCH_PROJECTION_JOB_NAME: set(RESEARCH_PROJECTION_ASSETS),
    }
    for job_name, expected_assets in expected_by_job.items():
        job = repository.get_job(job_name)
        actual_nodes = set(job.graph.node_dict.keys())
        missing_nodes = sorted(expected_assets - actual_nodes)
        if missing_nodes:
            raise RuntimeError(
                "research Dagster definitions are metadata-only or incomplete: "
                f"job `{job_name}` missing executable asset nodes: {', '.join(missing_nodes)}"
            )


def build_research_definitions() -> Definitions:
    assert_research_definitions_executable(research_definitions)
    return research_definitions


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
        raise ValueError(f"unknown research asset selection: {', '.join(sorted(unknown))}")
    return normalized


def _resolve_expected_materialization(selection: Sequence[str]) -> list[str]:
    resolved: set[str] = set()

    def _visit(asset_name: str) -> None:
        if asset_name in resolved:
            return
        for dependency in RESEARCH_DEPENDENCIES.get(asset_name, tuple()):
            _visit(dependency)
        resolved.add(asset_name)

    for asset_name in selection:
        _visit(asset_name)
    return [name for name in RESEARCH_ASSET_KEYS if name in resolved]


def _requires_strategy_space(asset_names: Sequence[str]) -> bool:
    strategy_space_assets = {
        *STRATEGY_REGISTRY_REFRESH_ASSETS,
        *RESEARCH_BACKTEST_ASSETS,
        *RESEARCH_PROJECTION_ASSETS,
    }
    return any(asset_name in strategy_space_assets for asset_name in asset_names)


def _research_run_config(
    *,
    canonical_output_dir: Path,
    registry_root: Path,
    research_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
    campaign_run_id: str,
    dataset_version: str,
    dataset_name: str = "research-materialized",
    universe_id: str = "moex-futures",
    timeframes: Sequence[str],
    base_timeframe: str = "",
    start_ts: str = "",
    end_ts: str = "",
    warmup_bars: int = DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS,
    split_method: str = "holdout",
    series_mode: str = "contract",
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str,
    indicator_profile_version: str,
    derived_indicator_set_version: str,
    derived_indicator_profile_version: str,
    feature_set_version: str,
    feature_profile_version: str,
    code_version: str = "research-orchestration",
    strategy_space: dict[str, object] | None = None,
    strategy_space_id: str = "",
    strategy_instances: Sequence[dict[str, object]] = (),
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
    reuse_existing_materialization: bool = False,
) -> dict[str, object]:
    resolved_strategy_space = dict(strategy_space or _default_strategy_space())
    resolved_timeframes = [str(item) for item in timeframes]
    resolved_materialized_output_dir, resolved_results_output_dir = _resolve_research_output_dirs(
        research_output_dir=research_output_dir,
        materialized_output_dir=materialized_output_dir,
        results_output_dir=results_output_dir,
    )
    return {
        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
        "registry_root": registry_root.resolve().as_posix(),
        "materialized_output_dir": resolved_materialized_output_dir.as_posix(),
        "results_output_dir": resolved_results_output_dir.as_posix(),
        "reuse_existing_materialization": reuse_existing_materialization,
        "campaign_run_id": campaign_run_id,
        "dataset_version": dataset_version,
        "dataset_name": dataset_name,
        "universe_id": universe_id,
        "timeframes": resolved_timeframes,
        "base_timeframe": base_timeframe or (resolved_timeframes[0] if resolved_timeframes else "15m"),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "warmup_bars": warmup_bars,
        "split_method": split_method,
        "series_mode": series_mode,
        "dataset_contract_ids": [str(item) for item in dataset_contract_ids],
        "dataset_instrument_ids": [str(item) for item in dataset_instrument_ids],
        "indicator_set_version": indicator_set_version,
        "indicator_profile_version": indicator_profile_version,
        "derived_indicator_set_version": derived_indicator_set_version,
        "derived_indicator_profile_version": derived_indicator_profile_version,
        "feature_set_version": feature_set_version,
        "feature_profile_version": feature_profile_version,
        "code_version": code_version,
        "strategy_space": resolved_strategy_space,
        "strategy_space_id": strategy_space_id,
        "strategy_instances": [dict(item) for item in strategy_instances],
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


def _materialize_research_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
    campaign_id: str = "camp_research_route",
    campaign_run_id: str = "crun_research_route",
    dataset_version: str,
    dataset_name: str = "research-materialized",
    universe_id: str = "moex-futures",
    timeframes: Sequence[str],
    base_timeframe: str = "",
    start_ts: str = "",
    end_ts: str = "",
    warmup_bars: int = DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS,
    split_method: str = "holdout",
    series_mode: str = "contract",
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    default_assets: Sequence[str],
    allowed_assets: Sequence[str],
    selection: Sequence[str] | None = None,
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    code_version: str = "research-orchestration",
    strategy_space: dict[str, object] | None = None,
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
    reuse_existing_materialization: bool = False,
    raise_on_error: bool = True,
) -> dict[str, object]:
    assert_research_definitions_executable()
    selected_assets = _resolve_selected_assets(selection, default_assets=default_assets, allowed_assets=allowed_assets)
    expected_materialized_assets = _resolve_expected_materialization(selected_assets)
    resolved_materialized_output_dir, resolved_results_output_dir = _resolve_research_output_dirs(
        research_output_dir=research_output_dir,
        materialized_output_dir=materialized_output_dir,
        results_output_dir=results_output_dir,
    )
    registry_root = research_registry_root(materialized_root=resolved_materialized_output_dir)
    strategy_space_id = ""
    strategy_instances: list[dict[str, object]] = []
    resolved_combination_count = combination_count
    if _requires_strategy_space(expected_materialized_assets):
        prepared_strategy_space = prepare_strategy_space(
            registry_root=registry_root,
            strategy_space=dict(strategy_space or _default_strategy_space()),
            backtest_policy={
                "combination_count": combination_count,
                "param_batch_size": param_batch_size,
                "series_batch_size": series_batch_size,
                "backtest_timeframe": backtest_timeframe,
            },
            execution_policy={
                "fees_bps": fees_bps,
                "slippage_bps": slippage_bps,
                "allow_short": allow_short,
                "window_count": window_count,
            },
            campaign_id=campaign_id,
            campaign_run_id=campaign_run_id,
            created_at="2026-04-18T00:00:00Z",
        )
        strategy_space_id = prepared_strategy_space.strategy_space_id
        strategy_instances = [instance.to_dict() for instance in prepared_strategy_space.execution_instances]
        resolved_combination_count = len(strategy_instances)
    output_paths = _research_output_paths(
        registry_root=registry_root,
        materialized_output_dir=resolved_materialized_output_dir,
        results_output_dir=resolved_results_output_dir,
    )

    result = materialize(
        assets=list(RESEARCH_ASSETS),
        selection=expected_materialized_assets,
        run_config={
            "ops": {
                "research_datasets": {
                    "config": _research_run_config(
                        canonical_output_dir=canonical_output_dir,
                        registry_root=registry_root,
                        materialized_output_dir=resolved_materialized_output_dir,
                        results_output_dir=resolved_results_output_dir,
                        campaign_run_id=campaign_run_id,
                        dataset_version=dataset_version,
                        dataset_name=dataset_name,
                        universe_id=universe_id,
                        timeframes=timeframes,
                        base_timeframe=base_timeframe,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        warmup_bars=warmup_bars,
                        split_method=split_method,
                        series_mode=series_mode,
                        dataset_contract_ids=dataset_contract_ids,
                        dataset_instrument_ids=dataset_instrument_ids,
                        indicator_set_version=indicator_set_version,
                        indicator_profile_version=indicator_profile_version,
                        derived_indicator_set_version=derived_indicator_set_version,
                        derived_indicator_profile_version=derived_indicator_profile_version,
                        feature_set_version=feature_set_version,
                        feature_profile_version=feature_profile_version,
                        code_version=code_version,
                        strategy_space=dict(strategy_space or _default_strategy_space()),
                        strategy_space_id=strategy_space_id,
                        strategy_instances=strategy_instances,
                        combination_count=resolved_combination_count,
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
                        reuse_existing_materialization=reuse_existing_materialization,
                    )
                }
            }
        },
        raise_on_error=raise_on_error,
    )

    report: dict[str, object] = {
        "success": bool(result.success),
        "strategy_space_id": strategy_space_id,
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
    findings_path = Path(output_paths["research_run_findings"])
    if has_delta_log(findings_path):
        rows_by_table["research_run_findings"] = len(read_delta_table_rows(findings_path))
    report["rows_by_table"] = rows_by_table
    return report


def materialize_research_data_prep_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
    campaign_id: str = "camp_research_route",
    campaign_run_id: str = "crun_research_route",
    dataset_version: str,
    dataset_name: str = "research-data-prep",
    universe_id: str = "moex-futures",
    timeframes: Sequence[str],
    base_timeframe: str = "",
    start_ts: str = "",
    end_ts: str = "",
    warmup_bars: int = DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS,
    split_method: str = "holdout",
    series_mode: str = "contract",
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    code_version: str = "research-data-prep-orchestration",
    reuse_existing_materialization: bool = False,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    return _materialize_research_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        materialized_output_dir=materialized_output_dir,
        results_output_dir=results_output_dir,
        campaign_id=campaign_id,
        campaign_run_id=campaign_run_id,
        dataset_version=dataset_version,
        dataset_name=dataset_name,
        universe_id=universe_id,
        timeframes=timeframes,
        base_timeframe=base_timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        warmup_bars=warmup_bars,
        split_method=split_method,
        series_mode=series_mode,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=RESEARCH_DATA_PREP_ASSETS,
        allowed_assets=RESEARCH_DATA_PREP_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        code_version=code_version,
        reuse_existing_materialization=reuse_existing_materialization,
        raise_on_error=raise_on_error,
    )


def materialize_strategy_registry_refresh_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
    campaign_id: str = "camp_research_route",
    campaign_run_id: str = "crun_research_route",
    dataset_version: str,
    dataset_name: str = "strategy-registry-refresh",
    universe_id: str = "moex-futures",
    timeframes: Sequence[str],
    base_timeframe: str = "",
    start_ts: str = "",
    end_ts: str = "",
    warmup_bars: int = DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS,
    split_method: str = "holdout",
    series_mode: str = "contract",
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    code_version: str = "strategy-registry-refresh-orchestration",
    strategy_space: dict[str, object] | None = None,
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
    reuse_existing_materialization: bool = True,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    return _materialize_research_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        materialized_output_dir=materialized_output_dir,
        results_output_dir=results_output_dir,
        campaign_id=campaign_id,
        campaign_run_id=campaign_run_id,
        dataset_version=dataset_version,
        dataset_name=dataset_name,
        universe_id=universe_id,
        timeframes=timeframes,
        base_timeframe=base_timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        warmup_bars=warmup_bars,
        split_method=split_method,
        series_mode=series_mode,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=STRATEGY_REGISTRY_REFRESH_ASSETS,
        allowed_assets=STRATEGY_REGISTRY_REFRESH_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        code_version=code_version,
        strategy_space=strategy_space,
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
        reuse_existing_materialization=reuse_existing_materialization,
        raise_on_error=raise_on_error,
    )


def materialize_research_backtest_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
    campaign_id: str = "camp_research_route",
    campaign_run_id: str = "crun_research_route",
    dataset_version: str,
    dataset_name: str = "research-backtest",
    universe_id: str = "moex-futures",
    timeframes: Sequence[str],
    base_timeframe: str = "",
    start_ts: str = "",
    end_ts: str = "",
    warmup_bars: int = DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS,
    split_method: str = "holdout",
    series_mode: str = "contract",
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    code_version: str = "research-backtest-orchestration",
    strategy_space: dict[str, object] | None = None,
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
    reuse_existing_materialization: bool = False,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    return _materialize_research_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        materialized_output_dir=materialized_output_dir,
        results_output_dir=results_output_dir,
        campaign_id=campaign_id,
        campaign_run_id=campaign_run_id,
        dataset_version=dataset_version,
        dataset_name=dataset_name,
        universe_id=universe_id,
        timeframes=timeframes,
        base_timeframe=base_timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        warmup_bars=warmup_bars,
        split_method=split_method,
        series_mode=series_mode,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=RESEARCH_BACKTEST_ASSETS,
        allowed_assets=RESEARCH_BACKTEST_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        code_version=code_version,
        strategy_space=strategy_space,
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
        reuse_existing_materialization=reuse_existing_materialization,
        raise_on_error=raise_on_error,
    )


def materialize_research_projection_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path | None = None,
    materialized_output_dir: Path | None = None,
    results_output_dir: Path | None = None,
    campaign_id: str = "camp_research_route",
    campaign_run_id: str = "crun_research_route",
    dataset_version: str,
    dataset_name: str = "research-projection",
    universe_id: str = "moex-futures",
    timeframes: Sequence[str],
    base_timeframe: str = "",
    start_ts: str = "",
    end_ts: str = "",
    warmup_bars: int = DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS,
    split_method: str = "holdout",
    series_mode: str = "contract",
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    code_version: str = "research-projection-orchestration",
    strategy_space: dict[str, object] | None = None,
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
    reuse_existing_materialization: bool = False,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    return _materialize_research_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        materialized_output_dir=materialized_output_dir,
        results_output_dir=results_output_dir,
        campaign_id=campaign_id,
        campaign_run_id=campaign_run_id,
        dataset_version=dataset_version,
        dataset_name=dataset_name,
        universe_id=universe_id,
        timeframes=timeframes,
        base_timeframe=base_timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        warmup_bars=warmup_bars,
        split_method=split_method,
        series_mode=series_mode,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=RESEARCH_PROJECTION_ASSETS,
        allowed_assets=RESEARCH_PROJECTION_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        code_version=code_version,
        strategy_space=strategy_space,
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
        reuse_existing_materialization=reuse_existing_materialization,
        raise_on_error=raise_on_error,
    )
