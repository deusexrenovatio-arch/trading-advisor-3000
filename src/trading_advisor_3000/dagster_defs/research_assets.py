from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
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
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    ensure_delta_table_columns,
    has_delta_log,
    read_delta_table_frame,
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
    MOEX_HISTORICAL_DATA_ROOT_ENV,
)
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    CandidateProjectionRequest,
    RankingPolicy,
    StrategyFamilySearchSpec,
    project_runtime_candidates,
    rank_backtest_results,
    run_backtest_batch,
)
from trading_advisor_3000.product_plane.research.continuous_front import (
    CONTINUOUS_FRONT_TABLES,
    continuous_front_store_contract,
    load_continuous_front_as_research_context,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    ResearchDatasetManifest,
    materialize_research_dataset,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.derived_indicators import (
    DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    materialize_derived_indicator_frames,
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import (
    materialize_indicator_frames,
    indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.registry_store import registry_output_paths, research_registry_root
from trading_advisor_3000.product_plane.research.strategy_space import prepare_strategy_space
from trading_advisor_3000.product_plane.research.strategies.families import phase_stg02_family_adapters
from trading_advisor_3000.spark_jobs import run_continuous_front_spark_job

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
    "continuous_front_bars",
    "continuous_front_roll_events",
    "continuous_front_adjustment_ladder",
    "continuous_front_qc_report",
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
)

STRATEGY_REGISTRY_REFRESH_EXECUTION_ASSETS = (
    "research_datasets",
    *STRATEGY_REGISTRY_REFRESH_ASSETS,
)

RESEARCH_BACKTEST_ASSETS = (
    "research_backtest_batches",
    "research_strategy_search_specs",
    "research_vbt_search_runs",
    "research_optimizer_studies",
    "research_optimizer_trials",
    "research_vbt_param_results",
    "research_vbt_param_gate_events",
    "research_vbt_ephemeral_indicator_cache",
    "research_strategy_promotion_events",
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
    "continuous_front_bars": tuple(),
    "continuous_front_roll_events": ("continuous_front_bars",),
    "continuous_front_adjustment_ladder": ("continuous_front_bars",),
    "continuous_front_qc_report": (
        "continuous_front_bars",
        "continuous_front_roll_events",
        "continuous_front_adjustment_ladder",
    ),
    "research_datasets": ("continuous_front_qc_report",),
    "research_instrument_tree": ("research_datasets",),
    "research_bar_views": ("research_datasets",),
    "research_indicator_frames": ("research_datasets", "research_bar_views"),
    "research_derived_indicator_frames": ("research_datasets", "research_bar_views", "research_indicator_frames"),
    "research_strategy_families": ("research_datasets",),
    "research_strategy_templates": ("research_strategy_families",),
    "research_strategy_template_modules": ("research_strategy_templates",),
    "research_backtest_batches": (
        "research_datasets",
        "research_indicator_frames",
        "research_derived_indicator_frames",
        "research_strategy_template_modules",
    ),
    "research_strategy_search_specs": ("research_backtest_batches",),
    "research_vbt_search_runs": ("research_backtest_batches",),
    "research_optimizer_studies": ("research_backtest_batches",),
    "research_optimizer_trials": ("research_backtest_batches",),
    "research_vbt_param_results": ("research_backtest_batches",),
    "research_vbt_param_gate_events": ("research_backtest_batches",),
    "research_vbt_ephemeral_indicator_cache": ("research_backtest_batches",),
    "research_strategy_promotion_events": ("research_backtest_batches",),
    "research_backtest_runs": ("research_backtest_batches",),
    "research_strategy_stats": ("research_backtest_batches",),
    "research_trade_records": ("research_backtest_batches",),
    "research_order_records": ("research_backtest_batches",),
    "research_drawdown_records": ("research_backtest_batches",),
    "research_strategy_rankings": ("research_backtest_runs", "research_strategy_stats", "research_trade_records"),
    "research_signal_candidates": ("research_datasets", "research_derived_indicator_frames", "research_strategy_rankings"),
}


def research_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="continuous_front_bars",
            description="Materialize causal continuous-front bars from canonical bars after canonical refresh.",
            inputs=("canonical_bars_delta", "canonical_session_calendar_delta", "canonical_roll_map_delta"),
            outputs=("continuous_front_bars_delta",),
        ),
        AssetSpec(
            key="continuous_front_roll_events",
            description="Expose confirmed roll decisions used by the continuous-front contour.",
            inputs=("continuous_front_bars_delta",),
            outputs=("continuous_front_roll_events_delta",),
        ),
        AssetSpec(
            key="continuous_front_adjustment_ladder",
            description="Expose the as-of additive adjustment ladder consumed by research materialization.",
            inputs=("continuous_front_bars_delta",),
            outputs=("continuous_front_adjustment_ladder_delta",),
        ),
        AssetSpec(
            key="continuous_front_qc_report",
            description="Publish fail-closed QC for each continuous-front run/instrument/timeframe.",
            inputs=(
                "continuous_front_bars_delta",
                "continuous_front_roll_events_delta",
                "continuous_front_adjustment_ladder_delta",
            ),
            outputs=("continuous_front_qc_report_delta",),
        ),
        AssetSpec(
            key="research_datasets",
            description="Materialize versioned research dataset manifests from canonical or continuous-front bars.",
            inputs=(
                "continuous_front_qc_report_delta",
                "canonical_bars_delta",
                "canonical_session_calendar_delta",
                "canonical_roll_map_delta",
            ),
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
            key="research_backtest_batches",
            description="Run vectorbt family-search surfaces over the materialized research plane.",
            inputs=(
                "research_datasets_delta",
                "research_indicator_frames_delta",
                "research_derived_indicator_frames_delta",
                "research_strategy_template_modules_delta",
            ),
            outputs=("research_backtest_batches_delta",),
        ),
        AssetSpec(
            key="research_strategy_search_specs",
            description="Expose family-level StrategyFamilySearchSpec rows used by vectorbt parametric search.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_strategy_search_specs_delta",),
        ),
        AssetSpec(
            key="research_vbt_search_runs",
            description="Expose vectorbt family-search run rows grouped by search spec, clock, dataset, and chunk.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_vbt_search_runs_delta",),
        ),
        AssetSpec(
            key="research_optimizer_studies",
            description="Expose Delta-first optimizer study provenance for adaptive family-search campaigns.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_optimizer_studies_delta",),
        ),
        AssetSpec(
            key="research_optimizer_trials",
            description="Expose optimizer trial rows linked to vectorbt param_hash results.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_optimizer_trials_delta",),
        ),
        AssetSpec(
            key="research_vbt_param_results",
            description="Expose param_hash-level vectorbt metrics from family-search surfaces.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_vbt_param_results_delta",),
        ),
        AssetSpec(
            key="research_vbt_param_gate_events",
            description="Expose gate outcomes for each vectorbt family-search parameter row.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_vbt_param_gate_events_delta",),
        ),
        AssetSpec(
            key="research_vbt_ephemeral_indicator_cache",
            description="Expose metadata for ephemeral vectorbt indicator inputs used by search surfaces.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_vbt_ephemeral_indicator_cache_delta",),
        ),
        AssetSpec(
            key="research_strategy_promotion_events",
            description="Expose post-ranking promotion events from param_hash rows into StrategyInstance records.",
            inputs=("research_backtest_batches_delta",),
            outputs=("research_strategy_promotion_events_delta",),
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
            inputs=("research_backtest_runs_delta", "research_strategy_stats_delta", "research_trade_records_delta"),
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
        "exclude_template_manifest_hashes": [],
        "max_parameter_combinations": 250000,
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
        "continuous_front_policy": dict,
        "dataset_contract_ids": [str],
        "dataset_instrument_ids": [str],
        "indicator_set_version": str,
        "indicator_profile_version": str,
        "derived_indicator_set_version": str,
        "derived_indicator_profile_version": str,
        "code_version": str,
        "strategy_space": dict,
        "strategy_space_id": str,
        "search_specs": [dict],
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
        "min_fold_count": int,
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
    registry_paths = registry_output_paths(registry_root=registry_root)
    return {
        **{table_name: registry_paths[table_name] for table_name in STRATEGY_REGISTRY_REFRESH_ASSETS},
        "continuous_front_bars": (resolved_materialized / "continuous_front_bars.delta").as_posix(),
        "continuous_front_roll_events": (resolved_materialized / "continuous_front_roll_events.delta").as_posix(),
        "continuous_front_adjustment_ladder": (
            resolved_materialized / "continuous_front_adjustment_ladder.delta"
        ).as_posix(),
        "continuous_front_qc_report": (resolved_materialized / "continuous_front_qc_report.delta").as_posix(),
        "research_datasets": (resolved_materialized / "research_datasets.delta").as_posix(),
        "research_instrument_tree": (resolved_materialized / "research_instrument_tree.delta").as_posix(),
        "research_bar_views": (resolved_materialized / "research_bar_views.delta").as_posix(),
        "research_indicator_frames": (resolved_materialized / "research_indicator_frames.delta").as_posix(),
        "research_derived_indicator_frames": (resolved_materialized / "research_derived_indicator_frames.delta").as_posix(),
        "research_strategy_search_specs": (resolved_results / "research_strategy_search_specs.delta").as_posix(),
        "research_vbt_search_runs": (resolved_results / "research_vbt_search_runs.delta").as_posix(),
        "research_optimizer_studies": (resolved_results / "research_optimizer_studies.delta").as_posix(),
        "research_optimizer_trials": (resolved_results / "research_optimizer_trials.delta").as_posix(),
        "research_vbt_param_results": (resolved_results / "research_vbt_param_results.delta").as_posix(),
        "research_vbt_param_gate_events": (resolved_results / "research_vbt_param_gate_events.delta").as_posix(),
        "research_vbt_ephemeral_indicator_cache": (resolved_results / "research_vbt_ephemeral_indicator_cache.delta").as_posix(),
        "research_strategy_promotion_events": (resolved_results / "research_strategy_promotion_events.delta").as_posix(),
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


def _delta_table_summary(*, table_path: Path, table_name: str) -> dict[str, object]:
    if not has_delta_log(table_path):
        raise RuntimeError(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
    return {
        "table": table_name,
        "path": table_path.as_posix(),
        "row_count": count_delta_table_rows(table_path),
    }


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_contract_mode_continuous_front_placeholders(
    *,
    output_dir: Path,
    dataset_version: str,
    policy: ContinuousFrontPolicy,
    run_id: str,
) -> dict[str, object]:
    contract = continuous_front_store_contract()
    now = _utc_now_iso()
    qc_row = {
        "dataset_version": dataset_version,
        "roll_policy_version": policy.roll_policy_version,
        "adjustment_policy_version": policy.adjustment_policy_version,
        "instrument_id": "*",
        "timeframe": "*",
        "run_id": run_id,
        "started_at": now,
        "completed_at": now,
        "input_row_count": 0,
        "output_row_count": 0,
        "roll_event_count": 0,
        "missing_active_bar_count": 0,
        "duplicate_key_count": 0,
        "timeline_error_count": 0,
        "ohlc_error_count": 0,
        "negative_volume_oi_count": 0,
        "missing_reference_price_count": 0,
        "future_causality_violation_count": 0,
        "gap_abs_max": 0.0,
        "gap_abs_mean": 0.0,
        "blocked_reason": "series_mode_contract",
        "status": "SKIP",
    }
    rows_by_table_payload: dict[str, list[dict[str, object]]] = {
        "continuous_front_bars": [],
        "continuous_front_roll_events": [],
        "continuous_front_adjustment_ladder": [],
        "continuous_front_qc_report": [qc_row],
    }
    output_paths: dict[str, str] = {}
    for table_name in CONTINUOUS_FRONT_TABLES:
        table_path = output_dir / f"{table_name}.delta"
        write_delta_table_rows(
            table_path=table_path,
            rows=rows_by_table_payload[table_name],
            columns=dict(contract[table_name]["columns"]),
        )
        output_paths[table_name] = table_path.as_posix()
    return {
        "success": True,
        "status": "SKIP",
        "run_id": run_id,
        "dataset_version": dataset_version,
        "policy": policy.to_config_dict(),
        "output_paths": output_paths,
        "staged_output_paths": {},
        "rows_by_table": {
            table_name: count_delta_table_rows(Path(path)) for table_name, path in output_paths.items()
        },
        "qc_rows": [qc_row],
        "contract_check_errors": [],
        "delta_manifest": contract,
    }


def _delta_table_filtered_row_count(
    *,
    table_path: Path,
    table_name: str,
    filters: list[tuple[str, str, object]],
) -> int:
    if not has_delta_log(table_path):
        raise RuntimeError(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
    return count_delta_table_rows(table_path, filters=filters)


def _require_delta_table_row(
    *,
    table_path: Path,
    table_name: str,
    filters: list[tuple[str, str, object]],
) -> None:
    if _delta_table_filtered_row_count(table_path=table_path, table_name=table_name, filters=filters) == 0:
        criteria = ", ".join(f"{column}{operator}{value}" for column, operator, value in filters)
        raise RuntimeError(f"missing reusable `{table_name}` row matching {criteria}")


def _ensure_reusable_data_prep_schema(
    *,
    materialized_output_dir: Path,
) -> None:
    contracts = {
        **research_dataset_store_contract(),
        **indicator_store_contract(),
        **research_derived_indicator_store_contract(),
    }
    for table_name, contract in contracts.items():
        table_path = materialized_output_dir / f"{table_name}.delta"
        if has_delta_log(table_path):
            ensure_delta_table_columns(table_path=table_path, columns=dict(contract["columns"]))


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
        if count_delta_table_rows(path) <= 0:
            raise RuntimeError(f"empty reusable research data prep table: {path.as_posix()}")

    _ensure_reusable_data_prep_schema(materialized_output_dir=materialized_output_dir)
    _require_delta_table_row(
        table_path=dataset_path,
        table_name="research_datasets",
        filters=[("dataset_version", "=", dataset_version)],
    )
    _require_delta_table_row(
        table_path=bar_views_path,
        table_name="research_bar_views",
        filters=[("dataset_version", "=", dataset_version)],
    )
    _require_delta_table_row(
        table_path=indicator_path,
        table_name="research_indicator_frames",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("indicator_set_version", "=", indicator_set_version),
        ],
    )
    _require_delta_table_row(
        table_path=derived_indicator_path,
        table_name="research_derived_indicator_frames",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("indicator_set_version", "=", indicator_set_version),
            ("derived_indicator_set_version", "=", derived_indicator_set_version),
        ],
    )


def _dataset_manifest_row(*, materialized_output_dir: Path, dataset_version: str) -> dict[str, object]:
    rows = read_delta_table_rows(
        materialized_output_dir / "research_datasets.delta",
        columns=list(research_dataset_store_contract()["research_datasets"]["columns"]),
        filters=[("dataset_version", "=", dataset_version)],
    )
    if not rows:
        raise RuntimeError(f"dataset_version not found: {dataset_version}")
    return dict(rows[0])


def _materialized_table_manifest(research_datasets: dict[str, object], table_name: str) -> dict[str, object]:
    output_paths = research_datasets.get("output_paths", {})
    if isinstance(output_paths, Mapping) and table_name in output_paths:
        table_path = Path(str(output_paths[table_name]))
    else:
        table_path = Path(str(research_datasets["materialized_output_dir"])) / f"{table_name}.delta"
    return {
        "table_name": table_name,
        "table_path": table_path.as_posix(),
        "row_count": count_delta_table_rows(table_path) if has_delta_log(table_path) else 0,
        "has_delta_log": has_delta_log(table_path),
    }


def _load_canonical_context(config: dict[str, object]) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    bars_path = _canonical_table_path(config, "canonical_bars")
    calendar_path = _canonical_table_path(config, "canonical_session_calendar")
    roll_map_path = _canonical_table_path(config, "canonical_roll_map")
    for path in (bars_path, calendar_path, roll_map_path):
        if not has_delta_log(path):
            raise RuntimeError(f"missing canonical delta table: {path.as_posix()}")

    contract_ids = {str(item) for item in _config_value(config, "dataset_contract_ids", [])}
    instrument_ids = {str(item) for item in _config_value(config, "dataset_instrument_ids", [])}
    timeframes = tuple(str(item) for item in _config_value(config, "timeframes", []))
    timeframe_set = set(timeframes)
    start_ts = str(_config_value(config, "start_ts", "") or "")
    end_ts = str(_config_value(config, "end_ts", "") or "")
    warmup_bars = int(_config_value(config, "warmup_bars", 0))

    def _value_filter(column: str, values: Sequence[str]) -> tuple[str, str, object] | None:
        resolved = tuple(str(value) for value in values if str(value))
        if not resolved:
            return None
        if len(resolved) == 1:
            return (column, "=", resolved[0])
        return (column, "in", sorted(resolved))

    calendar_filters: list[tuple[str, str, object]] = []
    for maybe_filter in (
        _value_filter("instrument_id", sorted(instrument_ids)),
        _value_filter("timeframe", timeframes),
    ):
        if maybe_filter is not None:
            calendar_filters.append(maybe_filter)
    session_calendar = [
        SessionCalendarEntry(
            instrument_id=str(row["instrument_id"]),
            timeframe=str(row["timeframe"]),
            session_date=str(row["session_date"]),
            session_open_ts=str(row["session_open_ts"]),
            session_close_ts=str(row["session_close_ts"]),
        )
        for row in read_delta_table_rows(calendar_path, filters=calendar_filters or None)
        if (
            (not instrument_ids or str(row.get("instrument_id")) in instrument_ids)
            and (not timeframe_set or str(row.get("timeframe")) in timeframe_set)
        )
    ]
    if str(_config_value(config, "series_mode", "contract")) == "continuous_front":
        bars, roll_map = load_continuous_front_as_research_context(
            continuous_front_output_dir=_materialized_output_dir(config),
            dataset_version=str(_config_value(config, "dataset_version")),
            instrument_ids=tuple(instrument_ids),
            timeframes=tuple(timeframes),
            start_ts=str(_config_value(config, "start_ts", "")) or None,
            end_ts=str(_config_value(config, "end_ts", "")) or None,
        )
        if contract_ids:
            bars = [row for row in bars if row.contract_id in contract_ids]
    else:
        bar_filters: list[tuple[str, str, object]] = []
        for maybe_filter in (
            _value_filter("contract_id", sorted(contract_ids)),
            _value_filter("instrument_id", sorted(instrument_ids)),
            _value_filter("timeframe", timeframes),
        ):
            if maybe_filter is not None:
                bar_filters.append(maybe_filter)
        if start_ts and warmup_bars <= 0:
            bar_filters.append(("ts", ">=", start_ts))
        if end_ts:
            bar_filters.append(("ts", "<=", end_ts))
        roll_map_filters: list[tuple[str, str, object]] = []
        roll_map_instrument_filter = _value_filter("instrument_id", sorted(instrument_ids))
        if roll_map_instrument_filter is not None:
            roll_map_filters.append(roll_map_instrument_filter)
        bars = [
            CanonicalBar.from_dict(row)
            for row in read_delta_table_rows(bars_path, filters=bar_filters or None)
            if (
                (not contract_ids or str(row.get("contract_id")) in contract_ids)
                and (not instrument_ids or str(row.get("instrument_id")) in instrument_ids)
                and (not timeframe_set or str(row.get("timeframe")) in timeframe_set)
                and (not start_ts or warmup_bars > 0 or str(row.get("ts")) >= start_ts)
                and (not end_ts or str(row.get("ts")) <= end_ts)
            )
        ]
        roll_map = [
            RollMapEntry(
                instrument_id=str(row["instrument_id"]),
                session_date=str(row["session_date"]),
                active_contract_id=str(row["active_contract_id"]),
                reason=str(row["reason"]),
            )
            for row in read_delta_table_rows(roll_map_path, filters=roll_map_filters or None)
            if not instrument_ids or str(row.get("instrument_id")) in instrument_ids
        ]
    return bars, session_calendar, roll_map


def _seed_manifest(config: dict[str, object]) -> ResearchDatasetManifest:
    series_mode = str(_config_value(config, "series_mode", "contract"))
    return ResearchDatasetManifest(
        dataset_version=str(_config_value(config, "dataset_version")),
        dataset_name=str(_config_value(config, "dataset_name", "research-materialized")),
        source_table="continuous_front_bars" if series_mode == "continuous_front" else "canonical_bars",
        universe_id=str(_config_value(config, "universe_id", "moex-futures")),
        timeframes=tuple(str(item) for item in _config_value(config, "timeframes")),
        base_timeframe=str(_config_value(config, "base_timeframe", "15m")),
        start_ts=str(_config_value(config, "start_ts", "")) or None,
        end_ts=str(_config_value(config, "end_ts", "")) or None,
        series_mode=series_mode,  # type: ignore[arg-type]
        split_method=str(_config_value(config, "split_method", "holdout")),  # type: ignore[arg-type]
        warmup_bars=int(_config_value(config, "warmup_bars", 200)),
        source_tables=CONTINUOUS_FRONT_TABLES if series_mode == "continuous_front" else (
            "canonical_bars",
            "canonical_session_calendar",
            "canonical_roll_map",
        ),
        continuous_front_policy=(
            ContinuousFrontPolicy.from_config(dict(_config_value(config, "continuous_front_policy", {})))
            if series_mode == "continuous_front"
            else None
        ),
        code_version=str(_config_value(config, "code_version", "research-data-prep")),
    )


@asset(group_name="research", config_schema=_research_config_schema())
def continuous_front_bars(context) -> dict[str, object]:
    config = dict(context.op_execution_context.op_config)
    materialized_output_dir = _materialized_output_dir(config)
    dataset_version = str(_config_value(config, "dataset_version"))
    series_mode = str(_config_value(config, "series_mode", "contract"))
    policy = ContinuousFrontPolicy.from_config(dict(_config_value(config, "continuous_front_policy", {})))
    run_id = str(_config_value(config, "campaign_run_id", "continuous_front_refresh"))
    if series_mode == "continuous_front":
        report = run_continuous_front_spark_job(
            dataset_version=dataset_version,
            canonical_bars_path=_canonical_table_path(config, "canonical_bars"),
            canonical_session_calendar_path=_canonical_table_path(config, "canonical_session_calendar"),
            canonical_roll_map_path=_canonical_table_path(config, "canonical_roll_map"),
            output_dir=materialized_output_dir,
            policy=policy,
            run_id=run_id,
            instrument_ids=tuple(str(item) for item in _config_value(config, "dataset_instrument_ids", [])),
            timeframes=tuple(str(item) for item in _config_value(config, "timeframes", [])),
            start_ts=str(_config_value(config, "start_ts", "")) or None,
            end_ts=str(_config_value(config, "end_ts", "")) or None,
        )
    else:
        report = _write_contract_mode_continuous_front_placeholders(
            output_dir=materialized_output_dir,
            dataset_version=dataset_version,
            policy=policy,
            run_id=run_id,
        )
    return {
        "materialized_output_dir": materialized_output_dir.as_posix(),
        "dataset_version": dataset_version,
        "status": report["status"],
        "output_paths": report["output_paths"],
        "rows_by_table": report["rows_by_table"],
        "qc_rows": report["qc_rows"],
        "spark_profile": report.get("spark_profile"),
    }


@asset(group_name="research")
def continuous_front_roll_events(continuous_front_bars: dict[str, object]) -> dict[str, object]:
    materialized_output_dir = Path(str(continuous_front_bars["materialized_output_dir"]))
    return _delta_table_summary(
        table_path=materialized_output_dir / "continuous_front_roll_events.delta",
        table_name="continuous_front_roll_events",
    )


@asset(group_name="research")
def continuous_front_adjustment_ladder(continuous_front_bars: dict[str, object]) -> dict[str, object]:
    materialized_output_dir = Path(str(continuous_front_bars["materialized_output_dir"]))
    return _delta_table_summary(
        table_path=materialized_output_dir / "continuous_front_adjustment_ladder.delta",
        table_name="continuous_front_adjustment_ladder",
    )


@asset(group_name="research")
def continuous_front_qc_report(
    continuous_front_bars: dict[str, object],
    continuous_front_roll_events: dict[str, object],
    continuous_front_adjustment_ladder: dict[str, object],
) -> dict[str, object]:
    materialized_output_dir = Path(str(continuous_front_bars["materialized_output_dir"]))
    summary = _delta_table_summary(
        table_path=materialized_output_dir / "continuous_front_qc_report.delta",
        table_name="continuous_front_qc_report",
    )
    summary["continuous_front_status"] = continuous_front_bars["status"]
    summary["roll_events"] = continuous_front_roll_events
    summary["adjustment_ladder"] = continuous_front_adjustment_ladder
    return summary


@asset(group_name="research", config_schema=_research_config_schema())
def research_datasets(context, continuous_front_qc_report: dict[str, object]) -> dict[str, object]:
    config = dict(context.op_execution_context.op_config)
    if (
        str(_config_value(config, "series_mode", "contract")) == "continuous_front"
        and str(continuous_front_qc_report.get("continuous_front_status")) != "PASS"
    ):
        raise RuntimeError("continuous_front research dataset materialization requires PASS QC")
    registry_root = Path(str(_config_value(config, "registry_root"))).resolve()
    materialized_output_dir = _materialized_output_dir(config)
    results_output_dir = _results_output_dir(config)
    dataset_version = str(_config_value(config, "dataset_version"))
    indicator_set_version = str(_config_value(config, "indicator_set_version", "indicators-v1"))
    derived_indicator_set_version = str(
        _config_value(config, "derived_indicator_set_version", DEFAULT_DERIVED_INDICATOR_SET_VERSION)
    )

    if _reuse_existing_materialization(config):
        _require_existing_data_prep(
            materialized_output_dir=materialized_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
        )
        report = {
            "dataset_manifest": _dataset_manifest_row(
                materialized_output_dir=materialized_output_dir,
                dataset_version=dataset_version,
            ),
            "instrument_tree_count": _delta_table_filtered_row_count(
                table_path=materialized_output_dir / "research_instrument_tree.delta",
                table_name="research_instrument_tree",
                filters=[("dataset_version", "=", dataset_version)],
            ),
            "bar_view_count": _delta_table_filtered_row_count(
                table_path=materialized_output_dir / "research_bar_views.delta",
                table_name="research_bar_views",
                filters=[("dataset_version", "=", dataset_version)],
            ),
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
    strategy_space_config = _config_value(config, "strategy_space", {})
    optimizer_policy = (
        dict(strategy_space_config.get("optimizer", {"engine": "grid"}))
        if isinstance(strategy_space_config, Mapping)
        else {"engine": "grid"}
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
        "backtest_request": {
            "strategy_space_id": str(_config_value(config, "strategy_space_id")),
            "search_specs": [dict(item) for item in _config_value(config, "search_specs", [])],
            "combination_count": int(_config_value(config, "combination_count", 1)),
            "param_batch_size": int(_config_value(config, "param_batch_size", 25)),
            "series_batch_size": int(_config_value(config, "series_batch_size", 4)),
            "timeframe": str(_config_value(config, "backtest_timeframe", str(_config_value(config, "base_timeframe", "15m")))),
            "contract_ids": tuple(str(item) for item in _config_value(config, "backtest_contract_ids", [])),
            "instrument_ids": tuple(str(item) for item in _config_value(config, "backtest_instrument_ids", [])),
            "optimizer_policy": optimizer_policy,
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
            "min_fold_count": int(_config_value(config, "min_fold_count", 1)),
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
            **continuous_front_store_contract(),
            **research_dataset_store_contract(),
            **indicator_store_contract(),
            **research_derived_indicator_store_contract(),
        },
    }


@asset(group_name="research")
def research_instrument_tree(research_datasets: dict[str, object]) -> dict[str, object]:
    return _materialized_table_manifest(research_datasets, "research_instrument_tree")


@asset(group_name="research")
def research_bar_views(research_datasets: dict[str, object]) -> dict[str, object]:
    return _materialized_table_manifest(research_datasets, "research_bar_views")


@asset(group_name="research")
def research_indicator_frames(
    research_datasets: dict[str, object],
    research_bar_views: dict[str, object],
) -> dict[str, object]:
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
    return _materialized_table_manifest(research_datasets, "research_indicator_frames")


@asset(group_name="research")
def research_derived_indicator_frames(
    research_datasets: dict[str, object],
    research_bar_views: dict[str, object],
    research_indicator_frames: dict[str, object],
) -> dict[str, object]:
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
    return _materialized_table_manifest(research_datasets, "research_derived_indicator_frames")


def _validate_strategy_seed_inventory(*, registry_root: Path) -> None:
    paths = {
        "research_strategy_families": registry_root / "research_strategy_families.delta",
        "research_strategy_templates": registry_root / "research_strategy_templates.delta",
        "research_strategy_template_modules": registry_root / "research_strategy_template_modules.delta",
    }
    for table_name, table_path in paths.items():
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing strategy registry table: {table_name} at {table_path.as_posix()}")

    family_rows = read_delta_table_rows(paths["research_strategy_families"])
    template_rows = read_delta_table_rows(paths["research_strategy_templates"])
    module_rows = read_delta_table_rows(paths["research_strategy_template_modules"])

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


def _backtest_request_config(research_datasets: dict[str, object]) -> BacktestBatchRequest:
    payload = dict(research_datasets["backtest_request"])
    return BacktestBatchRequest(
        campaign_run_id=str(research_datasets["campaign_run_id"]),
        strategy_space_id=str(payload["strategy_space_id"]),
        dataset_version=str(research_datasets["dataset_version"]),
        indicator_set_version=str(research_datasets["indicator_set_version"]),
        derived_indicator_set_version=str(research_datasets["derived_indicator_set_version"]),
        search_specs=tuple(
            StrategyFamilySearchSpec.from_dict(item)
            for item in payload["search_specs"]
        ),
        combination_count=int(payload["combination_count"]),
        param_batch_size=int(payload["param_batch_size"]),
        series_batch_size=int(payload["series_batch_size"]),
        timeframe=str(payload["timeframe"]),
        contract_ids=tuple(str(item) for item in payload["contract_ids"]),
        instrument_ids=tuple(str(item) for item in payload["instrument_ids"]),
        optimizer_policy=dict(payload.get("optimizer_policy", {"engine": "grid"})),
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
        min_fold_count=int(payload.get("min_fold_count", 1)),
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


RANKING_BACKTEST_RUN_COLUMNS = [
    "backtest_run_id",
    "campaign_run_id",
    "strategy_instance_id",
    "strategy_template_id",
    "family_id",
    "family_key",
    "strategy_version_label",
    "indicator_set_version",
    "derived_indicator_set_version",
    "contract_id",
    "instrument_id",
    "timeframe",
    "params_hash",
    "parameter_values_json",
]
RANKING_STAT_COLUMNS = [
    "backtest_run_id",
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
    "window_id",
    "params_hash",
    "total_return",
    "annualized_return",
    "sharpe",
    "sortino",
    "calmar",
    "max_drawdown",
    "profit_factor",
    "win_rate",
    "avg_trade",
    "turnover",
    "trade_count",
]
RANKING_TRADE_COLUMNS = [
    "backtest_run_id",
    "campaign_run_id",
    "contract_id",
    "instrument_id",
    "timeframe",
    "window_id",
    "entry_price",
    "exit_price",
    "qty",
    "gross_pnl",
    "net_pnl",
    "commission",
]


def _backtest_result_table_path(research_backtest_batches: dict[str, object], table_name: str) -> Path:
    output_paths = research_backtest_batches.get("output_paths", {})
    if not isinstance(output_paths, Mapping):
        output_paths = {}
    raw_path = output_paths.get(table_name)
    if raw_path is None:
        results_output_dir = Path(str(research_backtest_batches["results_output_dir"]))
        raw_path = (results_output_dir / f"{table_name}.delta").as_posix()
    return Path(str(raw_path))


def _backtest_result_table_manifest(research_backtest_batches: dict[str, object], table_name: str) -> dict[str, object]:
    table_path = _backtest_result_table_path(research_backtest_batches, table_name)
    row_counts = research_backtest_batches.get("row_counts", {})
    if not isinstance(row_counts, Mapping):
        row_counts = {}
    return {
        "table_name": table_name,
        "table_path": table_path.as_posix(),
        "row_count": int(row_counts.get(table_name, 0) or 0),
        "has_delta_log": has_delta_log(table_path),
    }


def _backtest_result_rows(
    research_backtest_batches: dict[str, object],
    table_name: str,
    *,
    columns: Sequence[str] | None = None,
) -> list[dict[str, object]]:
    table_path = _backtest_result_table_path(research_backtest_batches, table_name)
    if not has_delta_log(table_path):
        return []
    if columns is not None:
        frame = read_delta_table_frame(table_path, columns=list(columns))
        return [
            {str(key): item for key, item in row.items()}
            for row in frame.to_dict("records")
        ]
    return read_delta_table_rows(table_path)


@asset(group_name="research")
def research_backtest_batches(
    research_datasets: dict[str, object],
    research_indicator_frames: dict[str, object],
    research_derived_indicator_frames: dict[str, object],
    research_strategy_template_modules: list[dict[str, object]],
) -> dict[str, object]:
    del research_indicator_frames
    del research_derived_indicator_frames
    del research_strategy_template_modules
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    results_output_dir = Path(str(research_datasets["results_output_dir"]))
    report = run_backtest_batch(
        dataset_output_dir=materialized_output_dir,
        indicator_output_dir=materialized_output_dir,
        derived_indicator_output_dir=materialized_output_dir,
        output_dir=results_output_dir,
        request=_backtest_request_config(research_datasets),
        engine_config=_engine_config(research_datasets),
    )
    row_counts = {
        "research_strategy_search_specs": len(report["search_spec_rows"]),
        "research_vbt_search_runs": len(report["search_run_rows"]),
        "research_optimizer_studies": len(report["optimizer_study_rows"]),
        "research_optimizer_trials": len(report["optimizer_trial_rows"]),
        "research_vbt_param_results": len(report["param_result_rows"]),
        "research_vbt_param_gate_events": len(report["gate_event_rows"]),
        "research_vbt_ephemeral_indicator_cache": len(report["ephemeral_indicator_rows"]),
        "research_strategy_promotion_events": len(report["promotion_event_rows"]),
        "research_backtest_runs": len(report["run_rows"]),
        "research_strategy_stats": len(report["stat_rows"]),
        "research_trade_records": len(report["trade_rows"]),
        "research_order_records": len(report["order_rows"]),
        "research_drawdown_records": len(report["drawdown_rows"]),
    }
    return {
        "backtest_batch": dict(report["backtest_batch"]),
        "output_paths": dict(report["output_paths"]),
        "row_counts": row_counts,
        "results_output_dir": results_output_dir.as_posix(),
    }


@asset(group_name="research")
def research_strategy_search_specs(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_strategy_search_specs")


@asset(group_name="research")
def research_vbt_search_runs(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_vbt_search_runs")


@asset(group_name="research")
def research_optimizer_studies(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_optimizer_studies")


@asset(group_name="research")
def research_optimizer_trials(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_optimizer_trials")


@asset(group_name="research")
def research_vbt_param_results(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_vbt_param_results")


@asset(group_name="research")
def research_vbt_param_gate_events(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_vbt_param_gate_events")


@asset(group_name="research")
def research_vbt_ephemeral_indicator_cache(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_vbt_ephemeral_indicator_cache")


@asset(group_name="research")
def research_strategy_promotion_events(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_strategy_promotion_events")


@asset(group_name="research")
def research_backtest_runs(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_backtest_runs")


@asset(group_name="research")
def research_strategy_stats(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_strategy_stats")


@asset(group_name="research")
def research_trade_records(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_trade_records")


@asset(group_name="research")
def research_order_records(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_order_records")


@asset(group_name="research")
def research_drawdown_records(research_backtest_batches: dict[str, object]) -> dict[str, object]:
    return _backtest_result_table_manifest(research_backtest_batches, "research_drawdown_records")


@asset(group_name="research")
def research_strategy_rankings(
    research_datasets: dict[str, object],
    research_backtest_runs: dict[str, object],
    research_strategy_stats: dict[str, object],
    research_trade_records: dict[str, object],
) -> dict[str, object]:
    del research_backtest_runs
    del research_strategy_stats
    del research_trade_records
    results_output_dir = Path(str(research_datasets["results_output_dir"]))
    report = rank_backtest_results(
        backtest_output_dir=results_output_dir,
        output_dir=results_output_dir,
        policy=_ranking_policy(research_datasets),
    )
    return _backtest_result_table_manifest(
        {
            "output_paths": report.get("output_paths", {}),
            "row_counts": {
                "research_strategy_rankings": len(report.get("ranking_rows", ())),
                "research_run_findings": len(report.get("finding_rows", ())),
            },
            "results_output_dir": results_output_dir.as_posix(),
        },
        "research_strategy_rankings",
    )


@asset(group_name="research")
def research_signal_candidates(
    research_datasets: dict[str, object],
    research_derived_indicator_frames: dict[str, object],
    research_strategy_rankings: dict[str, object],
) -> dict[str, object]:
    del research_derived_indicator_frames
    materialized_output_dir = Path(str(research_datasets["materialized_output_dir"]))
    results_output_dir = Path(str(research_datasets["results_output_dir"]))
    ranking_table_path = Path(str(research_strategy_rankings["table_path"]))
    ranking_rows = read_delta_table_rows(ranking_table_path) if has_delta_log(ranking_table_path) else []
    report = project_runtime_candidates(
        dataset_output_dir=materialized_output_dir,
        indicator_output_dir=materialized_output_dir,
        derived_indicator_output_dir=materialized_output_dir,
        output_dir=results_output_dir,
        request=_projection_request(research_datasets),
        ranking_rows=ranking_rows,
        config=_engine_config(research_datasets),
    )
    candidate_path = Path(str(report["output_paths"]["research_signal_candidates"]))
    return {
        "table_name": "research_signal_candidates",
        "table_path": candidate_path.as_posix(),
        "row_count": len(report.get("candidate_rows", ())),
        "has_delta_log": has_delta_log(candidate_path),
    }


RESEARCH_ASSETS = (
    continuous_front_bars,
    continuous_front_roll_events,
    continuous_front_adjustment_ladder,
    continuous_front_qc_report,
    research_datasets,
    research_instrument_tree,
    research_bar_views,
    research_indicator_frames,
    research_derived_indicator_frames,
    research_strategy_families,
    research_strategy_templates,
    research_strategy_template_modules,
    research_backtest_batches,
    research_strategy_search_specs,
    research_vbt_search_runs,
    research_optimizer_studies,
    research_optimizer_trials,
    research_vbt_param_results,
    research_vbt_param_gate_events,
    research_vbt_ephemeral_indicator_cache,
    research_strategy_promotion_events,
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
        continuous_front_bars,
        continuous_front_roll_events,
        continuous_front_adjustment_ladder,
        continuous_front_qc_report,
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
    ),
)

research_backtest_job = define_asset_job(
    name=RESEARCH_BACKTEST_JOB_NAME,
    selection=AssetSelection.assets(
        research_backtest_batches,
        research_strategy_search_specs,
        research_vbt_search_runs,
        research_optimizer_studies,
        research_optimizer_trials,
        research_vbt_param_results,
        research_vbt_param_gate_events,
        research_vbt_ephemeral_indicator_cache,
        research_strategy_promotion_events,
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
    series_mode: str = "continuous_front",
    continuous_front_policy: dict[str, object] | None = None,
    campaign_run_id: str = "research_data_prep_scheduled",
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
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
    research_config = _research_run_config(
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
        series_mode=series_mode,
        continuous_front_policy=continuous_front_policy,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
        code_version="research-data-prep-after-moex",
        strategy_space_id="",
        strategy_instances=(),
        combination_count=0,
    )
    return {
        "ops": {
            "continuous_front_bars": {
                "config": research_config,
            },
            "research_datasets": {
                "config": research_config,
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
            series_mode="continuous_front",
            continuous_front_policy=ContinuousFrontPolicy.from_config().to_config_dict(),
        ),
        tags={
            "ta3000/upstream_job": MOEX_BASELINE_UPDATE_JOB_NAME,
            "ta3000/upstream_run_id": upstream_run_id,
            "ta3000/dataset_version": dataset_version,
            "ta3000/series_mode": "continuous_front",
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
    continuous_front_policy: dict[str, object] | None = None,
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str,
    indicator_profile_version: str,
    derived_indicator_set_version: str,
    derived_indicator_profile_version: str,
    code_version: str = "research-orchestration",
    strategy_space: dict[str, object] | None = None,
    strategy_space_id: str = "",
    search_specs: Sequence[dict[str, object]] = (),
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
    min_fold_count: int = 1,
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
        "continuous_front_policy": dict(
            continuous_front_policy or ContinuousFrontPolicy.from_config().to_config_dict()
        ),
        "dataset_contract_ids": [str(item) for item in dataset_contract_ids],
        "dataset_instrument_ids": [str(item) for item in dataset_instrument_ids],
        "indicator_set_version": indicator_set_version,
        "indicator_profile_version": indicator_profile_version,
        "derived_indicator_set_version": derived_indicator_set_version,
        "derived_indicator_profile_version": derived_indicator_profile_version,
        "code_version": code_version,
        "strategy_space": resolved_strategy_space,
        "strategy_space_id": strategy_space_id,
        "search_specs": [dict(item) for item in search_specs],
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
        "min_fold_count": min_fold_count,
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
    continuous_front_policy: dict[str, object] | None = None,
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    default_assets: Sequence[str],
    allowed_assets: Sequence[str],
    selection: Sequence[str] | None = None,
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
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
    min_fold_count: int = 1,
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
    search_specs: list[dict[str, object]] = []
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
        search_specs = [spec.to_dict() for spec in prepared_strategy_space.family_search_specs]
        resolved_combination_count = 0
        optimizer = dict((strategy_space or _default_strategy_space()).get("optimizer", {})) if isinstance(strategy_space or {}, dict) else {}
        if str(optimizer.get("engine", "grid")) == "optuna":
            per_spec_count = int(optimizer.get("n_trials", 0) or 0)
            resolved_combination_count = per_spec_count * len(prepared_strategy_space.family_search_specs)
        else:
            for spec in prepared_strategy_space.family_search_specs:
                count = 1
                for values in spec.search_spec.parameter_space.values():
                    count *= max(1, len(values))
                resolved_combination_count += count
    output_paths = _research_output_paths(
        registry_root=registry_root,
        materialized_output_dir=resolved_materialized_output_dir,
        results_output_dir=resolved_results_output_dir,
    )
    research_config = _research_run_config(
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
        continuous_front_policy=continuous_front_policy,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
        code_version=code_version,
        strategy_space=dict(strategy_space or _default_strategy_space()),
        strategy_space_id=strategy_space_id,
        search_specs=search_specs,
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
        min_fold_count=min_fold_count,
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

    result = materialize(
        assets=list(RESEARCH_ASSETS),
        selection=expected_materialized_assets,
        run_config={
            "ops": {
                "continuous_front_bars": {
                    "config": research_config,
                },
                "research_datasets": {
                    "config": research_config,
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
        rows_by_table[asset_name] = count_delta_table_rows(table_path)
    findings_path = Path(output_paths["research_run_findings"])
    if has_delta_log(findings_path):
        rows_by_table["research_run_findings"] = count_delta_table_rows(findings_path)
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
    continuous_front_policy: dict[str, object] | None = None,
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
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
        continuous_front_policy=continuous_front_policy,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=RESEARCH_DATA_PREP_ASSETS,
        allowed_assets=RESEARCH_DATA_PREP_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
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
    continuous_front_policy: dict[str, object] | None = None,
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
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
        continuous_front_policy=continuous_front_policy,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=STRATEGY_REGISTRY_REFRESH_ASSETS,
        allowed_assets=STRATEGY_REGISTRY_REFRESH_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
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
    continuous_front_policy: dict[str, object] | None = None,
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
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
    min_fold_count: int = 1,
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
        continuous_front_policy=continuous_front_policy,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=RESEARCH_BACKTEST_ASSETS,
        allowed_assets=RESEARCH_BACKTEST_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
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
        min_fold_count=min_fold_count,
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
    continuous_front_policy: dict[str, object] | None = None,
    dataset_contract_ids: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    derived_indicator_profile_version: str = "core_v1",
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
    min_fold_count: int = 1,
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
        continuous_front_policy=continuous_front_policy,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        default_assets=RESEARCH_PROJECTION_ASSETS,
        allowed_assets=RESEARCH_PROJECTION_ASSETS,
        selection=selection,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        derived_indicator_set_version=derived_indicator_set_version,
        derived_indicator_profile_version=derived_indicator_profile_version,
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
        min_fold_count=min_fold_count,
        max_drawdown_cap=max_drawdown_cap,
        min_positive_fold_ratio=min_positive_fold_ratio,
        stress_slippage_bps=stress_slippage_bps,
        min_parameter_stability=min_parameter_stability,
        min_slippage_score=min_slippage_score,
        reuse_existing_materialization=reuse_existing_materialization,
        raise_on_error=raise_on_error,
    )
