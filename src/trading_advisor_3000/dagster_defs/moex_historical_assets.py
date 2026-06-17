from __future__ import annotations

# ruff: noqa: E501
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from dagster import (
    AssetSelection,
    DagsterInstance,
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
    job,
    materialize,
    op,
)
from dagster import (
    Field as DagsterField,
)

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log
from trading_advisor_3000.product_plane.data_plane.moex.baseline_update import (
    BASELINE_UPDATE_REPORT_FILENAME,
    DEFAULT_CF_CATCH_UP_TIMEFRAMES,
    ECONOMICS_REFRESH_MODE_REFRESH,
    run_moex_baseline_update,
)
from trading_advisor_3000.product_plane.data_plane.moex.data_rebuild_profiles import (
    MOEX_REBUILD_DOWNSTREAM_MODES,
    MOEX_REBUILD_PUBLISH_MODES,
    MOEX_REBUILD_SOURCE_MODES,
    build_moex_data_rebuild_manifest,
    resolve_moex_data_rebuild_profile,
    write_moex_data_rebuild_manifest,
)
from trading_advisor_3000.product_plane.data_plane.moex.historical_canonical_route import (
    CANONICAL_MERGE_SCOPED_DELETE_INSERT,
    run_historical_canonical_route,
)
from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (
    validate_moex_runtime_run_id,
)
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    BASELINE_UPDATE_STORAGE_DIRNAME,
    CANONICAL_BASELINE_BARS_FILENAME,
    CANONICAL_BASELINE_PROVENANCE_FILENAME,
    CANONICAL_BASELINE_ROLL_MAP_FILENAME,
    CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
    CANONICAL_BASELINE_SESSION_CALENDAR_FILENAME,
    CANONICAL_BASELINE_SESSION_INTERVALS_FILENAME,
    CANONICAL_REFRESH_REPORT_FILENAME,
    CANONICAL_REFRESH_STORAGE_DIRNAME,
    RAW_BASELINE_TABLE_RELATIVE_PATH,
    RAW_INGEST_STORAGE_DIRNAME,
    ROUTE_REFRESH_REPORT_FILENAME,
    ROUTE_REFRESH_STORAGE_DIRNAME,
    configured_moex_historical_data_root,
    resolve_external_root,
)

PASS_LIKE_RAW_STATUSES = {"PASS", "PASS-NOOP"}
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MAPPING_REGISTRY = (
    REPO_ROOT / "configs" / "moex_foundation" / "instrument_mapping_registry.v1.yaml"
)
DEFAULT_UNIVERSE = (
    REPO_ROOT / "configs" / "moex_foundation" / "universe" / "moex-futures-priority.v1.yaml"
)
DEFAULT_TIMEFRAMES = "1m,5m,15m,1h,4h,1d,1w"
DEFAULT_RESEARCH_REBUILD_WARMUP_BARS = 300
DEFAULT_WORKERS = 4
DEFAULT_BATCH_SIZE = 250_000
DEFAULT_EXECUTION_MODE = "parallel"
DEFAULT_DAILY_REFRESH_WINDOW_DAYS = 7
DEFAULT_BOOTSTRAP_WINDOW_DAYS = DEFAULT_DAILY_REFRESH_WINDOW_DAYS
DEFAULT_STABILITY_LAG_MINUTES = 20
DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS = 14
DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS = 45
DEFAULT_REFRESH_OVERLAP_MINUTES = 180
DEFAULT_MAX_CHANGED_WINDOW_DAYS = 10


@dataclass(frozen=True)
class AssetSpec:
    key: str
    description: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


MOEX_HISTORICAL_ASSET_KEYS = (
    "moex_raw_ingest",
    "moex_canonical_refresh",
)

MOEX_HISTORICAL_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "moex_raw_ingest": tuple(),
    "moex_canonical_refresh": ("moex_raw_ingest",),
}

MOEX_HISTORICAL_OP_CONFIG_SCHEMA = {
    "raw_table_path": DagsterField(str, is_required=False),
    "raw_ingest_report_path": DagsterField(str, is_required=False),
    "canonical_output_dir": DagsterField(str, is_required=False),
    "canonical_target_output_dir": DagsterField(str, is_required=False),
    "canonical_bars_path": DagsterField(str, is_required=False),
    "canonical_provenance_path": DagsterField(str, is_required=False),
    "canonical_session_intervals_path": DagsterField(str, is_required=False),
    "canonical_session_calendar_path": DagsterField(str, is_required=False),
    "canonical_roll_map_path": DagsterField(str, is_required=False),
    "canonical_run_id": DagsterField(str, is_required=False),
    "mapping_registry_path": DagsterField(str, is_required=False),
    "universe_path": DagsterField(str, is_required=False),
    "raw_ingest_root": DagsterField(str, is_required=False),
    "canonicalization_root": DagsterField(str, is_required=False),
    "route_refresh_root": DagsterField(str, is_required=False),
    "timeframes": DagsterField(str, is_required=False),
    "workers": DagsterField(int, is_required=False),
    "batch_size": DagsterField(int, is_required=False),
    "execution_mode": DagsterField(str, is_required=False),
    "bootstrap_window_days": DagsterField(int, is_required=False),
    "stability_lag_minutes": DagsterField(int, is_required=False),
    "expand_contract_chain": DagsterField(bool, is_required=False),
    "contract_discovery_step_days": DagsterField(int, is_required=False),
    "contract_discovery_lookback_days": DagsterField(int, is_required=False),
    "refresh_overlap_minutes": DagsterField(int, is_required=False),
    "ingest_till_utc": DagsterField(str, is_required=False),
    "run_id": DagsterField(str, is_required=False),
    "profile_name": DagsterField(str, is_required=False),
    "source_mode": DagsterField(str, is_required=False),
    "publish_mode": DagsterField(str, is_required=False),
    "downstream_mode": DagsterField(str, is_required=False),
    "raw_root": DagsterField(str, is_required=False),
    "canonical_root": DagsterField(str, is_required=False),
    "session_root": DagsterField(str, is_required=False),
    "research_root": DagsterField(str, is_required=False),
    "research_output_dir": DagsterField(str, is_required=False),
    "materialized_output_dir": DagsterField(str, is_required=False),
    "results_output_dir": DagsterField(str, is_required=False),
    "manifest_path": DagsterField(str, is_required=False),
    "dataset_version": DagsterField(str, is_required=False),
    "dataset_name": DagsterField(str, is_required=False),
    "universe_id": DagsterField(str, is_required=False),
    "base_timeframe": DagsterField(str, is_required=False),
    "start_ts": DagsterField(str, is_required=False),
    "end_ts": DagsterField(str, is_required=False),
    "warmup_bars": DagsterField(int, is_required=False),
    "split_method": DagsterField(str, is_required=False),
    "series_mode": DagsterField(str, is_required=False),
    "dataset_contract_ids": DagsterField(str, is_required=False),
    "dataset_instrument_ids": DagsterField(str, is_required=False),
    "indicator_set_version": DagsterField(str, is_required=False),
    "indicator_profile_version": DagsterField(str, is_required=False),
    "derived_indicator_set_version": DagsterField(str, is_required=False),
    "derived_indicator_profile_version": DagsterField(str, is_required=False),
    "spark_master": DagsterField(str, is_required=False),
    "volume_profile_raw_1m_table_path": DagsterField(str, is_required=False),
    "volume_profile_tick_size_by_instrument": DagsterField(dict, is_required=False),
    "reuse_existing_materialization": DagsterField(bool, is_required=False),
    "continuous_front_policy": DagsterField(dict, is_required=False),
}
MOEX_DATA_REBUILD_OP_NAME = "moex_data_rebuild"
MOEX_BASELINE_UPDATE_OP_CONFIG_SCHEMA = {
    "mapping_registry_path": DagsterField(str, is_required=False),
    "universe_path": DagsterField(str, is_required=False),
    "raw_table_path": DagsterField(str, is_required=False),
    "canonical_bars_path": DagsterField(str, is_required=False),
    "canonical_provenance_path": DagsterField(str, is_required=False),
    "canonical_session_intervals_path": DagsterField(str, is_required=False),
    "canonical_session_calendar_path": DagsterField(str, is_required=False),
    "canonical_roll_map_path": DagsterField(str, is_required=False),
    "economics_mode": DagsterField(str, is_required=False),
    "raw_economics_root": DagsterField(str, is_required=False),
    "canonical_economics_root": DagsterField(str, is_required=False),
    "evidence_root": DagsterField(str, is_required=False),
    "timeframes": DagsterField(str, is_required=False),
    "refresh_window_days": DagsterField(int, is_required=False),
    "contract_discovery_lookback_days": DagsterField(int, is_required=False),
    "contract_discovery_step_days": DagsterField(int, is_required=False),
    "refresh_overlap_minutes": DagsterField(int, is_required=False),
    "cf_catch_up_timeframes": DagsterField(str, is_required=False),
    "cf_catch_up_overlap_minutes": DagsterField(int, is_required=False),
    "max_changed_window_days": DagsterField(int, is_required=False),
    "stability_lag_minutes": DagsterField(int, is_required=False),
    "expand_contract_chain": DagsterField(bool, is_required=False),
    "coverage_mode": DagsterField(str, is_required=False),
    "ingest_till_utc": DagsterField(str, is_required=False),
    "run_id": DagsterField(str, is_required=False),
}
MOEX_DATA_REBUILD_JOB_NAME = "moex_data_rebuild_job"
MOEX_HISTORICAL_CUTOVER_JOB_NAME = "moex_historical_cutover_job"
MOEX_BASELINE_UPDATE_JOB_NAME = "moex_baseline_update_job"
MOEX_BASELINE_DAILY_SCHEDULE_NAME = "moex_baseline_daily_update_schedule"
MOEX_BASELINE_DAILY_CRON = "0 2 * * *"
MOEX_HISTORICAL_NIGHTLY_CRON = MOEX_BASELINE_DAILY_CRON
MOEX_HISTORICAL_EXECUTION_TIMEZONE = "Europe/Moscow"
MOEX_HISTORICAL_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)
MOEX_BASELINE_UPDATE_RETRY_POLICY = RetryPolicy(max_retries=0, delay=0)
MOEX_BASELINE_UPDATE_HOT_TABLE_RUNTIME = "spark_delta_raw_tail+spark_delta_canonical"
MOEX_BASELINE_UPDATE_RUNTIME_BOUNDARY_TAG = "dagster+spark_delta_raw_tail+spark_delta_canonical"


def moex_historical_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="moex_baseline_update",
            description=(
                "Daily baseline updater for MOEX historical data. "
                "Appends/merges fresh raw bars into the stable raw baseline and Spark-refreshes only changed canonical windows."
            ),
            inputs=(
                "moex_iss",
                "mapping_registry",
                "universe",
                "baseline_raw",
                "baseline_canonical",
            ),
            outputs=(
                "baseline-update-report.json",
                "canonical_session_calendar.delta",
                "canonical_session_intervals.delta",
                "canonical_roll_map.delta",
            ),
        ),
        AssetSpec(
            key="moex_raw_ingest",
            description=(
                "Canonical raw-ingest owner node for MOEX historical refresh. "
                "Runs the existing Python raw-ingest contour and emits the authoritative "
                "raw table plus raw-ingest report for downstream canonicalization."
            ),
            inputs=("moex_iss", "mapping_registry", "universe"),
            outputs=("raw_ingest_owner_payload",),
        ),
        AssetSpec(
            key="moex_canonical_refresh",
            description=(
                "Spark canonicalization owner node for MOEX historical refresh. "
                "Blocked unless raw-ingest status is PASS/PASS-NOOP."
            ),
            inputs=("raw_ingest_owner_payload",),
            outputs=(
                "canonicalization-report.json",
                "canonical_bars.delta",
                "canonical_bar_provenance.delta",
                "canonical_session_calendar.delta",
                "canonical_session_intervals.delta",
                "canonical_roll_map.delta",
            ),
        ),
    ]


def _route_artifact_root() -> Path:
    return configured_moex_historical_data_root(repo_root=REPO_ROOT)


def _default_route_run_id(*, scheduled_execution_time: datetime | None = None) -> str:
    anchor = (
        scheduled_execution_time.astimezone(UTC)
        if scheduled_execution_time
        else datetime.now(tz=UTC)
    )
    return anchor.strftime("%Y%m%dT%H%M%SZ")


def _build_nightly_schedule_run_config(context) -> dict[str, object]:
    scheduled_execution_time = getattr(context, "scheduled_execution_time", None)
    run_id = _default_route_run_id(scheduled_execution_time=scheduled_execution_time)
    artifact_root = _route_artifact_root()
    canonical_output_dir = artifact_root / CANONICAL_REFRESH_STORAGE_DIRNAME / run_id
    return {
        "ops": {
            MOEX_DATA_REBUILD_OP_NAME: {
                "config": {
                    "profile_name": "full_raw_to_canonical",
                    "source_mode": "full_raw_ingest",
                    "publish_mode": "staging_only",
                    "downstream_mode": "invalidate",
                    "canonical_output_dir": canonical_output_dir.as_posix(),
                    "canonical_run_id": run_id,
                    "mapping_registry_path": DEFAULT_MAPPING_REGISTRY.as_posix(),
                    "universe_path": DEFAULT_UNIVERSE.as_posix(),
                    "raw_ingest_root": (artifact_root / RAW_INGEST_STORAGE_DIRNAME).as_posix(),
                    "canonicalization_root": (
                        artifact_root / CANONICAL_REFRESH_STORAGE_DIRNAME
                    ).as_posix(),
                    "route_refresh_root": (
                        artifact_root / ROUTE_REFRESH_STORAGE_DIRNAME
                    ).as_posix(),
                    "timeframes": DEFAULT_TIMEFRAMES,
                    "workers": DEFAULT_WORKERS,
                    "batch_size": DEFAULT_BATCH_SIZE,
                    "execution_mode": DEFAULT_EXECUTION_MODE,
                    "bootstrap_window_days": DEFAULT_BOOTSTRAP_WINDOW_DAYS,
                    "stability_lag_minutes": DEFAULT_STABILITY_LAG_MINUTES,
                    "expand_contract_chain": True,
                    "contract_discovery_step_days": DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
                    "contract_discovery_lookback_days": DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS,
                    "refresh_overlap_minutes": DEFAULT_REFRESH_OVERLAP_MINUTES,
                    "run_id": run_id,
                }
            },
        }
    }


def _split_timeframes(raw: str) -> set[str]:
    values = {item.strip() for item in raw.split(",") if item.strip()}
    if not values:
        raise RuntimeError("moex baseline update requires at least one timeframe")
    return values


def _baseline_paths_from_root(root: Path) -> dict[str, Path]:
    canonical_root = root / CANONICAL_BASELINE_ROOT_RELATIVE_PATH
    raw_table_path = root / RAW_BASELINE_TABLE_RELATIVE_PATH
    return {
        "raw_table_path": raw_table_path,
        "canonical_bars_path": canonical_root / CANONICAL_BASELINE_BARS_FILENAME,
        "canonical_provenance_path": canonical_root / CANONICAL_BASELINE_PROVENANCE_FILENAME,
        "canonical_session_intervals_path": canonical_root
        / CANONICAL_BASELINE_SESSION_INTERVALS_FILENAME,
        "canonical_session_calendar_path": canonical_root
        / CANONICAL_BASELINE_SESSION_CALENDAR_FILENAME,
        "canonical_roll_map_path": canonical_root / CANONICAL_BASELINE_ROLL_MAP_FILENAME,
        "raw_economics_root": root / "raw" / "economics",
        "canonical_economics_root": root / "canonical" / "economics",
        "evidence_root": root / BASELINE_UPDATE_STORAGE_DIRNAME,
    }


def build_moex_baseline_update_run_config(
    *,
    baseline_root: Path,
    run_id: str,
    ingest_till_utc: str | None = None,
    mapping_registry_path: Path = DEFAULT_MAPPING_REGISTRY,
    universe_path: Path = DEFAULT_UNIVERSE,
    timeframes: str | Sequence[str] = DEFAULT_TIMEFRAMES,
    refresh_window_days: int = DEFAULT_DAILY_REFRESH_WINDOW_DAYS,
    contract_discovery_lookback_days: int = DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS,
    contract_discovery_step_days: int = DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    refresh_overlap_minutes: int = DEFAULT_REFRESH_OVERLAP_MINUTES,
    cf_catch_up_timeframes: str | Sequence[str] | None = None,
    cf_catch_up_overlap_minutes: int | None = None,
    max_changed_window_days: int = DEFAULT_MAX_CHANGED_WINDOW_DAYS,
    stability_lag_minutes: int = DEFAULT_STABILITY_LAG_MINUTES,
    expand_contract_chain: bool = True,
    coverage_mode: str = "local_tail",
    economics_mode: str = ECONOMICS_REFRESH_MODE_REFRESH,
) -> dict[str, object]:
    resolved_run_id = validate_moex_runtime_run_id(
        run_id, name="build_moex_baseline_update_run_config.run_id"
    )
    resolved_root = baseline_root.resolve()
    resolved_ingest_till_utc = (
        ingest_till_utc.strip()
        if ingest_till_utc and ingest_till_utc.strip()
        else datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    resolved_timeframes = (
        timeframes.strip()
        if isinstance(timeframes, str)
        else ",".join(str(item).strip() for item in timeframes if str(item).strip())
    )
    resolved_cf_catch_up_timeframes = (
        ",".join(DEFAULT_CF_CATCH_UP_TIMEFRAMES)
        if cf_catch_up_timeframes is None
        else (
            cf_catch_up_timeframes.strip()
            if isinstance(cf_catch_up_timeframes, str)
            else ",".join(str(item).strip() for item in cf_catch_up_timeframes if str(item).strip())
        )
    )
    paths = _baseline_paths_from_root(resolved_root)
    return {
        "ops": {
            "moex_baseline_update": {
                "config": {
                    "mapping_registry_path": mapping_registry_path.resolve().as_posix(),
                    "universe_path": universe_path.resolve().as_posix(),
                    "raw_table_path": paths["raw_table_path"].as_posix(),
                    "canonical_bars_path": paths["canonical_bars_path"].as_posix(),
                    "canonical_provenance_path": paths["canonical_provenance_path"].as_posix(),
                    "canonical_session_intervals_path": paths[
                        "canonical_session_intervals_path"
                    ].as_posix(),
                    "canonical_session_calendar_path": paths[
                        "canonical_session_calendar_path"
                    ].as_posix(),
                    "canonical_roll_map_path": paths["canonical_roll_map_path"].as_posix(),
                    "economics_mode": str(economics_mode),
                    "raw_economics_root": paths["raw_economics_root"].as_posix(),
                    "canonical_economics_root": paths["canonical_economics_root"].as_posix(),
                    "evidence_root": paths["evidence_root"].as_posix(),
                    "timeframes": resolved_timeframes,
                    "refresh_window_days": int(refresh_window_days),
                    "contract_discovery_lookback_days": int(contract_discovery_lookback_days),
                    "contract_discovery_step_days": int(contract_discovery_step_days),
                    "refresh_overlap_minutes": int(refresh_overlap_minutes),
                    **(
                        {"cf_catch_up_timeframes": resolved_cf_catch_up_timeframes}
                        if resolved_cf_catch_up_timeframes
                        else {}
                    ),
                    **(
                        {"cf_catch_up_overlap_minutes": int(cf_catch_up_overlap_minutes)}
                        if cf_catch_up_overlap_minutes is not None
                        else {}
                    ),
                    "max_changed_window_days": int(max_changed_window_days),
                    "stability_lag_minutes": int(stability_lag_minutes),
                    "expand_contract_chain": bool(expand_contract_chain),
                    "coverage_mode": coverage_mode,
                    "ingest_till_utc": resolved_ingest_till_utc,
                    "run_id": resolved_run_id,
                }
            },
        }
    }


def _build_baseline_daily_schedule_run_config(context) -> dict[str, object]:
    scheduled_execution_time = getattr(context, "scheduled_execution_time", None)
    run_id = _default_route_run_id(scheduled_execution_time=scheduled_execution_time)
    ingest_till_utc = (
        scheduled_execution_time.astimezone(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
        if scheduled_execution_time is not None
        else datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    return build_moex_baseline_update_run_config(
        baseline_root=_route_artifact_root(),
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
    )


def _op_config_from_context(context) -> dict[str, object]:
    execution_context = getattr(context, "op_execution_context", None)
    op_config = getattr(execution_context, "op_config", None)
    if not isinstance(op_config, dict):
        op_config = getattr(context, "op_config", None)
    if not isinstance(op_config, dict):
        raise RuntimeError("moex historical asset context is missing op_config mapping")
    return dict(op_config)


def _text_value(op_config: dict[str, object], *keys: str) -> str:
    for key in keys:
        value = str(op_config.get(key, "")).strip()
        if value:
            return value
    return ""


def _int_value(op_config: dict[str, object], *keys: str, default: int) -> int:
    for key in keys:
        value = op_config.get(key)
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"moex historical asset op_config requires integer `{key}`") from exc
    return default


def _bool_value(op_config: dict[str, object], *keys: str, default: bool) -> bool:
    for key in keys:
        value = op_config.get(key)
        if isinstance(value, bool):
            return value
        if value is None or value == "":
            continue
        lowered = str(value).strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        raise RuntimeError(f"moex historical asset op_config requires boolean `{key}`")
    return default


def _read_raw_ingest_report(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"raw ingest report must be json object: {path.as_posix()}")
    return payload


def build_moex_historical_op_config(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    canonical_output_dir: Path,
    canonical_run_id: str,
    canonical_session_intervals_path: Path | None = None,
) -> dict[str, str]:
    config = {
        "raw_table_path": raw_table_path.resolve().as_posix(),
        "raw_ingest_report_path": raw_ingest_report_path.resolve().as_posix(),
        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
        "canonical_run_id": str(canonical_run_id).strip(),
    }
    if canonical_session_intervals_path is not None:
        config["canonical_session_intervals_path"] = (
            canonical_session_intervals_path.resolve().as_posix()
        )
    return config


def build_moex_data_rebuild_op_config(
    *,
    profile_name: str,
    canonical_run_id: str,
    canonical_output_dir: Path,
    canonical_target_output_dir: Path | None = None,
    raw_table_path: Path | None = None,
    raw_ingest_report_path: Path | None = None,
    canonical_bars_path: Path | None = None,
    canonical_provenance_path: Path | None = None,
    canonical_session_intervals_path: Path | None = None,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    source_mode: str | None = None,
    publish_mode: str = "promote",
    downstream_mode: str = "invalidate",
    raw_root: Path | None = None,
    canonical_root: Path | None = None,
    session_root: Path | None = None,
    research_root: Path | None = None,
    dataset_version: str | None = None,
    timeframes: str | Sequence[str] | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    warmup_bars: int | None = None,
    split_method: str | None = None,
    dataset_contract_ids: Sequence[str] | None = None,
    dataset_instrument_ids: Sequence[str] | None = None,
    spark_master: str | None = None,
) -> dict[str, object]:
    profile = resolve_moex_data_rebuild_profile(profile_name)
    resolved_source_mode = (source_mode or profile.source_mode).strip()
    if resolved_source_mode not in MOEX_REBUILD_SOURCE_MODES:
        raise ValueError(
            f"unknown MOEX data rebuild source_mode `{resolved_source_mode}`; "
            f"allowed modes: {', '.join(MOEX_REBUILD_SOURCE_MODES)}"
        )
    resolved_publish_mode = publish_mode.strip() or "promote"
    if resolved_publish_mode not in MOEX_REBUILD_PUBLISH_MODES:
        raise ValueError(
            f"unknown MOEX data rebuild publish_mode `{resolved_publish_mode}`; "
            f"allowed modes: {', '.join(MOEX_REBUILD_PUBLISH_MODES)}"
        )
    resolved_downstream_mode = downstream_mode.strip() or "invalidate"
    if resolved_downstream_mode not in MOEX_REBUILD_DOWNSTREAM_MODES:
        raise ValueError(
            f"unknown MOEX data rebuild downstream_mode `{resolved_downstream_mode}`; "
            f"allowed modes: {', '.join(MOEX_REBUILD_DOWNSTREAM_MODES)}"
        )
    requires_existing_raw_artifacts = resolved_source_mode == "existing_raw_delta" and bool(
        set(profile.stage_names).intersection({"sessions", "canonical"})
    )
    if requires_existing_raw_artifacts and (
        raw_table_path is None or raw_ingest_report_path is None
    ):
        raise ValueError(
            "canonical_from_existing_raw rebuild requires raw_table_path and raw_ingest_report_path"
        )
    requires_canonical_target_contract = bool(
        set(profile.stage_names).intersection({"raw", "sessions", "canonical"})
    )
    explicit_target_paths = {
        "canonical_bars_path": canonical_bars_path,
        "canonical_provenance_path": canonical_provenance_path,
        "canonical_session_calendar_path": canonical_session_calendar_path,
        "canonical_roll_map_path": canonical_roll_map_path,
    }
    supplied_explicit_targets = [
        key for key, value in explicit_target_paths.items() if value is not None
    ]
    if supplied_explicit_targets and len(supplied_explicit_targets) != len(explicit_target_paths):
        missing_targets = sorted(
            key for key, value in explicit_target_paths.items() if value is None
        )
        raise ValueError(
            "canonical rebuild explicit target paths must be supplied as a complete set; "
            f"missing: {', '.join(missing_targets)}"
        )
    if (
        requires_canonical_target_contract
        and resolved_publish_mode == "promote"
        and canonical_target_output_dir is None
        and not supplied_explicit_targets
    ):
        raise ValueError(
            "canonical rebuild publish_mode=promote requires canonical_target_output_dir "
            "or explicit target paths for canonical bars, provenance, session calendar, and roll map"
        )
    config: dict[str, str] = {
        "profile_name": profile.name,
        "source_mode": resolved_source_mode,
        "publish_mode": resolved_publish_mode,
        "downstream_mode": resolved_downstream_mode,
        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
        "canonical_run_id": str(canonical_run_id).strip(),
    }
    if raw_table_path is not None:
        config["raw_table_path"] = raw_table_path.resolve().as_posix()
    if raw_ingest_report_path is not None:
        config["raw_ingest_report_path"] = raw_ingest_report_path.resolve().as_posix()
    if canonical_target_output_dir is not None:
        config["canonical_target_output_dir"] = canonical_target_output_dir.resolve().as_posix()
    for key, value in explicit_target_paths.items():
        if value is not None:
            config[key] = value.resolve().as_posix()
    if canonical_session_intervals_path is not None:
        config["canonical_session_intervals_path"] = (
            canonical_session_intervals_path.resolve().as_posix()
        )
    optional_roots = {
        "raw_root": raw_root,
        "canonical_root": canonical_root,
        "session_root": session_root,
        "research_root": research_root,
    }
    for key, value in optional_roots.items():
        if value is not None:
            config[key] = value.resolve().as_posix()
    optional_text_values = {
        "dataset_version": dataset_version,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "split_method": split_method,
    }
    for key, value in optional_text_values.items():
        if value is not None:
            config[key] = str(value)
    if timeframes is not None:
        config["timeframes"] = (
            timeframes.strip()
            if isinstance(timeframes, str)
            else ",".join(str(item).strip() for item in timeframes if str(item).strip())
        )
    if warmup_bars is not None:
        config["warmup_bars"] = int(warmup_bars)
    optional_sequence_values = {
        "dataset_contract_ids": dataset_contract_ids,
        "dataset_instrument_ids": dataset_instrument_ids,
    }
    for key, values in optional_sequence_values.items():
        if values is not None:
            config[key] = ",".join(str(item).strip() for item in values if str(item).strip())
    if spark_master is not None:
        config["spark_master"] = str(spark_master)
    return config


def build_moex_data_rebuild_run_config(
    *,
    profile_name: str,
    canonical_run_id: str,
    canonical_output_dir: Path,
    canonical_target_output_dir: Path | None = None,
    raw_table_path: Path | None = None,
    raw_ingest_report_path: Path | None = None,
    canonical_bars_path: Path | None = None,
    canonical_provenance_path: Path | None = None,
    canonical_session_intervals_path: Path | None = None,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    source_mode: str | None = None,
    publish_mode: str = "promote",
    downstream_mode: str = "invalidate",
    raw_root: Path | None = None,
    canonical_root: Path | None = None,
    session_root: Path | None = None,
    research_root: Path | None = None,
    dataset_version: str | None = None,
    timeframes: str | Sequence[str] | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    warmup_bars: int | None = None,
    split_method: str | None = None,
    dataset_contract_ids: Sequence[str] | None = None,
    dataset_instrument_ids: Sequence[str] | None = None,
    spark_master: str | None = None,
) -> dict[str, object]:
    op_config = build_moex_data_rebuild_op_config(
        profile_name=profile_name,
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=canonical_output_dir,
        canonical_target_output_dir=canonical_target_output_dir,
        canonical_run_id=canonical_run_id,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
        canonical_session_calendar_path=canonical_session_calendar_path,
        canonical_roll_map_path=canonical_roll_map_path,
        source_mode=source_mode,
        publish_mode=publish_mode,
        downstream_mode=downstream_mode,
        raw_root=raw_root,
        canonical_root=canonical_root,
        session_root=session_root,
        research_root=research_root,
        dataset_version=dataset_version,
        timeframes=timeframes,
        start_ts=start_ts,
        end_ts=end_ts,
        warmup_bars=warmup_bars,
        split_method=split_method,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        spark_master=spark_master,
    )
    return {
        "ops": {
            MOEX_DATA_REBUILD_OP_NAME: {"config": dict(op_config)},
        }
    }


def build_moex_historical_run_config(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    canonical_output_dir: Path,
    canonical_run_id: str,
    canonical_session_intervals_path: Path | None = None,
) -> dict[str, object]:
    op_config = build_moex_historical_op_config(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=canonical_output_dir,
        canonical_run_id=canonical_run_id,
        canonical_session_intervals_path=canonical_session_intervals_path,
    )
    return {
        "ops": {
            "moex_raw_ingest": {"config": dict(op_config)},
            "moex_canonical_refresh": {"config": dict(op_config)},
        }
    }


def _merge_run_config(left: dict[str, object], right: dict[str, object]) -> dict[str, object]:
    merged: dict[str, object] = dict(left)
    for key, value in right.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _merge_run_config(
                {str(item_key): item_value for item_key, item_value in current.items()},
                {str(item_key): item_value for item_key, item_value in value.items()},
            )
        else:
            merged[key] = value
    return merged


def build_moex_historical_dagster_binding_artifact() -> dict[str, object]:
    return {
        "job": {
            "name": MOEX_BASELINE_UPDATE_JOB_NAME,
            "asset_keys": ["moex_baseline_update"],
        },
        "schedule": {
            "name": MOEX_BASELINE_DAILY_SCHEDULE_NAME,
            "cron": MOEX_BASELINE_DAILY_CRON,
            "execution_timezone": MOEX_HISTORICAL_EXECUTION_TIMEZONE,
            "default_status": DefaultScheduleStatus.RUNNING.value,
        },
        "retry_policy": {
            "max_retries": MOEX_BASELINE_UPDATE_RETRY_POLICY.max_retries,
            "delay": MOEX_BASELINE_UPDATE_RETRY_POLICY.delay,
        },
        "runtime_boundary": {
            "orchestrator": "dagster",
            "hot_table_runtime": MOEX_BASELINE_UPDATE_HOT_TABLE_RUNTIME,
            "python_role": "source_adapter_config_and_evidence",
        },
    }


def _run_python_raw_ingest_route(op_config: dict[str, object]) -> dict[str, object]:
    mapping_registry_path = Path(_text_value(op_config, "mapping_registry_path")).resolve()
    universe_path = Path(_text_value(op_config, "universe_path")).resolve()
    raw_ingest_root = resolve_external_root(
        _text_value(op_config, "raw_ingest_root"),
        repo_root=REPO_ROOT,
        field_name="raw_ingest_root",
        default_subdir=RAW_INGEST_STORAGE_DIRNAME,
    )
    canonicalization_root = resolve_external_root(
        _text_value(op_config, "canonicalization_root"),
        repo_root=REPO_ROOT,
        field_name="canonicalization_root",
        default_subdir=CANONICAL_REFRESH_STORAGE_DIRNAME,
    )
    route_refresh_root = resolve_external_root(
        _text_value(op_config, "route_refresh_root"),
        repo_root=REPO_ROOT,
        field_name="route_refresh_root",
        default_subdir=ROUTE_REFRESH_STORAGE_DIRNAME,
    )
    run_id = _text_value(op_config, "run_id", "canonical_run_id")
    timeframes = _text_value(op_config, "timeframes") or DEFAULT_TIMEFRAMES
    workers = _int_value(op_config, "workers", default=DEFAULT_WORKERS)
    batch_size = _int_value(op_config, "batch_size", default=DEFAULT_BATCH_SIZE)
    execution_mode = _text_value(op_config, "execution_mode") or DEFAULT_EXECUTION_MODE
    bootstrap_window_days = _int_value(
        op_config,
        "bootstrap_window_days",
        default=DEFAULT_BOOTSTRAP_WINDOW_DAYS,
    )
    stability_lag_minutes = _int_value(
        op_config,
        "stability_lag_minutes",
        default=DEFAULT_STABILITY_LAG_MINUTES,
    )
    expand_contract_chain = _bool_value(
        op_config,
        "expand_contract_chain",
        default=True,
    )
    contract_discovery_step_days = _int_value(
        op_config,
        "contract_discovery_step_days",
        default=DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    )
    contract_discovery_lookback_days = _int_value(
        op_config,
        "contract_discovery_lookback_days",
        default=DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS,
    )
    refresh_overlap_minutes = _int_value(
        op_config,
        "refresh_overlap_minutes",
        default=DEFAULT_REFRESH_OVERLAP_MINUTES,
    )
    ingest_till_utc = _text_value(op_config, "ingest_till_utc")

    if not mapping_registry_path.exists():
        raise RuntimeError(
            f"mapping registry path does not exist: {mapping_registry_path.as_posix()}"
        )
    if not universe_path.exists():
        raise RuntimeError(f"universe path does not exist: {universe_path.as_posix()}")
    if not run_id:
        raise RuntimeError("moex historical scheduled route requires non-empty `run_id`")
    if execution_mode not in {"sequential", "parallel"}:
        raise RuntimeError(
            "moex historical scheduled route requires `execution_mode` in {sequential, parallel}"
        )

    command = [
        sys.executable,
        (REPO_ROOT / "scripts" / "run_moex_route_refresh.py").as_posix(),
        "--allow-manual-route",
        "--stop-after-raw-ingest",
        "--mapping-registry",
        mapping_registry_path.as_posix(),
        "--universe",
        universe_path.as_posix(),
        "--raw-ingest-root",
        raw_ingest_root.as_posix(),
        "--canonical-root",
        canonicalization_root.as_posix(),
        "--route-root",
        route_refresh_root.as_posix(),
        "--run-id",
        run_id,
        "--timeframes",
        timeframes,
        "--workers",
        str(workers),
        "--batch-size",
        str(batch_size),
        "--execution-mode",
        execution_mode,
        "--bootstrap-window-days",
        str(bootstrap_window_days),
        "--stability-lag-minutes",
        str(stability_lag_minutes),
        "--contract-discovery-step-days",
        str(contract_discovery_step_days),
        "--contract-discovery-lookback-days",
        str(contract_discovery_lookback_days),
        "--refresh-overlap-minutes",
        str(refresh_overlap_minutes),
    ]
    command.append(
        "--expand-contract-chain" if expand_contract_chain else "--no-expand-contract-chain"
    )
    if ingest_till_utc:
        command.extend(["--ingest-till-utc", ingest_till_utc])

    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"python raw-ingest route failed: {detail}")

    route_report_path = route_refresh_root / run_id / ROUTE_REFRESH_REPORT_FILENAME
    if not route_report_path.exists():
        raise RuntimeError(
            "python raw-ingest route completed without route-refresh-report.json: "
            f"{route_report_path.as_posix()}"
        )
    route_report = _read_raw_ingest_report(route_report_path)
    raw_table_path = Path(str(route_report.get("raw_table_path", "")).strip()).resolve()
    raw_ingest_report_path = Path(
        str(route_report.get("raw_ingest_report_path", "")).strip()
    ).resolve()
    raw_ingest_root = Path(str(route_report.get("raw_ingest_root", "")).strip()).resolve()
    raw_ingest_run_report = _read_raw_ingest_report(raw_ingest_report_path)
    raw_status = str(raw_ingest_run_report.get("status", "")).strip()
    return {
        "route_mode": "scheduled-script",
        "run_id": run_id,
        "raw_ingest_root": raw_ingest_root.as_posix(),
        "canonicalization_root": (canonicalization_root / run_id).resolve().as_posix(),
        "route_refresh_root": (route_refresh_root / run_id).resolve().as_posix(),
        "raw_table_path": raw_table_path.as_posix(),
        "raw_ingest_report_path": raw_ingest_report_path.as_posix(),
        "raw_ingest_run_report": raw_ingest_run_report,
        "raw_status": raw_status,
        "raw_ingest_route_report_path": route_report_path.as_posix(),
    }


@asset(
    group_name="moex_baseline_update",
    config_schema=MOEX_BASELINE_UPDATE_OP_CONFIG_SCHEMA,
    retry_policy=MOEX_BASELINE_UPDATE_RETRY_POLICY,
)
def moex_baseline_update(context) -> dict[str, object]:
    op_config = _op_config_from_context(context)
    artifact_root = _route_artifact_root()
    default_paths = _baseline_paths_from_root(artifact_root)

    mapping_registry_path = Path(
        _text_value(op_config, "mapping_registry_path") or DEFAULT_MAPPING_REGISTRY.as_posix()
    ).resolve()
    universe_path = Path(
        _text_value(op_config, "universe_path") or DEFAULT_UNIVERSE.as_posix()
    ).resolve()
    raw_table_path = Path(
        _text_value(op_config, "raw_table_path") or default_paths["raw_table_path"].as_posix()
    ).resolve()
    canonical_bars_path = Path(
        _text_value(op_config, "canonical_bars_path")
        or default_paths["canonical_bars_path"].as_posix()
    ).resolve()
    canonical_provenance_path = Path(
        _text_value(op_config, "canonical_provenance_path")
        or default_paths["canonical_provenance_path"].as_posix()
    ).resolve()
    canonical_session_intervals_text = _text_value(op_config, "canonical_session_intervals_path")
    canonical_session_intervals_path = (
        Path(canonical_session_intervals_text).resolve()
        if canonical_session_intervals_text
        else None
    )
    canonical_session_calendar_path = Path(
        _text_value(op_config, "canonical_session_calendar_path")
        or default_paths["canonical_session_calendar_path"].as_posix()
    ).resolve()
    canonical_roll_map_path = Path(
        _text_value(op_config, "canonical_roll_map_path")
        or default_paths["canonical_roll_map_path"].as_posix()
    ).resolve()
    raw_economics_root = Path(
        _text_value(op_config, "raw_economics_root")
        or default_paths["raw_economics_root"].as_posix()
    ).resolve()
    canonical_economics_root = Path(
        _text_value(op_config, "canonical_economics_root")
        or default_paths["canonical_economics_root"].as_posix()
    ).resolve()
    evidence_root = Path(
        _text_value(op_config, "evidence_root") or default_paths["evidence_root"].as_posix()
    ).resolve()
    run_id = validate_moex_runtime_run_id(
        _text_value(op_config, "run_id", "canonical_run_id") or _default_route_run_id(),
        name="moex_baseline_update.run_id",
    )
    ingest_till_utc = _text_value(op_config, "ingest_till_utc") or datetime.now(tz=UTC).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "Z")

    report = run_moex_baseline_update(
        mapping_registry_path=mapping_registry_path,
        universe_path=universe_path,
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
        canonical_session_calendar_path=canonical_session_calendar_path,
        canonical_roll_map_path=canonical_roll_map_path,
        evidence_dir=evidence_root,
        run_id=run_id,
        timeframes=_split_timeframes(_text_value(op_config, "timeframes") or DEFAULT_TIMEFRAMES),
        ingest_till_utc=ingest_till_utc,
        refresh_window_days=_int_value(
            op_config,
            "refresh_window_days",
            default=DEFAULT_DAILY_REFRESH_WINDOW_DAYS,
        ),
        contract_discovery_lookback_days=_int_value(
            op_config,
            "contract_discovery_lookback_days",
            default=DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS,
        ),
        contract_discovery_step_days=_int_value(
            op_config,
            "contract_discovery_step_days",
            default=DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
        ),
        refresh_overlap_minutes=_int_value(
            op_config,
            "refresh_overlap_minutes",
            default=DEFAULT_REFRESH_OVERLAP_MINUTES,
        ),
        cf_catch_up_timeframes=_split_timeframes(
            _text_value(op_config, "cf_catch_up_timeframes") or ""
        )
        or None,
        cf_catch_up_overlap_minutes=(
            _int_value(
                op_config,
                "cf_catch_up_overlap_minutes",
                default=DEFAULT_REFRESH_OVERLAP_MINUTES,
            )
            if "cf_catch_up_overlap_minutes" in op_config
            else None
        ),
        max_changed_window_days=_int_value(
            op_config,
            "max_changed_window_days",
            default=DEFAULT_MAX_CHANGED_WINDOW_DAYS,
        ),
        stability_lag_minutes=_int_value(
            op_config,
            "stability_lag_minutes",
            default=DEFAULT_STABILITY_LAG_MINUTES,
        ),
        expand_contract_chain=_bool_value(op_config, "expand_contract_chain", default=True),
        coverage_mode=_text_value(op_config, "coverage_mode") or "local_tail",
        economics_mode=_text_value(op_config, "economics_mode") or ECONOMICS_REFRESH_MODE_REFRESH,
        raw_economics_root=raw_economics_root,
        canonical_economics_root=canonical_economics_root,
        repo_root=REPO_ROOT,
    )
    if str(report.get("publish_decision", "")).strip() != "publish":
        raise RuntimeError("moex baseline update did not publish")
    context.add_output_metadata(
        {
            "mode": "baseline_update",
            "coverage_mode": str(report.get("coverage_mode", "")),
            "orchestrator": "dagster_asset",
            "hot_table_runtime": MOEX_BASELINE_UPDATE_HOT_TABLE_RUNTIME,
            "source_rows": int(report.get("source_rows", 0) or 0),
            "incremental_rows": int(report.get("incremental_rows", 0) or 0),
            "current_changed_windows": int(report.get("current_changed_windows", 0) or 0),
            "effective_changed_windows": int(report.get("effective_changed_windows", 0) or 0),
            "scoped_canonical_rows": int(
                dict(report.get("canonical_report", {}) or {}).get("scoped_canonical_rows", 0) or 0
            ),
            "canonical_rows": int(
                dict(report.get("canonical_report", {}) or {}).get("canonical_rows", 0) or 0
            ),
            "raw_table_path": str(report.get("raw_table_path", "")),
            "canonical_bars_path": str(report.get("canonical_bars_path", "")),
            "economics_mode": str(report.get("economics_mode", "")),
            "canonical_economics_root": str(report.get("canonical_economics_root", "")),
            "canonical_session_intervals_path": str(
                report.get("canonical_session_intervals_path", "")
            ),
            "canonical_session_calendar_path": str(
                report.get("canonical_session_calendar_path", "")
            ),
            "canonical_roll_map_path": str(report.get("canonical_roll_map_path", "")),
        }
    )
    return report


@asset(
    group_name="moex_historical_cutover",
    config_schema=MOEX_HISTORICAL_OP_CONFIG_SCHEMA,
    retry_policy=MOEX_HISTORICAL_RETRY_POLICY,
)
def moex_raw_ingest(context) -> dict[str, object]:
    op_config = _op_config_from_context(context)
    raw_table_text = _text_value(op_config, "raw_table_path")
    raw_report_text = _text_value(op_config, "raw_ingest_report_path")

    if raw_table_text and raw_report_text:
        raw_table_path = Path(raw_table_text).resolve()
        raw_ingest_report_path = Path(raw_report_text).resolve()
        if not has_delta_log(raw_table_path):
            raise RuntimeError(f"raw table is missing `_delta_log`: {raw_table_path.as_posix()}")
        if not raw_ingest_report_path.exists():
            raise RuntimeError(
                f"raw ingest report path does not exist: {raw_ingest_report_path.as_posix()}"
            )

        raw_ingest_run_report = _read_raw_ingest_report(raw_ingest_report_path)
        raw_status = str(raw_ingest_run_report.get("status", "")).strip()
        return {
            "route_mode": "explicit-artifacts",
            "raw_table_path": raw_table_path.as_posix(),
            "raw_ingest_report_path": raw_ingest_report_path.as_posix(),
            "raw_ingest_run_report": raw_ingest_run_report,
            "raw_status": raw_status,
        }

    return _run_python_raw_ingest_route(op_config)


@asset(
    group_name="moex_historical_cutover",
    config_schema=MOEX_HISTORICAL_OP_CONFIG_SCHEMA,
    retry_policy=MOEX_HISTORICAL_RETRY_POLICY,
)
def moex_canonical_refresh(context, moex_raw_ingest: dict[str, object]) -> dict[str, object]:
    op_config = _op_config_from_context(context)
    raw_report = moex_raw_ingest.get("raw_ingest_run_report")
    if not isinstance(raw_report, dict):
        raise RuntimeError("moex_raw_ingest output is missing `raw_ingest_run_report` payload")

    raw_status = str(raw_report.get("status", "")).strip()
    if raw_status not in PASS_LIKE_RAW_STATUSES:
        raise RuntimeError(
            "canonical refresh cannot start because raw-ingest status is not PASS/PASS-NOOP; "
            f"got `{raw_status or 'EMPTY'}`"
        )

    raw_table_path = Path(str(moex_raw_ingest.get("raw_table_path", ""))).resolve()
    output_dir_text = _text_value(
        op_config,
        "canonical_output_dir",
    )
    if not output_dir_text:
        output_dir_text = str(moex_raw_ingest.get("canonicalization_root", "")).strip()
    run_id = _text_value(op_config, "canonical_run_id")
    if not run_id:
        run_id = str(moex_raw_ingest.get("run_id", "")).strip()
    if not output_dir_text:
        raise RuntimeError("moex canonical refresh step requires non-empty `canonical_output_dir`")
    if not run_id:
        raise RuntimeError("moex canonical refresh step requires non-empty `canonical_run_id`")

    output_dir = Path(output_dir_text).resolve()
    session_intervals_text = _text_value(op_config, "canonical_session_intervals_path")
    session_intervals_path = (
        Path(session_intervals_text).resolve() if session_intervals_text else None
    )
    canonical_bars_text = _text_value(op_config, "canonical_bars_path")
    canonical_provenance_text = _text_value(op_config, "canonical_provenance_path")
    canonical_session_calendar_text = _text_value(op_config, "canonical_session_calendar_path")
    canonical_roll_map_text = _text_value(op_config, "canonical_roll_map_path")

    report = run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id=run_id,
        raw_ingest_run_report=raw_report,
        canonical_bars_path=Path(canonical_bars_text).resolve() if canonical_bars_text else None,
        canonical_provenance_path=Path(canonical_provenance_text).resolve()
        if canonical_provenance_text
        else None,
        canonical_session_intervals_path=session_intervals_path,
        canonical_session_calendar_path=Path(canonical_session_calendar_text).resolve()
        if canonical_session_calendar_text
        else None,
        canonical_roll_map_path=Path(canonical_roll_map_text).resolve()
        if canonical_roll_map_text
        else None,
        canonical_merge_strategy=CANONICAL_MERGE_SCOPED_DELETE_INSERT,
    )
    publish_decision = str(report.get("publish_decision", "")).strip().lower()
    if publish_decision != "publish":
        raise RuntimeError(
            "canonical refresh step finished in blocked state and cannot represent the canonical route: "
            f"publish_decision={publish_decision or 'missing'}"
        )
    context.add_output_metadata(
        {
            "source_rows": int(report.get("source_rows", 0) or 0),
            "scoped_source_rows": int(report.get("scoped_source_rows", 0) or 0),
            "changed_windows_count": int(report.get("changed_windows_count", 0) or 0),
            "scoped_canonical_rows": int(report.get("scoped_canonical_rows", 0) or 0),
            "canonical_rows": int(report.get("canonical_rows", 0) or 0),
            "mutation_applied": bool(report.get("mutation_applied", False)),
        }
    )
    return report


def _optional_path_value(op_config: dict[str, object], *keys: str) -> Path | None:
    text = _text_value(op_config, *keys)
    return Path(text).resolve() if text else None


def _required_path_value(op_config: dict[str, object], key: str, *, context_name: str) -> Path:
    path = _optional_path_value(op_config, key)
    if path is None:
        raise RuntimeError(f"{context_name} requires non-empty `{key}`")
    return path


def _csv_sequence_value(
    raw: object,
    *,
    default: str | Sequence[str] = "",
    allow_empty: bool = False,
) -> tuple[str, ...]:
    source = raw if raw not in (None, "") else default
    if isinstance(source, str):
        values = tuple(item.strip() for item in source.split(",") if item.strip())
    else:
        values = tuple(str(item).strip() for item in source if str(item).strip())
    if not values and not allow_empty:
        raise RuntimeError("moex data rebuild requires at least one timeframe")
    return values


def _data_rebuild_input_roots(op_config: dict[str, object]) -> dict[str, Path]:
    roots: dict[str, Path] = {}
    for key in (
        "raw_root",
        "canonical_root",
        "session_root",
        "research_root",
        "raw_table_path",
        "canonical_output_dir",
        "canonical_target_output_dir",
        "canonical_bars_path",
        "canonical_provenance_path",
        "canonical_session_intervals_path",
        "canonical_session_calendar_path",
        "canonical_roll_map_path",
        "research_output_dir",
        "materialized_output_dir",
        "results_output_dir",
    ):
        path = _optional_path_value(op_config, key)
        if path is not None:
            roots[key] = path
    return roots


def _default_data_rebuild_manifest_path(op_config: dict[str, object], run_id: str) -> Path:
    root = _optional_path_value(
        op_config,
        "research_output_dir",
        "research_root",
        "canonical_output_dir",
        "canonical_root",
        "raw_root",
    )
    if root is None:
        root = _route_artifact_root()
    return root / "data-rebuild-manifests" / run_id / "moex-data-rebuild-manifest.json"


def _canonical_rebuild_staging_paths(staging_output_dir: Path) -> dict[str, Path]:
    output_paths = moex_historical_output_paths(staging_output_dir)
    return {
        "canonical_bars": Path(output_paths["canonical_bars"]).resolve(),
        "canonical_bar_provenance": Path(output_paths["canonical_bar_provenance"]).resolve(),
        "canonical_session_intervals": Path(output_paths["canonical_session_intervals"]).resolve(),
        "canonical_session_calendar": Path(output_paths["canonical_session_calendar"]).resolve(),
        "canonical_roll_map": Path(output_paths["canonical_roll_map"]).resolve(),
    }


def _canonical_rebuild_target_paths(op_config: dict[str, object]) -> dict[str, Path] | None:
    target_output_dir = _optional_path_value(op_config, "canonical_target_output_dir")
    explicit_paths = {
        "canonical_bars": _optional_path_value(op_config, "canonical_bars_path"),
        "canonical_bar_provenance": _optional_path_value(op_config, "canonical_provenance_path"),
        "canonical_session_intervals": _optional_path_value(
            op_config, "canonical_session_intervals_path"
        ),
        "canonical_session_calendar": _optional_path_value(
            op_config, "canonical_session_calendar_path"
        ),
        "canonical_roll_map": _optional_path_value(op_config, "canonical_roll_map_path"),
    }
    supplied_explicit_paths = {key: value for key, value in explicit_paths.items() if value}
    if supplied_explicit_paths:
        missing = sorted(key for key, value in explicit_paths.items() if value is None)
        if missing:
            raise RuntimeError(
                "canonical rebuild explicit target paths must be supplied as a complete set; "
                f"missing: {', '.join(missing)}"
            )
        return {key: value for key, value in explicit_paths.items() if value is not None}
    if target_output_dir is None:
        return None
    return {
        "canonical_bars": target_output_dir / CANONICAL_BASELINE_BARS_FILENAME,
        "canonical_bar_provenance": target_output_dir / CANONICAL_BASELINE_PROVENANCE_FILENAME,
        "canonical_session_intervals": target_output_dir
        / CANONICAL_BASELINE_SESSION_INTERVALS_FILENAME,
        "canonical_session_calendar": target_output_dir
        / CANONICAL_BASELINE_SESSION_CALENDAR_FILENAME,
        "canonical_roll_map": target_output_dir / CANONICAL_BASELINE_ROLL_MAP_FILENAME,
    }


def _canonical_publish_paths(
    *, op_config: dict[str, object], publish_mode: str, staging_output_dir: Path
) -> tuple[dict[str, Path], dict[str, Path]]:
    staging_paths = _canonical_rebuild_staging_paths(staging_output_dir)
    if publish_mode == "staging_only":
        return staging_paths, staging_paths
    target_paths = _canonical_rebuild_target_paths(op_config)
    if target_paths is None:
        raise RuntimeError(
            "canonical rebuild publish_mode=promote requires canonical_target_output_dir "
            "or explicit target paths for canonical bars, provenance, session intervals, "
            "session calendar, and roll map"
        )
    return staging_paths, target_paths


def _require_subreport_success(stage_name: str, report: Mapping[str, object]) -> None:
    if not bool(report.get("success", False)):
        reason = str(report.get("failure_reason", "")).strip() or "unknown failure"
        raise RuntimeError(f"moex data rebuild stage `{stage_name}` failed: {reason}")


def _collect_manifest_outputs(
    reports_by_stage: Mapping[str, Mapping[str, object]], *, publish_mode: str
) -> tuple[dict[str, str], dict[str, str], dict[str, int]]:
    staged_output_paths: dict[str, str] = {}
    promoted_output_paths: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    for report in reports_by_stage.values():
        raw_staged_output_paths = report.get("staged_output_paths", {})
        if isinstance(raw_staged_output_paths, Mapping):
            for key, value in raw_staged_output_paths.items():
                staged_output_paths[str(key)] = str(value)
        raw_promoted_output_paths = report.get("promoted_output_paths", {})
        if isinstance(raw_promoted_output_paths, Mapping):
            for key, value in raw_promoted_output_paths.items():
                promoted_output_paths[str(key)] = str(value)
        if not raw_staged_output_paths and not raw_promoted_output_paths:
            raw_output_paths = report.get("output_paths", {})
            if isinstance(raw_output_paths, Mapping):
                target = (
                    staged_output_paths if publish_mode == "staging_only" else promoted_output_paths
                )
                for key, value in raw_output_paths.items():
                    target[str(key)] = str(value)
        for row_count_key in ("rows_by_table", "total_rows_by_table"):
            raw_row_counts = report.get(row_count_key, {})
            if isinstance(raw_row_counts, Mapping):
                for key, value in raw_row_counts.items():
                    row_counts[str(key)] = int(value)
    if publish_mode == "staging_only":
        return staged_output_paths, {}, row_counts
    return staged_output_paths, promoted_output_paths, row_counts


def _run_existing_raw_canonical_rebuild(
    *, op_config: dict[str, object], run_id: str, publish_mode: str
) -> dict[str, object]:
    staging_output_dir = _required_path_value(
        op_config,
        "canonical_output_dir",
        context_name="canonical_from_existing_raw rebuild",
    )
    staging_paths, publish_paths = _canonical_publish_paths(
        op_config=op_config,
        publish_mode=publish_mode,
        staging_output_dir=staging_output_dir,
    )
    report = materialize_moex_historical_assets(
        raw_table_path=_required_path_value(
            op_config,
            "raw_table_path",
            context_name="canonical_from_existing_raw rebuild",
        ),
        raw_ingest_report_path=_required_path_value(
            op_config,
            "raw_ingest_report_path",
            context_name="canonical_from_existing_raw rebuild",
        ),
        canonical_output_dir=staging_output_dir,
        canonical_run_id=run_id,
        canonical_bars_path=publish_paths["canonical_bars"],
        canonical_provenance_path=publish_paths["canonical_bar_provenance"],
        canonical_session_intervals_path=publish_paths["canonical_session_intervals"],
        canonical_session_calendar_path=publish_paths["canonical_session_calendar"],
        canonical_roll_map_path=publish_paths["canonical_roll_map"],
        selection=MOEX_HISTORICAL_ASSET_KEYS,
        raise_on_error=True,
    )
    report["staged_output_paths"] = {key: value.as_posix() for key, value in staging_paths.items()}
    report["promoted_output_paths"] = (
        {key: value.as_posix() for key, value in publish_paths.items()}
        if publish_mode == "promote"
        else {}
    )
    return report


def _run_full_raw_to_canonical_rebuild(
    *, op_config: dict[str, object], run_id: str, publish_mode: str
) -> dict[str, object]:
    canonical_output_dir = _required_path_value(
        op_config,
        "canonical_output_dir",
        context_name="full_raw_to_canonical rebuild",
    )
    staging_paths, publish_paths = _canonical_publish_paths(
        op_config=op_config,
        publish_mode=publish_mode,
        staging_output_dir=canonical_output_dir,
    )
    asset_op_config = dict(op_config)
    asset_op_config["run_id"] = run_id
    asset_op_config["canonical_run_id"] = run_id
    asset_op_config["canonical_bars_path"] = publish_paths["canonical_bars"].as_posix()
    asset_op_config["canonical_provenance_path"] = publish_paths[
        "canonical_bar_provenance"
    ].as_posix()
    asset_op_config["canonical_session_intervals_path"] = publish_paths[
        "canonical_session_intervals"
    ].as_posix()
    asset_op_config["canonical_session_calendar_path"] = publish_paths[
        "canonical_session_calendar"
    ].as_posix()
    asset_op_config["canonical_roll_map_path"] = publish_paths["canonical_roll_map"].as_posix()
    result = materialize(
        assets=list(MOEX_HISTORICAL_ASSETS),
        selection=list(MOEX_HISTORICAL_ASSET_KEYS),
        run_config={
            "ops": {
                "moex_raw_ingest": {"config": asset_op_config},
                "moex_canonical_refresh": {"config": asset_op_config},
            }
        },
        raise_on_error=True,
    )
    output_paths = moex_historical_output_paths(canonical_output_dir)
    canonical_report_path = Path(output_paths["canonical_report"])
    if result.success and not canonical_report_path.exists():
        raise RuntimeError(
            "full_raw_to_canonical rebuild reported success but canonical report artifact is missing: "
            f"{canonical_report_path.as_posix()}"
        )
    published_output_paths = {
        key: value.as_posix()
        for key, value in (publish_paths if publish_mode == "promote" else staging_paths).items()
    }
    return {
        "success": bool(result.success),
        "selected_assets": list(MOEX_HISTORICAL_ASSET_KEYS),
        "materialized_assets": list(MOEX_HISTORICAL_ASSET_KEYS),
        "output_paths": published_output_paths,
        "staged_output_paths": {key: value.as_posix() for key, value in staging_paths.items()},
        "promoted_output_paths": (
            {key: value.as_posix() for key, value in publish_paths.items()}
            if publish_mode == "promote"
            else {}
        ),
        "canonical_report_exists": canonical_report_path.exists(),
    }


def _research_rebuild_kwargs(op_config: dict[str, object], *, run_id: str) -> dict[str, object]:
    research_output_dir = _optional_path_value(op_config, "research_output_dir", "research_root")
    materialized_output_dir = _optional_path_value(op_config, "materialized_output_dir")
    results_output_dir = _optional_path_value(op_config, "results_output_dir")
    volume_profile_raw_1m_table_path = _optional_path_value(
        op_config, "volume_profile_raw_1m_table_path"
    )
    continuous_front_policy = op_config.get("continuous_front_policy")
    volume_profile_tick_sizes = op_config.get("volume_profile_tick_size_by_instrument")
    return {
        "canonical_output_dir": _required_path_value(
            op_config,
            "canonical_output_dir",
            context_name="MOEX research data-layer rebuild",
        ),
        "research_output_dir": research_output_dir,
        "materialized_output_dir": materialized_output_dir,
        "results_output_dir": results_output_dir,
        "campaign_run_id": run_id,
        "dataset_version": _text_value(op_config, "dataset_version") or run_id,
        "dataset_name": _text_value(op_config, "dataset_name") or "moex-data-layer-rebuild",
        "universe_id": _text_value(op_config, "universe_id") or "moex-futures",
        "timeframes": _csv_sequence_value(
            _text_value(op_config, "timeframes"), default=DEFAULT_TIMEFRAMES
        ),
        "base_timeframe": _text_value(op_config, "base_timeframe"),
        "start_ts": _text_value(op_config, "start_ts"),
        "end_ts": _text_value(op_config, "end_ts"),
        "warmup_bars": _int_value(
            op_config, "warmup_bars", default=DEFAULT_RESEARCH_REBUILD_WARMUP_BARS
        ),
        "split_method": _text_value(op_config, "split_method") or "holdout",
        "series_mode": _text_value(op_config, "series_mode") or "continuous_front",
        "continuous_front_policy": (
            dict(continuous_front_policy) if isinstance(continuous_front_policy, dict) else None
        ),
        "dataset_contract_ids": _csv_sequence_value(
            op_config.get("dataset_contract_ids"), default=(), allow_empty=True
        ),
        "dataset_instrument_ids": _csv_sequence_value(
            op_config.get("dataset_instrument_ids"), default=(), allow_empty=True
        ),
        "indicator_set_version": _text_value(op_config, "indicator_set_version") or "indicators-v1",
        "indicator_profile_version": _text_value(op_config, "indicator_profile_version")
        or "core_v1",
        "derived_indicator_set_version": _text_value(op_config, "derived_indicator_set_version")
        or "derived-v1",
        "derived_indicator_profile_version": _text_value(
            op_config, "derived_indicator_profile_version"
        )
        or "core_v1",
        "volume_profile_raw_1m_table_path": volume_profile_raw_1m_table_path,
        "volume_profile_tick_size_by_instrument": (
            dict(volume_profile_tick_sizes) if isinstance(volume_profile_tick_sizes, dict) else None
        ),
        "reuse_existing_materialization": _bool_value(
            op_config, "reuse_existing_materialization", default=False
        ),
        "spark_master": _text_value(op_config, "spark_master"),
        "raise_on_error": True,
    }


def _run_research_rebuild_stages(
    *, stage_names: Sequence[str], op_config: dict[str, object], run_id: str
) -> dict[str, dict[str, object]]:
    import trading_advisor_3000.dagster_defs.research_assets as research_assets

    runner_by_stage = {
        "continuous_front": research_assets.materialize_moex_cf_rebuild_assets,
        "research_bar": research_assets.materialize_moex_research_bar_rebuild_assets,
        "indicator": research_assets.materialize_moex_indicator_rebuild_assets,
        "derived": research_assets.materialize_moex_derived_indicator_rebuild_assets,
        "indicator_sidecar": research_assets.materialize_moex_indicator_sidecar_assets,
    }
    kwargs = _research_rebuild_kwargs(op_config, run_id=run_id)
    reports: dict[str, dict[str, object]] = {}
    for stage_name in stage_names:
        runner = runner_by_stage.get(stage_name)
        if runner is None:
            continue
        stage_kwargs = dict(kwargs)
        if stage_name in {"indicator", "derived"}:
            for key in ("start_ts", "end_ts", "warmup_bars", "split_method"):
                stage_kwargs.pop(key, None)
        report = dict(runner(**stage_kwargs))
        _require_subreport_success(stage_name, report)
        reports[stage_name] = report
    return reports


def run_moex_data_rebuild_profile(op_config: dict[str, object]) -> dict[str, object]:
    profile = resolve_moex_data_rebuild_profile(
        _text_value(op_config, "profile_name") or "canonical_from_existing_raw"
    )
    run_id = validate_moex_runtime_run_id(
        _text_value(op_config, "canonical_run_id", "run_id") or _default_route_run_id(),
        name="moex_data_rebuild.run_id",
    )
    publish_mode = _text_value(op_config, "publish_mode") or "promote"
    downstream_mode = _text_value(op_config, "downstream_mode") or "invalidate"
    reports_by_stage: dict[str, dict[str, object]] = {}

    if profile.name == "full_raw_to_canonical":
        reports_by_stage["canonical"] = _run_full_raw_to_canonical_rebuild(
            op_config=op_config, run_id=run_id, publish_mode=publish_mode
        )
    elif set(profile.stage_names).intersection({"sessions", "canonical"}):
        reports_by_stage["canonical"] = _run_existing_raw_canonical_rebuild(
            op_config=op_config, run_id=run_id, publish_mode=publish_mode
        )
    elif profile.stage_names:
        reports_by_stage.update(
            _run_research_rebuild_stages(
                stage_names=profile.stage_names,
                op_config=op_config,
                run_id=run_id,
            )
        )

    for stage_name, report in reports_by_stage.items():
        _require_subreport_success(stage_name, report)

    staged_outputs, promoted_outputs, row_counts = _collect_manifest_outputs(
        reports_by_stage, publish_mode=publish_mode
    )
    manifest = build_moex_data_rebuild_manifest(
        profile=profile,
        run_id=run_id,
        publish_mode=publish_mode,
        downstream_mode=downstream_mode,
        input_roots=_data_rebuild_input_roots(op_config),
        staged_outputs=staged_outputs,
        promoted_outputs=promoted_outputs,
        row_counts=row_counts,
    )
    manifest_path = _optional_path_value(op_config, "manifest_path")
    if manifest_path is None:
        manifest_path = _default_data_rebuild_manifest_path(op_config, run_id)
    written_manifest_path = write_moex_data_rebuild_manifest(manifest_path, manifest)
    return {
        "success": True,
        "profile_name": profile.name,
        "run_id": run_id,
        "source_mode": _text_value(op_config, "source_mode") or profile.source_mode,
        "publish_mode": publish_mode,
        "downstream_mode": downstream_mode,
        "stage_names": list(profile.stage_names),
        "layer_reports": reports_by_stage,
        "manifest": manifest,
        "manifest_path": written_manifest_path.as_posix(),
        "materialized_assets": sorted(
            {
                str(asset_name)
                for report in reports_by_stage.values()
                for asset_name in report.get("materialized_assets", [])
            }
        ),
    }


@op(
    name=MOEX_DATA_REBUILD_OP_NAME,
    config_schema=MOEX_HISTORICAL_OP_CONFIG_SCHEMA,
    retry_policy=MOEX_HISTORICAL_RETRY_POLICY,
)
def moex_data_rebuild(context) -> dict[str, object]:
    report = run_moex_data_rebuild_profile(_op_config_from_context(context))
    context.add_output_metadata(
        {
            "profile_name": str(report.get("profile_name", "")),
            "run_id": str(report.get("run_id", "")),
            "stage_count": len(list(report.get("stage_names", []))),
            "manifest_path": str(report.get("manifest_path", "")),
        }
    )
    return report


MOEX_HISTORICAL_ASSETS = (
    moex_baseline_update,
    moex_raw_ingest,
    moex_canonical_refresh,
)


moex_baseline_update_job = define_asset_job(
    name=MOEX_BASELINE_UPDATE_JOB_NAME,
    selection=AssetSelection.groups("moex_baseline_update"),
    op_retry_policy=MOEX_BASELINE_UPDATE_RETRY_POLICY,
)


@job(name=MOEX_DATA_REBUILD_JOB_NAME)
def moex_data_rebuild_job():
    moex_data_rebuild()


@job(name=MOEX_HISTORICAL_CUTOVER_JOB_NAME)
def moex_historical_cutover_job():
    moex_data_rebuild()


moex_data_rebuild_preview_schedule = ScheduleDefinition(
    name="moex_data_rebuild_preview_schedule",
    job=moex_data_rebuild_job,
    cron_schedule=MOEX_BASELINE_DAILY_CRON,
    run_config_fn=_build_nightly_schedule_run_config,
    execution_timezone=MOEX_HISTORICAL_EXECUTION_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Manual proof-only schedule definition for data rebuild tests; not registered as an active nightly schedule.",
)
moex_historical_cutover_preview_schedule = ScheduleDefinition(
    name="moex_historical_cutover_preview_schedule",
    job=moex_historical_cutover_job,
    cron_schedule=MOEX_BASELINE_DAILY_CRON,
    run_config_fn=_build_nightly_schedule_run_config,
    execution_timezone=MOEX_HISTORICAL_EXECUTION_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Manual proof-only schedule definition for historical cutover tests; not registered as an active nightly schedule.",
)

moex_baseline_daily_update_schedule = ScheduleDefinition(
    name=MOEX_BASELINE_DAILY_SCHEDULE_NAME,
    job=moex_baseline_update_job,
    cron_schedule=MOEX_BASELINE_DAILY_CRON,
    run_config_fn=_build_baseline_daily_schedule_run_config,
    execution_timezone=MOEX_HISTORICAL_EXECUTION_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Daily Dagster schedule for the MOEX baseline updater. "
        "It appends/merges fresh raw MOEX data into the stable baseline and Spark-refreshes only changed canonical windows."
    ),
)


moex_historical_definitions = Definitions(
    assets=list(MOEX_HISTORICAL_ASSETS),
    schedules=[
        moex_baseline_daily_update_schedule,
        moex_historical_cutover_preview_schedule,
    ],
    jobs=[
        moex_baseline_update_job,
        moex_data_rebuild_job,
        moex_historical_cutover_job,
    ],
)


def assert_moex_historical_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or moex_historical_definitions
    try:
        repository = defs.get_repository_def()
        repository.get_job(MOEX_BASELINE_UPDATE_JOB_NAME)
        job = repository.get_job(MOEX_DATA_REBUILD_JOB_NAME)
    except Exception as exc:
        raise RuntimeError(
            "moex historical Dagster definitions are metadata-only or incomplete: "
            "missing executable baseline update or data rebuild job."
        ) from exc

    expected_nodes = {MOEX_DATA_REBUILD_OP_NAME}
    actual_nodes = set(job.graph.node_dict.keys())
    missing_nodes = sorted(expected_nodes - actual_nodes)
    if missing_nodes:
        missing_text = ", ".join(missing_nodes)
        raise RuntimeError(
            "moex historical Dagster definitions are metadata-only or incomplete: "
            f"missing executable data rebuild node: {missing_text}"
        )


def build_moex_historical_definitions() -> Definitions:
    assert_moex_historical_definitions_executable(moex_historical_definitions)
    return moex_historical_definitions


def moex_historical_output_paths(output_dir: Path) -> dict[str, str]:
    resolved = output_dir.resolve()
    return {
        "canonical_report": (resolved / CANONICAL_REFRESH_REPORT_FILENAME).as_posix(),
        "canonical_bars": (resolved / "delta" / "canonical_bars.delta").as_posix(),
        "canonical_bar_provenance": (
            resolved / "delta" / "canonical_bar_provenance.delta"
        ).as_posix(),
        "canonical_session_intervals": (
            resolved / "delta" / "canonical_session_intervals.delta"
        ).as_posix(),
        "canonical_session_calendar": (
            resolved / "delta" / "canonical_session_calendar.delta"
        ).as_posix(),
        "canonical_roll_map": (resolved / "delta" / "canonical_roll_map.delta").as_posix(),
    }


def moex_baseline_update_output_paths(*, baseline_root: Path, run_id: str) -> dict[str, str]:
    resolved_run_id = validate_moex_runtime_run_id(
        run_id, name="moex_baseline_update_output_paths.run_id"
    )
    paths = _baseline_paths_from_root(baseline_root.resolve())
    return {
        "raw_table": paths["raw_table_path"].as_posix(),
        "canonical_bars": paths["canonical_bars_path"].as_posix(),
        "canonical_bar_provenance": paths["canonical_provenance_path"].as_posix(),
        "canonical_session_calendar": paths["canonical_session_calendar_path"].as_posix(),
        "canonical_roll_map": paths["canonical_roll_map_path"].as_posix(),
        "evidence_root": paths["evidence_root"].as_posix(),
        "baseline_update_report": (
            paths["evidence_root"] / resolved_run_id / BASELINE_UPDATE_REPORT_FILENAME
        ).as_posix(),
    }


def execute_moex_baseline_update_job(
    *,
    baseline_root: Path,
    instance: DagsterInstance,
    run_id: str,
    ingest_till_utc: str | None = None,
    staging_profile: str = "verification",
    timeframes: str | Sequence[str] = DEFAULT_TIMEFRAMES,
    refresh_window_days: int = DEFAULT_DAILY_REFRESH_WINDOW_DAYS,
    contract_discovery_lookback_days: int = DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS,
    contract_discovery_step_days: int = DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    refresh_overlap_minutes: int = DEFAULT_REFRESH_OVERLAP_MINUTES,
    max_changed_window_days: int = DEFAULT_MAX_CHANGED_WINDOW_DAYS,
    stability_lag_minutes: int = DEFAULT_STABILITY_LAG_MINUTES,
    expand_contract_chain: bool = True,
    coverage_mode: str = "local_tail",
    extra_tags: dict[str, str] | None = None,
    raise_on_error: bool = False,
) -> dict[str, object]:
    resolved_run_id = validate_moex_runtime_run_id(
        run_id, name="execute_moex_baseline_update_job.run_id"
    )

    assert_moex_historical_definitions_executable()
    definitions = build_moex_historical_definitions()
    repository = definitions.get_repository_def()
    job = repository.get_job(MOEX_BASELINE_UPDATE_JOB_NAME)
    resolved_baseline_root = baseline_root.resolve()
    run_config = build_moex_baseline_update_run_config(
        baseline_root=resolved_baseline_root,
        run_id=resolved_run_id,
        ingest_till_utc=ingest_till_utc,
        timeframes=timeframes,
        refresh_window_days=refresh_window_days,
        contract_discovery_lookback_days=contract_discovery_lookback_days,
        contract_discovery_step_days=contract_discovery_step_days,
        refresh_overlap_minutes=refresh_overlap_minutes,
        max_changed_window_days=max_changed_window_days,
        stability_lag_minutes=stability_lag_minutes,
        expand_contract_chain=expand_contract_chain,
        coverage_mode=coverage_mode,
    )
    normalized_staging_profile = staging_profile.strip() or "verification"
    tags: dict[str, str] = {
        "ta3000/staging_profile": normalized_staging_profile,
        "ta3000/runtime_boundary": MOEX_BASELINE_UPDATE_RUNTIME_BOUNDARY_TAG,
        **{str(key): str(value) for key, value in dict(extra_tags or {}).items()},
    }

    result = job.execute_in_process(
        run_config=run_config,
        instance=instance,
        run_id=str(uuid4()),
        raise_on_error=raise_on_error,
        tags=tags,
    )
    output_paths = moex_baseline_update_output_paths(
        baseline_root=resolved_baseline_root,
        run_id=resolved_run_id,
    )
    baseline_report_path = Path(output_paths["baseline_update_report"])
    if result.success and not baseline_report_path.exists():
        raise RuntimeError(
            "moex baseline Dagster job reported success but the baseline update report artifact is missing: "
            f"{baseline_report_path.as_posix()}"
        )

    return {
        "success": bool(result.success),
        "dagster_run_id": result.run_id,
        "logical_run_id": resolved_run_id,
        "dagster_job_name": job.name,
        "dagster_run_status": "SUCCESS" if result.success else "FAILURE",
        "staging_profile": normalized_staging_profile,
        "baseline_root": resolved_baseline_root.as_posix(),
        "runtime_boundary": {
            "orchestrator": "dagster",
            "hot_table_runtime": MOEX_BASELINE_UPDATE_HOT_TABLE_RUNTIME,
            "python_role": "source_adapter_config_and_evidence",
        },
        "tags": tags,
        "output_paths": output_paths,
        "baseline_update_report_exists": baseline_report_path.exists(),
    }


def execute_moex_data_rebuild_job(
    *,
    raw_table_path: Path | None = None,
    raw_ingest_report_path: Path | None = None,
    canonical_output_dir: Path,
    canonical_target_output_dir: Path | None = None,
    canonical_run_id: str,
    instance: DagsterInstance,
    run_id: str,
    profile_name: str = "canonical_from_existing_raw",
    source_mode: str | None = None,
    publish_mode: str = "promote",
    downstream_mode: str = "invalidate",
    canonical_bars_path: Path | None = None,
    canonical_provenance_path: Path | None = None,
    canonical_session_intervals_path: Path | None = None,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    research_root: Path | None = None,
    dataset_version: str | None = None,
    timeframes: str | Sequence[str] | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    warmup_bars: int | None = None,
    split_method: str | None = None,
    dataset_contract_ids: Sequence[str] | None = None,
    dataset_instrument_ids: Sequence[str] | None = None,
    spark_master: str | None = None,
    extra_tags: dict[str, str] | None = None,
    scheduled_execution_time: datetime | None = None,
    raise_on_error: bool = False,
) -> dict[str, object]:
    resolved_canonical_run_id = canonical_run_id.strip()
    if not resolved_canonical_run_id:
        raise RuntimeError("execute_moex_data_rebuild_job requires canonical_run_id")

    assert_moex_historical_definitions_executable()
    definitions = build_moex_historical_definitions()
    repository = definitions.get_repository_def()
    job = repository.get_job(MOEX_DATA_REBUILD_JOB_NAME)

    run_config = build_moex_data_rebuild_run_config(
        profile_name=profile_name,
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=canonical_output_dir,
        canonical_target_output_dir=canonical_target_output_dir,
        canonical_run_id=resolved_canonical_run_id,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
        canonical_session_calendar_path=canonical_session_calendar_path,
        canonical_roll_map_path=canonical_roll_map_path,
        research_root=research_root,
        dataset_version=dataset_version,
        source_mode=source_mode,
        publish_mode=publish_mode,
        downstream_mode=downstream_mode,
        timeframes=timeframes,
        start_ts=start_ts,
        end_ts=end_ts,
        warmup_bars=warmup_bars,
        split_method=split_method,
        dataset_contract_ids=dataset_contract_ids,
        dataset_instrument_ids=dataset_instrument_ids,
        spark_master=spark_master,
    )
    tags: dict[str, str] = dict(extra_tags or {})
    schedule_payload: dict[str, object] | None = None

    if scheduled_execution_time is not None:
        tags = {
            "dagster/schedule_name": moex_data_rebuild_preview_schedule.name,
            "dagster/scheduled_execution_time": scheduled_execution_time.isoformat(),
            **tags,
        }
        schedule_payload = {
            "name": moex_data_rebuild_preview_schedule.name,
            "cron": MOEX_BASELINE_DAILY_CRON,
            "execution_timezone": MOEX_HISTORICAL_EXECUTION_TIMEZONE,
            "scheduled_execution_time": scheduled_execution_time.isoformat(),
        }

    result = job.execute_in_process(
        run_config=run_config,
        instance=instance,
        run_id=str(uuid4()),
        raise_on_error=raise_on_error,
        tags=tags,
    )
    op_report: dict[str, object] = {}
    if result.success:
        raw_op_report = result.output_for_node(MOEX_DATA_REBUILD_OP_NAME)
        if isinstance(raw_op_report, dict):
            op_report = raw_op_report
    profile = resolve_moex_data_rebuild_profile(profile_name)
    output_paths = (
        moex_historical_output_paths(canonical_output_dir)
        if set(profile.stage_names).intersection({"sessions", "canonical"})
        else {}
    )
    canonical_report_path = (
        Path(output_paths["canonical_report"]) if "canonical_report" in output_paths else None
    )
    if result.success and canonical_report_path is not None and not canonical_report_path.exists():
        raise RuntimeError(
            "moex data rebuild Dagster job reported success but the canonical refresh report artifact is missing: "
            f"{canonical_report_path.as_posix()}"
        )
    reported_output_paths: dict[str, object] = (
        {"canonical_report": output_paths["canonical_report"]}
        if "canonical_report" in output_paths
        else {}
    )
    manifest_payload = op_report.get("manifest")
    if isinstance(manifest_payload, dict):
        manifest_outputs_key = "promoted_outputs" if publish_mode == "promote" else "staged_outputs"
        manifest_outputs = manifest_payload.get(manifest_outputs_key, {})
        if isinstance(manifest_outputs, dict):
            reported_output_paths.update(dict(manifest_outputs))
    elif output_paths:
        reported_output_paths.update(output_paths)

    return {
        "success": bool(result.success),
        "dagster_run_id": result.run_id,
        "logical_run_id": run_id,
        "dagster_job_name": job.name,
        "dagster_run_status": "SUCCESS" if result.success else "FAILURE",
        "profile_name": profile_name,
        "source_mode": source_mode or resolve_moex_data_rebuild_profile(profile_name).source_mode,
        "publish_mode": publish_mode,
        "downstream_mode": downstream_mode,
        "schedule": schedule_payload,
        "tags": tags,
        "materialized_assets": list(op_report.get("materialized_assets", [])),
        "output_paths": reported_output_paths,
        "canonical_report_exists": (
            canonical_report_path.exists() if canonical_report_path is not None else False
        ),
        "manifest_path": str(op_report.get("manifest_path", "")),
    }


def execute_moex_historical_cutover_job(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    canonical_output_dir: Path,
    canonical_target_output_dir: Path | None = None,
    canonical_run_id: str,
    instance: DagsterInstance,
    run_id: str,
    profile_name: str = "canonical_from_existing_raw",
    source_mode: str | None = None,
    publish_mode: str = "promote",
    downstream_mode: str = "invalidate",
    canonical_bars_path: Path | None = None,
    canonical_provenance_path: Path | None = None,
    canonical_session_intervals_path: Path | None = None,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    extra_tags: dict[str, str] | None = None,
    scheduled_execution_time: datetime | None = None,
    raise_on_error: bool = False,
) -> dict[str, object]:
    return execute_moex_data_rebuild_job(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=canonical_output_dir,
        canonical_target_output_dir=canonical_target_output_dir,
        canonical_run_id=canonical_run_id,
        instance=instance,
        run_id=run_id,
        profile_name=profile_name,
        source_mode=source_mode,
        publish_mode=publish_mode,
        downstream_mode=downstream_mode,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
        canonical_session_calendar_path=canonical_session_calendar_path,
        canonical_roll_map_path=canonical_roll_map_path,
        extra_tags=extra_tags,
        scheduled_execution_time=scheduled_execution_time,
        raise_on_error=raise_on_error,
    )


def _resolve_selected_assets(selection: Sequence[str] | None) -> list[str]:
    if selection is None:
        return list(MOEX_HISTORICAL_ASSET_KEYS)
    normalized = sorted({item.strip() for item in selection if item.strip()})
    if not normalized:
        raise ValueError("selection must include at least one moex historical asset key")
    unknown = [item for item in normalized if item not in MOEX_HISTORICAL_DEPENDENCIES]
    if unknown:
        unknown_text = ", ".join(sorted(unknown))
        raise ValueError(f"unknown moex historical selection: {unknown_text}")
    return normalized


def _resolve_expected_materialization(selection: Sequence[str]) -> list[str]:
    resolved: set[str] = set()

    def _visit(asset_name: str) -> None:
        if asset_name in resolved:
            return
        for dependency in MOEX_HISTORICAL_DEPENDENCIES.get(asset_name, tuple()):
            _visit(dependency)
        resolved.add(asset_name)

    for asset_name in selection:
        _visit(asset_name)
    return [asset_name for asset_name in MOEX_HISTORICAL_ASSET_KEYS if asset_name in resolved]


def materialize_moex_historical_assets(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    canonical_output_dir: Path,
    canonical_run_id: str,
    canonical_bars_path: Path | None = None,
    canonical_provenance_path: Path | None = None,
    canonical_session_intervals_path: Path | None = None,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    resolved_canonical_run_id = canonical_run_id.strip()
    if not resolved_canonical_run_id:
        raise RuntimeError("materialize_moex_historical_assets requires canonical_run_id")

    assert_moex_historical_definitions_executable()
    selected_assets = _resolve_selected_assets(selection)
    expected_materialized_assets = _resolve_expected_materialization(selected_assets)
    output_paths = moex_historical_output_paths(canonical_output_dir)
    canonical_report_path = Path(output_paths["canonical_report"])

    op_config = {
        "raw_table_path": raw_table_path.resolve().as_posix(),
        "raw_ingest_report_path": raw_ingest_report_path.resolve().as_posix(),
        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
        "canonical_run_id": resolved_canonical_run_id,
    }
    if canonical_session_intervals_path is not None:
        op_config["canonical_session_intervals_path"] = (
            canonical_session_intervals_path.resolve().as_posix()
        )
    if canonical_bars_path is not None:
        op_config["canonical_bars_path"] = canonical_bars_path.resolve().as_posix()
    if canonical_provenance_path is not None:
        op_config["canonical_provenance_path"] = canonical_provenance_path.resolve().as_posix()
    if canonical_session_calendar_path is not None:
        op_config["canonical_session_calendar_path"] = (
            canonical_session_calendar_path.resolve().as_posix()
        )
    if canonical_roll_map_path is not None:
        op_config["canonical_roll_map_path"] = canonical_roll_map_path.resolve().as_posix()

    raw_report = _read_raw_ingest_report(raw_ingest_report_path)
    raw_status = str(raw_report.get("status", "")).strip()
    if (
        "moex_canonical_refresh" in expected_materialized_assets
        and raw_status not in PASS_LIKE_RAW_STATUSES
    ):
        return {
            "success": False,
            "selected_assets": selected_assets,
            "materialized_assets": expected_materialized_assets,
            "output_paths": output_paths,
            "canonical_report_exists": canonical_report_path.exists(),
            "failure_reason": (
                "canonical refresh cannot start because raw-ingest status is not PASS/PASS-NOOP; "
                f"got `{raw_status or 'EMPTY'}`"
            ),
        }

    result = materialize(
        assets=list(MOEX_HISTORICAL_ASSETS),
        selection=expected_materialized_assets,
        run_config={
            "ops": {
                "moex_raw_ingest": {"config": op_config},
                "moex_canonical_refresh": {"config": op_config},
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
    if result.success and "moex_canonical_refresh" in expected_materialized_assets:
        if not canonical_report_path.exists():
            raise RuntimeError(
                "moex canonical refresh reported success but canonical report artifact is missing: "
                f"{canonical_report_path.as_posix()}"
            )
    report["canonical_report_exists"] = canonical_report_path.exists()
    return report
