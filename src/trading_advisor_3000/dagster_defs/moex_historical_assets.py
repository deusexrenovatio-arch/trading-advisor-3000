from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

from dagster import (
    AssetSelection,
    DagsterInstance,
    DefaultScheduleStatus,
    Definitions,
    Field as DagsterField,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    build_schedule_context,
    define_asset_job,
    materialize,
)

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log
from trading_advisor_3000.product_plane.data_plane.moex.baseline_update import run_moex_baseline_update
from trading_advisor_3000.product_plane.data_plane.moex.phase02_canonical import run_phase02_canonical
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    BASELINE_UPDATE_STORAGE_DIRNAME,
    CANONICAL_BASELINE_BARS_FILENAME,
    CANONICAL_BASELINE_PROVENANCE_FILENAME,
    CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
    CANONICAL_REFRESH_REPORT_FILENAME,
    CANONICAL_REFRESH_STORAGE_DIRNAME,
    configured_moex_historical_data_root,
    RAW_BASELINE_TABLE_RELATIVE_PATH,
    RAW_INGEST_STORAGE_DIRNAME,
    ROUTE_REFRESH_REPORT_FILENAME,
    ROUTE_REFRESH_STORAGE_DIRNAME,
    resolve_external_root,
)


PASS_LIKE_RAW_STATUSES = {"PASS", "PASS-NOOP"}
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MAPPING_REGISTRY = REPO_ROOT / "configs" / "moex_phase01" / "instrument_mapping_registry.v1.yaml"
DEFAULT_UNIVERSE = REPO_ROOT / "configs" / "moex_phase01" / "universe" / "moex-futures-priority.v1.yaml"
DEFAULT_TIMEFRAMES = "5m,15m,1h,4h,1d,1w"
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
}
MOEX_BASELINE_UPDATE_OP_CONFIG_SCHEMA = {
    "mapping_registry_path": DagsterField(str, is_required=False),
    "universe_path": DagsterField(str, is_required=False),
    "raw_table_path": DagsterField(str, is_required=False),
    "canonical_bars_path": DagsterField(str, is_required=False),
    "canonical_provenance_path": DagsterField(str, is_required=False),
    "evidence_root": DagsterField(str, is_required=False),
    "timeframes": DagsterField(str, is_required=False),
    "refresh_window_days": DagsterField(int, is_required=False),
    "contract_discovery_lookback_days": DagsterField(int, is_required=False),
    "contract_discovery_step_days": DagsterField(int, is_required=False),
    "refresh_overlap_minutes": DagsterField(int, is_required=False),
    "max_changed_window_days": DagsterField(int, is_required=False),
    "stability_lag_minutes": DagsterField(int, is_required=False),
    "expand_contract_chain": DagsterField(bool, is_required=False),
    "ingest_till_utc": DagsterField(str, is_required=False),
    "run_id": DagsterField(str, is_required=False),
}
MOEX_HISTORICAL_CUTOVER_JOB_NAME = "moex_historical_cutover_job"
MOEX_BASELINE_UPDATE_JOB_NAME = "moex_baseline_update_job"
MOEX_BASELINE_DAILY_SCHEDULE_NAME = "moex_baseline_daily_update_schedule"
MOEX_BASELINE_DAILY_CRON = "0 2 * * *"
MOEX_HISTORICAL_EXECUTION_TIMEZONE = "Europe/Moscow"
MOEX_HISTORICAL_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


def moex_historical_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="moex_baseline_update",
            description=(
                "Daily baseline updater for MOEX historical data. "
                "Appends/merges fresh raw bars into the stable raw baseline and Spark-refreshes only changed canonical windows."
            ),
            inputs=("moex_iss", "mapping_registry", "universe", "baseline_raw", "baseline_canonical"),
            outputs=("baseline-update-report.json",),
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
            outputs=("canonicalization-report.json",),
        ),
    ]


def _route_artifact_root() -> Path:
    return configured_moex_historical_data_root(repo_root=REPO_ROOT)


def _default_route_run_id(*, scheduled_execution_time: datetime | None = None) -> str:
    anchor = scheduled_execution_time.astimezone(UTC) if scheduled_execution_time else datetime.now(tz=UTC)
    return anchor.strftime("%Y%m%dT%H%M%SZ")


def _build_nightly_schedule_run_config(context) -> dict[str, object]:
    scheduled_execution_time = getattr(context, "scheduled_execution_time", None)
    run_id = _default_route_run_id(scheduled_execution_time=scheduled_execution_time)
    artifact_root = _route_artifact_root()
    return {
        "ops": {
            "moex_raw_ingest": {
                "config": {
                    "mapping_registry_path": DEFAULT_MAPPING_REGISTRY.as_posix(),
                    "universe_path": DEFAULT_UNIVERSE.as_posix(),
                    "raw_ingest_root": (artifact_root / RAW_INGEST_STORAGE_DIRNAME).as_posix(),
                    "canonicalization_root": (artifact_root / CANONICAL_REFRESH_STORAGE_DIRNAME).as_posix(),
                    "route_refresh_root": (artifact_root / ROUTE_REFRESH_STORAGE_DIRNAME).as_posix(),
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
            "moex_canonical_refresh": {
                "config": {
                    "canonicalization_root": (artifact_root / CANONICAL_REFRESH_STORAGE_DIRNAME).as_posix(),
                    "canonical_run_id": run_id,
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
    return {
        "raw_table_path": root / RAW_BASELINE_TABLE_RELATIVE_PATH,
        "canonical_bars_path": canonical_root / CANONICAL_BASELINE_BARS_FILENAME,
        "canonical_provenance_path": canonical_root / CANONICAL_BASELINE_PROVENANCE_FILENAME,
        "evidence_root": root / BASELINE_UPDATE_STORAGE_DIRNAME,
    }


def _build_baseline_daily_schedule_run_config(context) -> dict[str, object]:
    scheduled_execution_time = getattr(context, "scheduled_execution_time", None)
    run_id = _default_route_run_id(scheduled_execution_time=scheduled_execution_time)
    ingest_till_utc = (
        scheduled_execution_time.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if scheduled_execution_time is not None
        else datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    artifact_root = _route_artifact_root()
    paths = _baseline_paths_from_root(artifact_root)
    return {
        "ops": {
            "moex_baseline_update": {
                "config": {
                    "mapping_registry_path": DEFAULT_MAPPING_REGISTRY.as_posix(),
                    "universe_path": DEFAULT_UNIVERSE.as_posix(),
                    "raw_table_path": paths["raw_table_path"].as_posix(),
                    "canonical_bars_path": paths["canonical_bars_path"].as_posix(),
                    "canonical_provenance_path": paths["canonical_provenance_path"].as_posix(),
                    "evidence_root": paths["evidence_root"].as_posix(),
                    "timeframes": DEFAULT_TIMEFRAMES,
                    "refresh_window_days": DEFAULT_DAILY_REFRESH_WINDOW_DAYS,
                    "contract_discovery_lookback_days": DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS,
                    "contract_discovery_step_days": DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
                    "refresh_overlap_minutes": DEFAULT_REFRESH_OVERLAP_MINUTES,
                    "max_changed_window_days": DEFAULT_MAX_CHANGED_WINDOW_DAYS,
                    "stability_lag_minutes": DEFAULT_STABILITY_LAG_MINUTES,
                    "expand_contract_chain": True,
                    "ingest_till_utc": ingest_till_utc,
                    "run_id": run_id,
                }
            },
        }
    }


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
) -> dict[str, str]:
    return {
        "raw_table_path": raw_table_path.resolve().as_posix(),
        "raw_ingest_report_path": raw_ingest_report_path.resolve().as_posix(),
        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
        "canonical_run_id": str(canonical_run_id).strip(),
    }


def build_moex_historical_run_config(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    canonical_output_dir: Path,
    canonical_run_id: str,
) -> dict[str, object]:
    op_config = build_moex_historical_op_config(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=canonical_output_dir,
        canonical_run_id=canonical_run_id,
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
            "max_retries": MOEX_HISTORICAL_RETRY_POLICY.max_retries,
            "delay": MOEX_HISTORICAL_RETRY_POLICY.delay,
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
        raise RuntimeError(f"mapping registry path does not exist: {mapping_registry_path.as_posix()}")
    if not universe_path.exists():
        raise RuntimeError(f"universe path does not exist: {universe_path.as_posix()}")
    if not run_id:
        raise RuntimeError("moex historical scheduled route requires non-empty `run_id`")
    if execution_mode not in {"sequential", "parallel"}:
        raise RuntimeError("moex historical scheduled route requires `execution_mode` in {sequential, parallel}")

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
    command.append("--expand-contract-chain" if expand_contract_chain else "--no-expand-contract-chain")
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
    raw_ingest_report_path = Path(str(route_report.get("raw_ingest_report_path", "")).strip()).resolve()
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
    retry_policy=MOEX_HISTORICAL_RETRY_POLICY,
)
def moex_baseline_update(context) -> dict[str, object]:
    op_config = _op_config_from_context(context)
    artifact_root = _route_artifact_root()
    default_paths = _baseline_paths_from_root(artifact_root)

    mapping_registry_path = Path(
        _text_value(op_config, "mapping_registry_path") or DEFAULT_MAPPING_REGISTRY.as_posix()
    ).resolve()
    universe_path = Path(_text_value(op_config, "universe_path") or DEFAULT_UNIVERSE.as_posix()).resolve()
    raw_table_path = Path(
        _text_value(op_config, "raw_table_path") or default_paths["raw_table_path"].as_posix()
    ).resolve()
    canonical_bars_path = Path(
        _text_value(op_config, "canonical_bars_path") or default_paths["canonical_bars_path"].as_posix()
    ).resolve()
    canonical_provenance_path = Path(
        _text_value(op_config, "canonical_provenance_path")
        or default_paths["canonical_provenance_path"].as_posix()
    ).resolve()
    evidence_root = Path(
        _text_value(op_config, "evidence_root") or default_paths["evidence_root"].as_posix()
    ).resolve()
    run_id = _text_value(op_config, "run_id", "canonical_run_id") or _default_route_run_id()
    ingest_till_utc = _text_value(op_config, "ingest_till_utc") or datetime.now(tz=UTC).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "Z")

    report = run_moex_baseline_update(
        mapping_registry_path=mapping_registry_path,
        universe_path=universe_path,
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
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
        repo_root=REPO_ROOT,
    )
    if str(report.get("publish_decision", "")).strip() != "publish":
        raise RuntimeError("moex baseline update did not publish")
    context.add_output_metadata(
        {
            "mode": "baseline_update",
            "source_rows": int(report.get("source_rows", 0) or 0),
            "incremental_rows": int(report.get("incremental_rows", 0) or 0),
            "current_changed_windows": int(report.get("current_changed_windows", 0) or 0),
            "effective_changed_windows": int(report.get("effective_changed_windows", 0) or 0),
            "scoped_canonical_rows": int(
                dict(report.get("canonical_report", {}) or {}).get("scoped_canonical_rows", 0) or 0
            ),
            "canonical_rows": int(dict(report.get("canonical_report", {}) or {}).get("canonical_rows", 0) or 0),
            "raw_table_path": str(report.get("raw_table_path", "")),
            "canonical_bars_path": str(report.get("canonical_bars_path", "")),
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
            raise RuntimeError(f"raw ingest report path does not exist: {raw_ingest_report_path.as_posix()}")

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

    report = run_phase02_canonical(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id=run_id,
        raw_ingest_run_report=raw_report,
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


MOEX_HISTORICAL_ASSETS = (
    moex_baseline_update,
    moex_raw_ingest,
    moex_canonical_refresh,
)


moex_baseline_update_job = define_asset_job(
    name=MOEX_BASELINE_UPDATE_JOB_NAME,
    selection=AssetSelection.groups("moex_baseline_update"),
    op_retry_policy=MOEX_HISTORICAL_RETRY_POLICY,
)

moex_historical_cutover_job = define_asset_job(
    name=MOEX_HISTORICAL_CUTOVER_JOB_NAME,
    selection=AssetSelection.groups("moex_historical_cutover"),
    op_retry_policy=MOEX_HISTORICAL_RETRY_POLICY,
)

moex_historical_cutover_preview_schedule = ScheduleDefinition(
    name="moex_historical_cutover_preview_schedule",
    job=moex_historical_cutover_job,
    cron_schedule=MOEX_BASELINE_DAILY_CRON,
    run_config_fn=_build_nightly_schedule_run_config,
    execution_timezone=MOEX_HISTORICAL_EXECUTION_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Manual proof-only schedule definition for cutover tests; not registered as an active nightly schedule.",
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
    schedules=[moex_baseline_daily_update_schedule],
    jobs=[moex_baseline_update_job, moex_historical_cutover_job],
)


def assert_moex_historical_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or moex_historical_definitions
    try:
        repository = defs.get_repository_def()
        repository.get_job(MOEX_BASELINE_UPDATE_JOB_NAME)
        job = repository.get_job(MOEX_HISTORICAL_CUTOVER_JOB_NAME)
    except Exception as exc:
        raise RuntimeError(
            "moex historical Dagster definitions are metadata-only or incomplete: "
            "missing executable baseline update or cutover job."
        ) from exc

    expected_nodes = set(MOEX_HISTORICAL_ASSET_KEYS)
    actual_nodes = set(job.graph.node_dict.keys())
    missing_nodes = sorted(expected_nodes - actual_nodes)
    if missing_nodes:
        missing_text = ", ".join(missing_nodes)
        raise RuntimeError(
            "moex historical Dagster definitions are metadata-only or incomplete: "
            f"missing executable asset nodes: {missing_text}"
        )


def build_moex_historical_definitions() -> Definitions:
    assert_moex_historical_definitions_executable(moex_historical_definitions)
    return moex_historical_definitions


def moex_historical_output_paths(output_dir: Path) -> dict[str, str]:
    resolved = output_dir.resolve()
    return {
        "canonical_report": (resolved / CANONICAL_REFRESH_REPORT_FILENAME).as_posix(),
        "canonical_bars": (resolved / "delta" / "canonical_bars.delta").as_posix(),
        "canonical_bar_provenance": (resolved / "delta" / "canonical_bar_provenance.delta").as_posix(),
    }


def execute_moex_historical_cutover_job(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    canonical_output_dir: Path,
    canonical_run_id: str,
    instance: DagsterInstance,
    run_id: str,
    extra_tags: dict[str, str] | None = None,
    scheduled_execution_time: datetime | None = None,
    raise_on_error: bool = False,
) -> dict[str, object]:
    resolved_canonical_run_id = canonical_run_id.strip()
    if not resolved_canonical_run_id:
        raise RuntimeError("execute_moex_historical_cutover_job requires canonical_run_id")

    assert_moex_historical_definitions_executable()
    definitions = build_moex_historical_definitions()
    repository = definitions.get_repository_def()
    job = repository.get_job(MOEX_HISTORICAL_CUTOVER_JOB_NAME)

    run_config = build_moex_historical_run_config(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=canonical_output_dir,
        canonical_run_id=resolved_canonical_run_id,
    )
    tags: dict[str, str] = dict(extra_tags or {})
    schedule_payload: dict[str, object] | None = None

    if scheduled_execution_time is not None:
        schedule_context = build_schedule_context(
            instance=instance,
            scheduled_execution_time=scheduled_execution_time,
            repository_def=repository,
        )
        execution_data = moex_historical_cutover_preview_schedule.evaluate_tick(schedule_context)
        run_requests = list(getattr(execution_data, "run_requests", []) or [])
        if len(run_requests) != 1:
            raise RuntimeError(
                "moex historical nightly schedule must resolve exactly one run request for governed execution"
            )
        request = run_requests[0]
        schedule_run_config = request.run_config if isinstance(request.run_config, dict) else {}
        run_config = _merge_run_config(schedule_run_config, run_config)
        tags = {
            **{str(key): str(value) for key, value in dict(request.tags).items()},
            **tags,
        }
        schedule_payload = {
            "name": moex_historical_cutover_preview_schedule.name,
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
    output_paths = moex_historical_output_paths(canonical_output_dir)
    canonical_report_path = Path(output_paths["canonical_report"])
    if result.success and not canonical_report_path.exists():
        raise RuntimeError(
            "moex historical Dagster job reported success but the canonical refresh report artifact is missing: "
            f"{canonical_report_path.as_posix()}"
        )

    return {
        "success": bool(result.success),
        "dagster_run_id": result.run_id,
        "logical_run_id": run_id,
        "dagster_job_name": job.name,
        "dagster_run_status": "SUCCESS" if result.success else "FAILURE",
        "schedule": schedule_payload,
        "tags": tags,
        "materialized_assets": list(MOEX_HISTORICAL_ASSET_KEYS),
        "output_paths": output_paths,
        "canonical_report_exists": canonical_report_path.exists(),
    }


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

    op_config = {
        "raw_table_path": raw_table_path.resolve().as_posix(),
        "raw_ingest_report_path": raw_ingest_report_path.resolve().as_posix(),
        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
        "canonical_run_id": resolved_canonical_run_id,
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
    canonical_report_path = Path(output_paths["canonical_report"])
    if result.success and "moex_canonical_refresh" in expected_materialized_assets:
        if not canonical_report_path.exists():
            raise RuntimeError(
                "moex canonical refresh reported success but canonical report artifact is missing: "
                f"{canonical_report_path.as_posix()}"
            )
    report["canonical_report_exists"] = canonical_report_path.exists()
    return report
