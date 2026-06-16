from __future__ import annotations

import hashlib
import json
import math
import shutil
import traceback
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import yaml

from trading_advisor_3000.dagster_defs import (
    materialize_research_backtest_assets,
    materialize_research_data_prep_assets,
    materialize_research_projection_assets,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import (
    load_schema,
    validate_schema,
)
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_frame,
    read_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import ContinuousFrontPolicy
from trading_advisor_3000.product_plane.research.jobs._common import (
    validate_research_contracts,
    write_json,
)
from trading_advisor_3000.product_plane.research.registry_store import (
    append_rankings_index,
    append_run_stats_index,
    build_campaign_id,
    build_campaign_run_id,
    registry_output_paths,
    research_registry_root,
    write_campaign_definition,
    write_campaign_run,
    write_strategy_note,
)
from trading_advisor_3000.product_plane.research.validation import (
    NESTED_WALK_FORWARD_SCHEME,
    build_nested_walk_forward_plan,
)

CAMPAIGN_SCHEMA = Path(
    "src/trading_advisor_3000/product_plane/contracts/schemas/research_campaign.v1.json"
)
RUN_SUMMARY_SCHEMA = Path(
    "src/trading_advisor_3000/product_plane/contracts/schemas/research_run_summary.v1.json"
)
STATUS_VALUES = {"queued", "running", "success", "failed", "blocked"}
TIMEFRAME_ORDER = {
    "1m": 1,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
}
TARGET_STEPS = {
    "data_prep": ("research_data_prep",),
    "backtest": ("research_data_prep", "strategy_registry_refresh", "backtest", "ranking"),
    "projection": (
        "research_data_prep",
        "strategy_registry_refresh",
        "backtest",
        "ranking",
        "projection",
    ),
}
DATA_PREP_TABLES = (
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
MATERIALIZATION_LOCK_FILENAME = "materialization.lock.json"
RESEARCH_DATA_PREP_KWARGS = {
    "canonical_output_dir",
    "materialized_output_dir",
    "results_output_dir",
    "campaign_id",
    "campaign_run_id",
    "dataset_version",
    "dataset_name",
    "universe_id",
    "timeframes",
    "base_timeframe",
    "start_ts",
    "end_ts",
    "warmup_bars",
    "split_method",
    "series_mode",
    "continuous_front_policy",
    "dataset_contract_ids",
    "dataset_instrument_ids",
    "spark_master",
    "indicator_set_version",
    "indicator_profile_version",
    "derived_indicator_set_version",
    "derived_indicator_profile_version",
    "volume_profile_raw_1m_table_path",
    "volume_profile_tick_size_by_instrument",
    "validation_plan",
    "continuous_front_indicator_qc_mode",
    "continuous_front_indicator_sidecar_materialization_mode",
    "code_version",
    "reuse_existing_materialization",
}


class CampaignBlockedError(RuntimeError):
    """Raised for fail-closed campaign validation or route decisions."""


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def resolve_path(*, repo_root: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S%fZ")


def load_campaign_file(*, config_path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CampaignBlockedError(
            f"campaign config must decode into an object: {config_path.as_posix()}"
        )
    return {str(key): value for key, value in payload.items()}


def normalize_campaign_config(*, repo_root: Path, raw: dict[str, Any]) -> dict[str, Any]:
    schema = load_schema(repo_root / CAMPAIGN_SCHEMA)
    validate_schema(schema, raw)

    volume_profile = _normalize_volume_profile(raw.get("volume_profile"), repo_root=repo_root)
    normalized = {
        "campaign_name": _normalized_non_empty(raw["campaign_name"]),
        "target_stage": _normalized_enum(
            raw["target_stage"], {"data_prep", "backtest", "projection"}, field="target_stage"
        ),
        "canonical_output_dir": resolve_path(
            repo_root=repo_root, raw=str(raw["canonical_output_dir"])
        ).as_posix(),
        "materialized_root": resolve_path(
            repo_root=repo_root, raw=str(raw["materialized_root"])
        ).as_posix(),
        "runs_root": resolve_path(repo_root=repo_root, raw=str(raw["runs_root"])).as_posix(),
        "dataset": _normalize_dataset(payload=_expect_dict(raw, "dataset")),
        "profiles": _normalize_profiles(_expect_dict(raw, "profiles")),
        "strategy_space": _normalize_strategy_space(_expect_dict(raw, "strategy_space")),
        "backtest": _normalize_backtest(_expect_dict(raw, "backtest")),
        "ranking_policy": _normalize_ranking_policy(_expect_dict(raw, "ranking_policy")),
        "projection_policy": _normalize_projection_policy(_expect_dict(raw, "projection_policy")),
        "execution": _normalize_execution(_expect_dict(raw, "execution")),
    }
    if volume_profile:
        normalized["volume_profile"] = volume_profile
    validation = _normalize_validation(
        raw.get("validation"),
        dataset=dict(normalized["dataset"]),
        backtest=dict(normalized["backtest"]),
    )
    if validation:
        normalized["validation"] = validation
    if normalized["dataset"]["base_timeframe"] not in normalized["dataset"]["timeframes"]:
        raise CampaignBlockedError("dataset.base_timeframe must be included in dataset.timeframes")
    if normalized["backtest"]["backtest_timeframe"] not in normalized["dataset"]["timeframes"]:
        raise CampaignBlockedError(
            "backtest.backtest_timeframe must be included in dataset.timeframes"
        )
    validate_schema(schema, normalized)
    return normalized


def build_config_fingerprint(normalized_config: dict[str, Any]) -> str:
    return _stable_hash(normalized_config)


def build_materialization_key(normalized_config: dict[str, Any]) -> str:
    dataset = dict(normalized_config["dataset"])
    profiles = dict(normalized_config["profiles"])
    payload = {
        "canonical_output_dir": normalized_config["canonical_output_dir"],
        "dataset_version": dataset["dataset_version"],
        "series_mode": dataset["series_mode"],
        "timeframes": dataset["timeframes"],
        "base_timeframe": dataset["base_timeframe"],
        "start_ts": dataset["start_ts"],
        "end_ts": dataset["end_ts"],
        "warmup_bars": dataset["warmup_bars"],
        "split_method": dataset["split_method"],
        "contract_ids": dataset["contract_ids"],
        "instrument_ids": dataset["instrument_ids"],
        "continuous_front_policy": dict(dataset["continuous_front_policy"]),
        "indicator_set_version": profiles["indicator_set_version"],
        "indicator_profile_version": profiles["indicator_profile_version"],
        "derived_indicator_set_version": profiles["derived_indicator_set_version"],
        "derived_indicator_profile_version": profiles["derived_indicator_profile_version"],
        "volume_profile": dict(normalized_config.get("volume_profile", {})),
        "validation": dict(normalized_config.get("validation", {})),
    }
    return _stable_hash(payload)


def run_campaign(*, config_path: Path, repo_root: Path | None = None) -> dict[str, Any]:
    repo_root = repo_root or resolve_repo_root()
    resolved_config_path = resolve_path(repo_root=repo_root, raw=config_path.as_posix())
    run_id = build_run_id()
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    started = perf_counter()
    warnings: list[str] = []

    raw_config: dict[str, Any] = {}
    raw_config_error: Exception | None = None
    raw_config_traceback = ""
    try:
        raw_config = load_campaign_file(config_path=resolved_config_path)
    except Exception as exc:  # noqa: BLE001
        raw_config_error = exc
        raw_config_traceback = traceback.format_exc()

    run_root, campaign_name, provisional_warnings = _prepare_provisional_run_root(
        repo_root=repo_root,
        raw_config=raw_config,
        run_id=run_id,
    )
    warnings.extend(provisional_warnings)
    _write_status(
        run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="queued"
    )
    final_results_root = run_root / "results"
    staging_results_root = run_root / "results-staging"

    if raw_config_error is not None:
        stderr_lines.append(raw_config_traceback)
        _write_prevalidation_lock(
            run_root=run_root,
            run_id=run_id,
            resolved_config_path=resolved_config_path,
            raw_config=raw_config,
            results_root=final_results_root,
            error=raw_config_error,
            repo_root=repo_root,
        )
        summary = _build_prevalidation_failure_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_name=campaign_name,
            raw_config=raw_config,
            resolved_config_path=resolved_config_path,
            run_root=run_root,
            warnings=warnings,
            exc=raw_config_error,
            started=started,
        )
        _write_logs(run_root=run_root, stdout_lines=stdout_lines, stderr_lines=stderr_lines)
        _write_terminal_artifacts(run_root=run_root, summary=summary)
        _write_status(
            run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="blocked"
        )
        return summary

    try:
        normalized_config = normalize_campaign_config(repo_root=repo_root, raw=raw_config)
    except Exception as exc:  # noqa: BLE001
        stderr_lines.append(traceback.format_exc())
        _write_prevalidation_lock(
            run_root=run_root,
            run_id=run_id,
            resolved_config_path=resolved_config_path,
            raw_config=raw_config,
            results_root=final_results_root,
            error=exc,
            repo_root=repo_root,
        )
        summary = _build_prevalidation_failure_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_name=campaign_name,
            raw_config=raw_config,
            resolved_config_path=resolved_config_path,
            run_root=run_root,
            warnings=warnings,
            exc=exc,
            started=started,
        )
        _write_logs(run_root=run_root, stdout_lines=stdout_lines, stderr_lines=stderr_lines)
        _write_terminal_artifacts(run_root=run_root, summary=summary)
        _write_status(
            run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="blocked"
        )
        return summary

    campaign_name = str(normalized_config["campaign_name"])
    config_fingerprint = build_config_fingerprint(normalized_config)
    campaign_id = build_campaign_id(config_fingerprint=config_fingerprint)
    campaign_run_id = build_campaign_run_id(campaign_id=campaign_id, run_id=run_id)
    materialization_key = build_materialization_key(normalized_config)
    validation_plan = _build_validation_plan(normalized_config)
    materialized_root = Path(str(normalized_config["materialized_root"]))
    registry_root = research_registry_root(materialized_root=materialized_root)
    materialization_exists = _has_reusable_materialization(
        materialized_root,
        materialization_key=materialization_key,
    )
    reuse_existing_materialization = materialization_exists and not bool(
        normalized_config["execution"]["force_rematerialize"]
    )
    write_json(
        run_root / "campaign.lock.json",
        {
            "run_id": run_id,
            "campaign_id": campaign_id,
            "campaign_run_id": campaign_run_id,
            "config_path": resolved_config_path.as_posix(),
            "config_fingerprint": config_fingerprint,
            "materialization_key": materialization_key,
            "registry_root": registry_root.as_posix(),
            "materialized_output_dir": materialized_root.as_posix(),
            "results_output_dir": final_results_root.as_posix(),
            "staging_results_output_dir": staging_results_root.as_posix(),
            "reuse_existing_materialization": reuse_existing_materialization,
            "campaign": normalized_config,
            "validation_plan": validation_plan,
            "created_at": utc_now_iso(),
        },
    )
    _write_status(
        run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="running"
    )
    write_campaign_definition(
        registry_root=registry_root,
        campaign_id=campaign_id,
        campaign_name=campaign_name,
        target_stage=str(normalized_config["target_stage"]),
        config_fingerprint=config_fingerprint,
        dataset_version=str(normalized_config["dataset"]["dataset_version"]),
        indicator_set_version=str(normalized_config["profiles"]["indicator_set_version"]),
        indicator_profile_version=str(normalized_config["profiles"]["indicator_profile_version"]),
        derived_indicator_set_version=str(
            normalized_config["profiles"]["derived_indicator_set_version"]
        ),
        derived_indicator_profile_version=str(
            normalized_config["profiles"]["derived_indicator_profile_version"]
        ),
        strategy_space=dict(normalized_config["strategy_space"]),
        backtest_policy=dict(normalized_config["backtest"]),
        ranking_policy=dict(normalized_config["ranking_policy"]),
        projection_policy=dict(normalized_config["projection_policy"]),
        execution_policy=dict(normalized_config["execution"]),
        campaign_config=normalized_config,
    )

    warnings: list[str] = []
    if reuse_existing_materialization:
        warnings.append("materialized research gold layer reused from matching materialization_key")

    stage = str(normalized_config["target_stage"])
    stage_steps = list(TARGET_STEPS[stage])
    reused_steps = ["research_data_prep"] if reuse_existing_materialization else []
    executed_steps = [step for step in stage_steps if step not in reused_steps]
    if stage == "data_prep" and reuse_existing_materialization:
        executed_steps = []

    try:
        report = _dispatch_campaign(
            normalized_config=normalized_config,
            materialized_root=materialized_root,
            results_root=staging_results_root,
            reuse_existing_materialization=reuse_existing_materialization,
            campaign_id=campaign_id,
            campaign_run_id=campaign_run_id,
        )
        contract_validation = validate_research_contracts(
            output_paths=dict(report["output_paths"]),
            materialized_assets=list(report["materialized_assets"]),
            rows_by_table=dict(report.get("rows_by_table", {})),
        )
        warnings.extend(list(contract_validation.get("warnings", [])))
        if contract_validation["status"] != "passed":
            raise CampaignBlockedError(
                "; ".join(contract_validation["errors"]) or "research contract validation failed"
            )
        if "research_data_prep" in executed_steps:
            _write_materialization_lock(
                materialized_root=materialized_root,
                materialization_key=materialization_key,
                normalized_config=normalized_config,
                campaign_id=campaign_id,
                campaign_run_id=campaign_run_id,
                report=report,
            )

        published_output_paths = _publish_staged_results(
            staging_results_root=staging_results_root,
            final_results_root=final_results_root,
            output_paths=dict(report["output_paths"]),
        )
        registry_paths = _campaign_registry_output_paths(registry_root=registry_root)
        index_paths = _publish_global_indices(
            registry_root=registry_root, results_root=final_results_root
        )
        total_seconds = round(perf_counter() - started, 6)
        duration_metrics = _build_duration_metrics(
            total_seconds=total_seconds,
            rows_by_table=dict(report.get("rows_by_table", {})),
            results_root=final_results_root,
        )
        result_digest = _build_result_digest(
            target_stage=stage,
            results_root=final_results_root,
            duration_metrics=duration_metrics,
            rows_by_table=dict(report.get("rows_by_table", {})),
            materialized_root=materialized_root,
            output_paths={**published_output_paths, **registry_paths, **index_paths},
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            force_rematerialize=bool(normalized_config["execution"]["force_rematerialize"]),
        )
        success_run_record = {
            "registry_root": registry_root,
            "campaign_run_id": campaign_run_id,
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "target_stage": stage,
            "config_fingerprint": config_fingerprint,
            "strategy_space_id": str(report.get("strategy_space_id", "")),
            "materialization_key": materialization_key,
            "materialized_output_dir": materialized_root.as_posix(),
            "results_output_dir": final_results_root.as_posix(),
            "reuse_existing_materialization": reuse_existing_materialization,
            "executed_steps": executed_steps,
            "reused_steps": reused_steps,
            "rows_by_table": dict(report.get("rows_by_table", {})),
            "output_paths": {**published_output_paths, **registry_paths, **index_paths},
            "warnings": warnings,
            "started_at": utc_now_iso(),
            "finished_at": utc_now_iso(),
            "result_digest": result_digest,
        }
        write_campaign_run(**success_run_record, status="publishing")
        write_strategy_note(
            registry_root=registry_root,
            entity_type="campaign_run",
            entity_id=campaign_run_id,
            note_kind="verdict",
            summary=f"campaign run {campaign_name} completed successfully",
            source="system",
            tags=("campaign", stage, "success"),
        )
        _write_publish_commit(
            final_results_root=final_results_root,
            campaign_run_id=campaign_run_id,
            published_output_paths=published_output_paths,
            registry_output_paths={**registry_paths, **index_paths},
        )
        write_campaign_run(**success_run_record, status="success")
        summary = _build_terminal_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_id=campaign_id,
            campaign_run_id=campaign_run_id,
            campaign_name=campaign_name,
            target_stage=stage,
            canonical_output_dir=str(normalized_config["canonical_output_dir"]),
            registry_root=registry_root.as_posix(),
            materialization_key=materialization_key,
            strategy_space_id=str(report.get("strategy_space_id", "")),
            materialized_root=materialized_root.as_posix(),
            run_root=run_root.as_posix(),
            dataset_version=str(normalized_config["dataset"]["dataset_version"]),
            config_fingerprint=config_fingerprint,
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            durations=duration_metrics,
            rows_by_table=dict(report.get("rows_by_table", {})),
            output_paths={**published_output_paths, **registry_paths, **index_paths},
            dagster_selected_assets=list(report["selected_assets"]),
            dagster_materialized_assets=list(report["materialized_assets"]),
            warnings=warnings,
            status="success",
            error=None,
            result_digest=result_digest,
        )
        write_json(
            run_root / "artifacts-index.json",
            _build_artifacts_index(
                {**published_output_paths, **registry_paths, **index_paths},
                report.get("rows_by_table", {}),
            ),
        )
        _write_logs(run_root=run_root, stdout_lines=stdout_lines, stderr_lines=stderr_lines)
        _write_terminal_artifacts(run_root=run_root, summary=summary)
        _write_status(
            run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="success"
        )
        return summary
    except CampaignBlockedError as exc:
        stderr_lines.append(traceback.format_exc())
        _quarantine_uncommitted_results(final_results_root=final_results_root, run_root=run_root)
        _record_failed_campaign_run(
            registry_root=registry_root,
            campaign_run_id=campaign_run_id,
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            target_stage=stage,
            config_fingerprint=config_fingerprint,
            strategy_space_id=str(report.get("strategy_space_id", ""))
            if "report" in locals()
            else "",
            materialization_key=materialization_key,
            materialized_root=materialized_root,
            final_results_root=final_results_root,
            reuse_existing_materialization=reuse_existing_materialization,
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            warnings=warnings,
            status="blocked",
            exc=exc,
        )
        write_strategy_note(
            registry_root=registry_root,
            entity_type="campaign_run",
            entity_id=campaign_run_id,
            note_kind="finding",
            severity="error",
            summary=str(exc),
            source="system",
            tags=("campaign", stage, "blocked"),
        )
        summary = _build_failure_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_id=campaign_id,
            campaign_run_id=campaign_run_id,
            registry_root=registry_root,
            campaign_name=campaign_name,
            normalized_config=normalized_config,
            materialization_key=materialization_key,
            strategy_space_id=str(report.get("strategy_space_id", ""))
            if "report" in locals()
            else "",
            materialized_root=materialized_root,
            run_root=run_root,
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            warnings=warnings,
            status="blocked",
            exc=exc,
            started=started,
        )
        _write_logs(run_root=run_root, stdout_lines=stdout_lines, stderr_lines=stderr_lines)
        _write_terminal_artifacts(run_root=run_root, summary=summary)
        _write_status(
            run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="blocked"
        )
        return summary
    except Exception as exc:  # noqa: BLE001
        stderr_lines.append(traceback.format_exc())
        _quarantine_uncommitted_results(final_results_root=final_results_root, run_root=run_root)
        _record_failed_campaign_run(
            registry_root=registry_root,
            campaign_run_id=campaign_run_id,
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            target_stage=stage,
            config_fingerprint=config_fingerprint,
            strategy_space_id=str(report.get("strategy_space_id", ""))
            if "report" in locals()
            else "",
            materialization_key=materialization_key,
            materialized_root=materialized_root,
            final_results_root=final_results_root,
            reuse_existing_materialization=reuse_existing_materialization,
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            warnings=warnings,
            status="failed",
            exc=exc,
        )
        write_strategy_note(
            registry_root=registry_root,
            entity_type="campaign_run",
            entity_id=campaign_run_id,
            note_kind="finding",
            severity="critical",
            summary=str(exc),
            source="system",
            tags=("campaign", stage, "failed"),
        )
        summary = _build_failure_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_id=campaign_id,
            campaign_run_id=campaign_run_id,
            registry_root=registry_root,
            campaign_name=campaign_name,
            normalized_config=normalized_config,
            materialization_key=materialization_key,
            strategy_space_id=str(report.get("strategy_space_id", ""))
            if "report" in locals()
            else "",
            materialized_root=materialized_root,
            run_root=run_root,
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            warnings=warnings,
            status="failed",
            exc=exc,
            started=started,
        )
        _write_logs(run_root=run_root, stdout_lines=stdout_lines, stderr_lines=stderr_lines)
        _write_terminal_artifacts(run_root=run_root, summary=summary)
        _write_status(
            run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="failed"
        )
        return summary


def _build_prevalidation_failure_summary(
    *,
    repo_root: Path,
    run_id: str,
    campaign_name: str,
    raw_config: dict[str, Any],
    resolved_config_path: Path,
    run_root: Path,
    warnings: list[str],
    exc: Exception,
    started: float,
) -> dict[str, Any]:
    return _build_terminal_summary(
        repo_root=repo_root,
        run_id=run_id,
        campaign_id="",
        campaign_run_id="",
        campaign_name=campaign_name,
        target_stage=_raw_target_stage(raw_config),
        canonical_output_dir=_resolved_optional_path(
            repo_root=repo_root, raw=raw_config.get("canonical_output_dir")
        ),
        registry_root="",
        materialization_key="",
        strategy_space_id="",
        materialized_root=_resolved_optional_path(
            repo_root=repo_root, raw=raw_config.get("materialized_root")
        ),
        run_root=run_root.as_posix(),
        dataset_version=_raw_dataset_version(raw_config),
        config_fingerprint=_raw_config_fingerprint(
            raw_config=raw_config, config_path=resolved_config_path
        ),
        executed_steps=[],
        reused_steps=[],
        durations={"total_seconds": round(perf_counter() - started, 6)},
        rows_by_table={},
        output_paths={},
        dagster_selected_assets=[],
        dagster_materialized_assets=[],
        warnings=warnings,
        status="blocked",
        error={"type": type(exc).__name__, "message": str(exc)},
        result_digest=None,
    )


def _dispatch_campaign(
    *,
    normalized_config: dict[str, Any],
    materialized_root: Path,
    results_root: Path,
    reuse_existing_materialization: bool,
    campaign_id: str,
    campaign_run_id: str,
) -> dict[str, Any]:
    common = _dagster_common_kwargs(
        normalized_config=normalized_config,
        materialized_root=materialized_root,
        results_root=results_root,
        reuse_existing_materialization=reuse_existing_materialization,
        campaign_id=campaign_id,
        campaign_run_id=campaign_run_id,
    )
    stage = str(normalized_config["target_stage"])
    raise_on_error = bool(normalized_config["execution"]["raise_on_error"])
    if stage == "data_prep":
        data_prep = {
            key: value for key, value in common.items() if key in RESEARCH_DATA_PREP_KWARGS
        }
        return materialize_research_data_prep_assets(**data_prep, raise_on_error=raise_on_error)
    if stage == "backtest":
        return materialize_research_backtest_assets(**common, raise_on_error=raise_on_error)
    if stage == "projection":
        return materialize_research_projection_assets(**common, raise_on_error=raise_on_error)
    raise CampaignBlockedError(f"unsupported target_stage `{stage}`")


def _dagster_common_kwargs(
    *,
    normalized_config: dict[str, Any],
    materialized_root: Path,
    results_root: Path,
    reuse_existing_materialization: bool,
    campaign_id: str,
    campaign_run_id: str,
) -> dict[str, Any]:
    dataset = dict(normalized_config["dataset"])
    profiles = dict(normalized_config["profiles"])
    backtest = dict(normalized_config["backtest"])
    ranking = dict(normalized_config["ranking_policy"])
    projection = dict(normalized_config["projection_policy"])
    volume_profile = dict(normalized_config.get("volume_profile", {}))
    validation_plan = _build_validation_plan(normalized_config)
    return {
        "canonical_output_dir": Path(str(normalized_config["canonical_output_dir"])),
        "materialized_output_dir": materialized_root,
        "results_output_dir": results_root,
        "campaign_id": campaign_id,
        "campaign_run_id": campaign_run_id,
        "dataset_version": str(dataset["dataset_version"]),
        "dataset_name": str(dataset["dataset_name"]),
        "universe_id": str(dataset["universe_id"]),
        "timeframes": tuple(str(item) for item in dataset["timeframes"]),
        "base_timeframe": str(dataset["base_timeframe"]),
        "start_ts": str(dataset["start_ts"] or ""),
        "end_ts": str(dataset["end_ts"] or ""),
        "warmup_bars": int(dataset["warmup_bars"]),
        "split_method": "walk_forward" if validation_plan else str(dataset["split_method"]),
        "validation_plan": validation_plan,
        "series_mode": str(dataset["series_mode"]),
        "continuous_front_policy": dict(dataset["continuous_front_policy"]),
        "dataset_contract_ids": tuple(str(item) for item in dataset["contract_ids"]),
        "dataset_instrument_ids": tuple(str(item) for item in dataset["instrument_ids"]),
        "indicator_set_version": str(profiles["indicator_set_version"]),
        "indicator_profile_version": str(profiles["indicator_profile_version"]),
        "derived_indicator_set_version": str(profiles["derived_indicator_set_version"]),
        "derived_indicator_profile_version": str(profiles["derived_indicator_profile_version"]),
        "volume_profile_raw_1m_table_path": str(volume_profile.get("raw_1m_table_path") or ""),
        "volume_profile_tick_size_by_instrument": dict(
            volume_profile.get("tick_size_by_instrument", {})
        ),
        "continuous_front_indicator_qc_mode": str(
            normalized_config["execution"]["continuous_front_indicator_qc_mode"]
        ),
        "continuous_front_indicator_sidecar_materialization_mode": str(
            normalized_config["execution"][
                "continuous_front_indicator_sidecar_materialization_mode"
            ]
        ),
        "spark_master": str(normalized_config["execution"]["spark_master"]),
        "code_version": "product-plane-run-campaign",
        "strategy_space": dict(normalized_config["strategy_space"]),
        "param_batch_size": int(backtest["param_batch_size"]),
        "series_batch_size": int(backtest["series_batch_size"]),
        "backtest_timeframe": str(backtest["backtest_timeframe"]),
        "backtest_contract_ids": tuple(str(item) for item in dataset["contract_ids"]),
        "backtest_instrument_ids": tuple(str(item) for item in dataset["instrument_ids"]),
        "fees_bps": float(backtest["fees_bps"]),
        "slippage_bps": float(backtest["slippage_bps"]),
        "allow_short": bool(backtest["allow_short"]),
        "window_count": int(backtest["window_count"]),
        "ranking_policy_id": str(ranking["policy_id"]),
        "ranking_metric_order": tuple(str(item) for item in ranking["metric_order"]),
        "require_out_of_sample_pass": bool(ranking["require_out_of_sample_pass"]),
        "min_trade_count": int(ranking["min_trade_count"]),
        "min_trade_count_per_fold": int(ranking["min_trade_count_per_fold"]),
        "min_fold_count": int(ranking["min_fold_count"]),
        "max_drawdown_cap": float(ranking["max_drawdown_cap"]),
        "min_positive_fold_ratio": float(ranking["min_positive_fold_ratio"]),
        "stress_slippage_bps": float(ranking["stress_slippage_bps"]),
        "min_parameter_stability": float(ranking["min_parameter_stability"]),
        "min_slippage_score": float(ranking["min_slippage_score"]),
        "selection_policy": str(projection["selection_policy"]),
        "max_candidates_per_partition": int(projection["max_candidates_per_partition"]),
        "min_robust_score": float(projection["min_robust_score"]),
        "decision_lag_bars_max": int(projection["decision_lag_bars_max"]),
        "reuse_existing_materialization": reuse_existing_materialization,
    }


def _build_validation_plan(normalized_config: dict[str, Any]) -> dict[str, object]:
    validation = dict(normalized_config.get("validation", {}))
    if not validation:
        return {}
    return _build_validation_plan_from_parts(
        validation=validation,
        dataset=dict(normalized_config["dataset"]),
        backtest=dict(normalized_config["backtest"]),
    )


def _build_validation_plan_from_parts(
    *,
    validation: dict[str, Any],
    dataset: dict[str, Any],
    backtest: dict[str, Any],
) -> dict[str, object]:
    if str(validation.get("scheme", "")) != NESTED_WALK_FORWARD_SCHEME:
        raise ValueError(f"unsupported validation.scheme `{validation.get('scheme', '')}`")
    leakage = dict(validation.get("leakage_controls", {}))
    outer = dict(validation.get("outer_folds", {}))
    inner = dict(validation.get("inner_folds", {}))
    plan = build_nested_walk_forward_plan(
        start_ts=str(dataset.get("start_ts") or ""),
        end_ts=str(dataset.get("end_ts") or ""),
        backtest_timeframe=str(backtest.get("backtest_timeframe") or ""),
        warmup_bars=int(leakage.get("warmup_bars", dataset.get("warmup_bars", 0))),
        purge_bars=int(leakage["purge_bars"]) if leakage.get("purge_bars") is not None else None,
        embargo_bars=int(leakage["embargo_bars"])
        if leakage.get("embargo_bars") is not None
        else None,
        outer_train_months=int(outer.get("train_months", 18)),
        outer_confirmation_months=int(outer.get("confirmation_months", 3)),
        outer_step_months=int(outer.get("step_months", 3)),
        inner_train_months=int(inner.get("train_months", 9)),
        inner_validation_months=int(inner.get("validation_months", 3)),
        inner_step_months=int(inner.get("step_months", 3)),
    )
    _assert_validation_plan_safe(plan)
    return plan


def _assert_validation_plan_safe(plan: dict[str, object]) -> None:
    windows = plan.get("windows", [])
    if not isinstance(windows, list):
        raise ValueError("validation plan windows must be a list")
    inner_by_outer: dict[str, list[dict[str, object]]] = {}
    for row in windows:
        if not isinstance(row, dict):
            raise ValueError("validation plan windows must be objects")
        fold_role = str(row.get("fold_role", ""))
        if fold_role == "confirmation" and bool(row.get("optimizer_visible", True)):
            raise ValueError("confirmation folds must not be optimizer-visible")
        score_start = str(row.get("score_start_ts", ""))
        score_end = str(row.get("score_end_ts", ""))
        purge_start = str(row.get("purge_start_ts", ""))
        purge_end = str(row.get("purge_end_ts", ""))
        embargo_start = str(row.get("embargo_start_ts", ""))
        embargo_end = str(row.get("embargo_end_ts", ""))
        if not (score_start and score_end and score_start < score_end):
            raise ValueError("validation score window must be non-empty")
        if purge_start and purge_end and purge_end > score_start:
            raise ValueError("purge window must end before scoring starts")
        if embargo_start and embargo_end and embargo_start < score_end:
            raise ValueError("embargo window must start after scoring ends")
        if fold_role == "optimization_validation":
            inner_by_outer.setdefault(str(row.get("outer_fold_id", "")), []).append(row)
    for outer_fold_id, outer_rows in inner_by_outer.items():
        previous_score_end = ""
        previous_embargo_end = ""
        for row in sorted(outer_rows, key=lambda item: str(item.get("score_start_ts", ""))):
            score_start = str(row.get("score_start_ts", ""))
            if previous_score_end and score_start < previous_score_end:
                raise ValueError(f"validation score windows overlap in {outer_fold_id}")
            if previous_embargo_end and score_start < previous_embargo_end:
                raise ValueError(
                    f"validation embargo overlaps the next scoring window in {outer_fold_id}"
                )
            previous_score_end = str(row.get("score_end_ts", ""))
            previous_embargo_end = str(row.get("embargo_end_ts", ""))


def _build_terminal_summary(
    *,
    repo_root: Path,
    run_id: str,
    campaign_id: str,
    campaign_run_id: str,
    campaign_name: str,
    target_stage: str,
    canonical_output_dir: str,
    registry_root: str,
    materialization_key: str,
    strategy_space_id: str,
    materialized_root: str,
    run_root: str,
    dataset_version: str,
    config_fingerprint: str,
    executed_steps: list[str],
    reused_steps: list[str],
    durations: dict[str, Any],
    rows_by_table: dict[str, Any],
    output_paths: dict[str, Any],
    dagster_selected_assets: list[str],
    dagster_materialized_assets: list[str],
    warnings: list[str],
    status: str,
    error: dict[str, str] | None,
    result_digest: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = {
        "run_id": run_id,
        "campaign_id": campaign_id,
        "campaign_run_id": campaign_run_id,
        "campaign_name": campaign_name,
        "target_stage": target_stage,
        "status": status,
        "canonical_output_dir": canonical_output_dir,
        "registry_root": registry_root,
        "materialization_key": materialization_key,
        "strategy_space_id": strategy_space_id,
        "materialized_root": materialized_root,
        "run_root": run_root,
        "dataset_version": dataset_version,
        "config_fingerprint": config_fingerprint,
        "executed_steps": executed_steps,
        "reused_steps": reused_steps,
        "durations": durations,
        "rows_by_table": rows_by_table,
        "output_paths": output_paths,
        "dagster_selected_assets": dagster_selected_assets,
        "dagster_materialized_assets": dagster_materialized_assets,
        "warnings": warnings,
        "error": error,
        "result_digest": result_digest,
    }
    validate_schema(load_schema(repo_root / RUN_SUMMARY_SCHEMA), summary)
    return summary


def _build_failure_summary(
    *,
    repo_root: Path,
    run_id: str,
    campaign_id: str,
    campaign_run_id: str,
    registry_root: Path,
    campaign_name: str,
    normalized_config: dict[str, Any],
    materialization_key: str,
    strategy_space_id: str,
    materialized_root: Path,
    run_root: Path,
    executed_steps: list[str],
    reused_steps: list[str],
    warnings: list[str],
    status: str,
    exc: Exception,
    started: float,
) -> dict[str, Any]:
    return _build_terminal_summary(
        repo_root=repo_root,
        run_id=run_id,
        campaign_id=campaign_id,
        campaign_run_id=campaign_run_id,
        campaign_name=campaign_name,
        target_stage=str(normalized_config["target_stage"]),
        canonical_output_dir=str(normalized_config["canonical_output_dir"]),
        registry_root=registry_root.as_posix(),
        materialization_key=materialization_key,
        strategy_space_id=strategy_space_id,
        materialized_root=materialized_root.as_posix(),
        run_root=run_root.as_posix(),
        dataset_version=str(normalized_config["dataset"]["dataset_version"]),
        config_fingerprint=build_config_fingerprint(normalized_config),
        executed_steps=executed_steps,
        reused_steps=reused_steps,
        durations={"total_seconds": round(perf_counter() - started, 6)},
        rows_by_table={},
        output_paths={},
        dagster_selected_assets=[],
        dagster_materialized_assets=[],
        warnings=warnings,
        status=status,
        error={"type": type(exc).__name__, "message": str(exc)},
        result_digest=None,
    )


def _write_prevalidation_lock(
    *,
    run_root: Path,
    run_id: str,
    resolved_config_path: Path,
    raw_config: dict[str, Any],
    results_root: Path,
    error: Exception,
    repo_root: Path,
) -> None:
    write_json(
        run_root / "campaign.lock.json",
        {
            "run_id": run_id,
            "config_path": resolved_config_path.as_posix(),
            "config_fingerprint": _raw_config_fingerprint(
                raw_config=raw_config, config_path=resolved_config_path
            ),
            "materialization_key": "",
            "materialized_output_dir": _resolved_optional_path(
                repo_root=repo_root, raw=raw_config.get("materialized_root")
            ),
            "results_output_dir": results_root.as_posix(),
            "reuse_existing_materialization": False,
            "campaign": raw_config,
            "validation_status": "blocked",
            "error": {"type": type(error).__name__, "message": str(error)},
            "created_at": utc_now_iso(),
        },
    )


def _rate(count: object, total_seconds: float) -> float:
    try:
        value = float(count)
    except (TypeError, ValueError):
        value = 0.0
    return round(value / max(total_seconds, 1e-9), 6)


def _build_duration_metrics(
    *,
    total_seconds: float,
    rows_by_table: dict[str, Any],
    results_root: Path | None = None,
) -> dict[str, float]:
    backtest_seconds = _backtest_stage_seconds(results_root) if results_root is not None else None
    throughput_seconds = backtest_seconds or total_seconds
    metrics = {
        "total_seconds": total_seconds,
        "optimizer_trials_per_second": _rate(
            rows_by_table.get("research_optimizer_trials", 0), throughput_seconds
        ),
        "backtest_runs_per_second": _rate(
            rows_by_table.get("research_backtest_runs", 0), throughput_seconds
        ),
        "strategy_stats_per_second": _rate(
            rows_by_table.get("research_strategy_stats", 0), throughput_seconds
        ),
        "trade_records_per_second": _rate(
            rows_by_table.get("research_trade_records", 0), throughput_seconds
        ),
        "order_records_per_second": _rate(
            rows_by_table.get("research_order_records", 0), throughput_seconds
        ),
        "campaign_optimizer_trials_per_second": _rate(
            rows_by_table.get("research_optimizer_trials", 0), total_seconds
        ),
        "campaign_backtest_runs_per_second": _rate(
            rows_by_table.get("research_backtest_runs", 0), total_seconds
        ),
        "campaign_strategy_stats_per_second": _rate(
            rows_by_table.get("research_strategy_stats", 0), total_seconds
        ),
        "campaign_trade_records_per_second": _rate(
            rows_by_table.get("research_trade_records", 0), total_seconds
        ),
        "campaign_order_records_per_second": _rate(
            rows_by_table.get("research_order_records", 0), total_seconds
        ),
        "ranking_rows_per_campaign_second": _rate(
            rows_by_table.get("research_strategy_rankings", 0), total_seconds
        ),
    }
    if backtest_seconds is not None:
        metrics["backtest_duration_seconds"] = round(backtest_seconds, 6)
    return metrics


def _backtest_stage_seconds(results_root: Path) -> float | None:
    batch_path = results_root / "research_backtest_batches.delta"
    if not has_delta_log(batch_path):
        return None
    rows = _read_projected_delta_records(batch_path, columns=["duration_seconds"])
    durations: list[float] = []
    for row in rows:
        try:
            duration = float(row.get("duration_seconds", 0.0) or 0.0)
        except (TypeError, ValueError):
            duration = 0.0
        if duration > 0:
            durations.append(duration)
    if not durations:
        return None
    return sum(durations)


def _optimizer_status_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status", ""))
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _study_config_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(payload, dict):
            return {str(key): item for key, item in payload.items()}
    return {}


def _json_value(value: object) -> object:
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _optimizer_digest(results_root: Path) -> dict[str, Any]:
    studies_path = results_root / "research_optimizer_studies.delta"
    trials_path = results_root / "research_optimizer_trials.delta"
    study_rows = read_delta_table_rows(studies_path) if has_delta_log(studies_path) else []
    trial_rows = read_delta_table_rows(trials_path) if has_delta_log(trials_path) else []
    batch_counts: list[int] = []
    fixed_parameters: dict[str, object] = {}
    observed_unique_value_counts: dict[str, int] = {}
    for row in study_rows:
        config = _study_config_dict(row.get("study_config_json", {}))
        if "ask_tell_batch_count" in config:
            batch_counts.append(int(config.get("ask_tell_batch_count", 0) or 0))
        diagnostics = config.get("parameter_space_diagnostics", {})
        if isinstance(diagnostics, dict):
            raw_fixed = diagnostics.get("fixed_parameters", {})
            if isinstance(raw_fixed, dict):
                fixed_parameters.update({str(key): value for key, value in raw_fixed.items()})
            raw_counts = diagnostics.get("observed_unique_value_counts", {})
            if isinstance(raw_counts, dict):
                for key, value in raw_counts.items():
                    observed_unique_value_counts[str(key)] = max(
                        observed_unique_value_counts.get(str(key), 0),
                        int(value or 0),
                    )
    return {
        "study_count": len(study_rows),
        "trial_count": len(trial_rows),
        "trial_status_counts": _optimizer_status_counts(trial_rows),
        "ask_tell_batch_count_min": min(batch_counts) if batch_counts else 0,
        "ask_tell_batch_count_max": max(batch_counts) if batch_counts else 0,
        "fixed_parameters": dict(sorted(fixed_parameters.items())),
        "observed_unique_value_counts": dict(sorted(observed_unique_value_counts.items())),
    }


def _build_result_digest(
    *,
    target_stage: str,
    results_root: Path,
    duration_metrics: dict[str, float] | None = None,
    rows_by_table: dict[str, Any] | None = None,
    materialized_root: Path | None = None,
    output_paths: dict[str, Any] | None = None,
    executed_steps: Sequence[str] = (),
    reused_steps: Sequence[str] = (),
    force_rematerialize: bool = False,
) -> dict[str, Any] | None:
    data_prep_proof = (
        _build_data_prep_proof_digest(
            materialized_root=materialized_root,
            output_paths=output_paths or {},
            rows_by_table=rows_by_table or {},
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            force_rematerialize=force_rematerialize,
        )
        if materialized_root is not None
        else None
    )
    if target_stage == "data_prep":
        return {"data_prep_proof": data_prep_proof} if data_prep_proof is not None else None

    ranking_path = results_root / "research_strategy_rankings.delta"
    ranking_rows = _read_projected_delta_records(
        ranking_path,
        columns=[
            "strategy_instance_id",
            "family_key",
            "contract_id",
            "timeframe",
            "rank",
            "score_total",
            "objective_score",
            "parameter_stability_score",
            "slippage_sensitivity_score",
            "policy_failure_reasons_json",
            "qualifies_for_projection",
        ],
    )
    evaluation_path = results_root / "research_strategy_evaluation_profiles.delta"
    evaluation_rows = _read_projected_delta_records(
        evaluation_path,
        columns=[
            "evaluation_profile_id",
            "strategy_instance_id",
            "family_key",
            "contract_id",
            "timeframe",
            "verdict",
            "promotion_state",
            "paper_signal_ready",
            "paper_trade_ready",
            "live_candidate_ready",
            "blocker_reasons_json",
            "missing_data_json",
            "evidence_snapshot_json",
        ],
    )
    ranking_sorted = sorted(
        ranking_rows,
        key=lambda row: (
            float(row.get("score_total", 0.0) or 0.0),
            float(row.get("objective_score", 0.0) or 0.0),
            -int(row.get("rank", 0)),
        ),
        reverse=True,
    )
    ranking_top_rows = [_digest_ranking_row(row) for row in ranking_sorted[:5]]
    projection_eligible_top_rows = [
        _digest_ranking_row(row)
        for row in ranking_sorted
        if bool(row.get("qualifies_for_projection", False))
    ][:5]
    evaluation_by_verdict: dict[str, int] = {}
    for row in evaluation_rows:
        verdict = str(row.get("verdict", ""))
        evaluation_by_verdict[verdict] = evaluation_by_verdict.get(verdict, 0) + 1
    digest: dict[str, Any] = {
        "runtime_metrics": duration_metrics or {},
        "rows_by_table": dict(rows_by_table or {}),
        "optimizer": _optimizer_digest(results_root),
        "projection_qualified_count": sum(
            1 for row in ranking_rows if bool(row.get("qualifies_for_projection", False))
        ),
        "strategy_evaluation_count": len(evaluation_rows),
        "strategy_evaluations_by_verdict": dict(sorted(evaluation_by_verdict.items())),
        "paper_signal_ready_count": sum(
            1 for row in evaluation_rows if bool(row.get("paper_signal_ready", False))
        ),
        "paper_trade_ready_count": sum(
            1 for row in evaluation_rows if bool(row.get("paper_trade_ready", False))
        ),
        "live_candidate_ready_count": sum(
            1 for row in evaluation_rows if bool(row.get("live_candidate_ready", False))
        ),
        "strategy_evaluation_top_rows": [
            _digest_strategy_evaluation(row) for row in evaluation_rows[:5]
        ],
        "ranking_top_rows": ranking_top_rows,
        "best_overall_rows": ranking_top_rows,
        "projection_eligible_top_rows": projection_eligible_top_rows,
    }
    if data_prep_proof is not None:
        digest["data_prep_proof"] = data_prep_proof
    if target_stage == "projection":
        candidate_path = results_root / "research_signal_candidates.delta"
        candidate_rows = _read_projected_delta_records(
            candidate_path,
            columns=["strategy_instance_id", "timeframe"],
        )
        by_strategy: dict[str, int] = {}
        by_timeframe: dict[str, int] = {}
        for row in candidate_rows:
            strategy = str(row.get("strategy_instance_id", ""))
            timeframe = str(row.get("timeframe", ""))
            by_strategy[strategy] = by_strategy.get(strategy, 0) + 1
            by_timeframe[timeframe] = by_timeframe.get(timeframe, 0) + 1
        digest["candidate_count"] = len(candidate_rows)
        digest["candidate_rows_by_strategy"] = dict(sorted(by_strategy.items()))
        digest["candidate_rows_by_timeframe"] = dict(sorted(by_timeframe.items()))
    return digest


def _digest_ranking_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_instance_id": str(row.get("strategy_instance_id", "")),
        "family_key": str(row.get("family_key", "")),
        "contract_id": str(row.get("contract_id", "")),
        "timeframe": str(row.get("timeframe", "")),
        "rank": int(row.get("rank", 0)),
        "score_total": round(float(row.get("score_total", 0.0) or 0.0), 6),
        "objective_score": round(float(row.get("objective_score", 0.0) or 0.0), 6),
        "parameter_stability_score": round(
            float(row.get("parameter_stability_score", 0.0) or 0.0), 6
        ),
        "slippage_sensitivity_score": round(
            float(row.get("slippage_sensitivity_score", 0.0) or 0.0), 6
        ),
        "qualifies_for_projection": bool(row.get("qualifies_for_projection", False)),
        "policy_failure_reasons": _json_value(row.get("policy_failure_reasons_json", ())),
    }


def _digest_strategy_evaluation(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "evaluation_profile_id": str(row.get("evaluation_profile_id", "")),
        "strategy_instance_id": str(row.get("strategy_instance_id", "")),
        "family_key": str(row.get("family_key", "")),
        "contract_id": str(row.get("contract_id", "")),
        "timeframe": str(row.get("timeframe", "")),
        "verdict": str(row.get("verdict", "")),
        "promotion_state": str(row.get("promotion_state", "")),
        "paper_signal_ready": bool(row.get("paper_signal_ready", False)),
        "paper_trade_ready": bool(row.get("paper_trade_ready", False)),
        "live_candidate_ready": bool(row.get("live_candidate_ready", False)),
        "blocker_reasons": _json_value(row.get("blocker_reasons_json", ())),
        "missing_data": _json_value(row.get("missing_data_json", ())),
        "evidence_snapshot": _json_value(row.get("evidence_snapshot_json", {})),
    }


def _build_data_prep_proof_digest(
    *,
    materialized_root: Path,
    output_paths: dict[str, Any],
    rows_by_table: dict[str, Any],
    executed_steps: Sequence[str],
    reused_steps: Sequence[str],
    force_rematerialize: bool,
) -> dict[str, Any]:
    data_prep_executed = "research_data_prep" in set(executed_steps)
    data_prep_reused = "research_data_prep" in set(reused_steps)
    if data_prep_executed and force_rematerialize:
        mode = "forced_refresh"
    elif data_prep_executed:
        mode = "fresh_materialization"
    elif data_prep_reused:
        mode = "reused_materialization"
    else:
        mode = "not_requested"

    tables: dict[str, dict[str, Any]] = {}
    for table_name in DATA_PREP_TABLES:
        raw_path = output_paths.get(table_name)
        table_path = Path(str(raw_path)) if raw_path else materialized_root / f"{table_name}.delta"
        tables[table_name] = {
            "path": table_path.as_posix(),
            "row_count": int(rows_by_table.get(table_name, 0) or 0),
            "has_delta_log": has_delta_log(table_path),
        }
    return {
        "mode": mode,
        "force_rematerialize": bool(force_rematerialize),
        "materialized_root": materialized_root.as_posix(),
        "tables": tables,
    }


def _read_projected_delta_records(path: Path, *, columns: list[str]) -> list[dict[str, Any]]:
    if not has_delta_log(path):
        return []
    frame = read_delta_table_frame(path, columns=columns)
    return [{str(key): value for key, value in row.items()} for row in frame.to_dict("records")]


def _publish_staged_results(
    *,
    staging_results_root: Path,
    final_results_root: Path,
    output_paths: dict[str, object],
) -> dict[str, str]:
    final_results_root.mkdir(parents=True, exist_ok=True)
    published_paths: dict[str, str] = {}
    resolved_staging_root = staging_results_root.resolve()
    for table_name, raw_path in output_paths.items():
        source = Path(str(raw_path))
        if not source.exists():
            continue
        try:
            source.resolve().relative_to(resolved_staging_root)
        except ValueError:
            published_paths[table_name] = source.as_posix()
            continue
        target = final_results_root / source.name
        if target.exists():
            shutil.rmtree(target)
        shutil.move(source.as_posix(), target.as_posix())
        published_paths[table_name] = target.as_posix()
    return published_paths


def _write_publish_commit(
    *,
    final_results_root: Path,
    campaign_run_id: str,
    published_output_paths: dict[str, str],
    registry_output_paths: dict[str, str],
) -> None:
    write_json(
        final_results_root / "publish-commit.json",
        {
            "campaign_run_id": campaign_run_id,
            "published_at": utc_now_iso(),
            "tables": sorted(published_output_paths),
            "registry_tables": sorted(registry_output_paths),
        },
    )


def _campaign_registry_output_paths(*, registry_root: Path) -> dict[str, str]:
    all_paths = registry_output_paths(registry_root=registry_root)
    return {
        table_name: all_paths[table_name]
        for table_name in (
            "research_campaigns",
            "research_campaign_runs",
            "research_strategy_notes",
        )
    }


def _quarantine_uncommitted_results(*, final_results_root: Path, run_root: Path) -> None:
    if not final_results_root.exists() or (final_results_root / "publish-commit.json").exists():
        return
    quarantine_root = run_root / "results-quarantine"
    if quarantine_root.exists():
        shutil.rmtree(quarantine_root)
    shutil.move(final_results_root.as_posix(), quarantine_root.as_posix())


def _record_failed_campaign_run(
    *,
    registry_root: Path,
    campaign_run_id: str,
    campaign_id: str,
    campaign_name: str,
    target_stage: str,
    config_fingerprint: str,
    strategy_space_id: str,
    materialization_key: str,
    materialized_root: Path,
    final_results_root: Path,
    reuse_existing_materialization: bool,
    executed_steps: list[str],
    reused_steps: list[str],
    warnings: list[str],
    status: str,
    exc: Exception,
) -> None:
    try:
        write_campaign_run(
            registry_root=registry_root,
            campaign_run_id=campaign_run_id,
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            target_stage=target_stage,
            config_fingerprint=config_fingerprint,
            strategy_space_id=strategy_space_id,
            materialization_key=materialization_key,
            materialized_output_dir=materialized_root.as_posix(),
            results_output_dir=final_results_root.as_posix(),
            reuse_existing_materialization=reuse_existing_materialization,
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            rows_by_table={},
            output_paths={},
            warnings=warnings,
            status=status,
            started_at=utc_now_iso(),
            finished_at=utc_now_iso(),
            result_digest={"error": str(exc)},
        )
    except Exception:  # noqa: BLE001
        return


def _publish_global_indices(*, registry_root: Path, results_root: Path) -> dict[str, str]:
    stats_path = results_root / "research_strategy_stats.delta"
    ranking_path = results_root / "research_strategy_rankings.delta"
    published_paths: dict[str, str] = {}
    if has_delta_log(stats_path):
        append_run_stats_index(
            registry_root=registry_root,
            rows=[
                {
                    "campaign_run_id": row["campaign_run_id"],
                    "backtest_run_id": row["backtest_run_id"],
                    "strategy_instance_id": row["strategy_instance_id"],
                    "strategy_template_id": row["strategy_template_id"],
                    "family_id": row["family_id"],
                    "family_key": row["family_key"],
                    "dataset_version": row["dataset_version"],
                    "instrument_id": row["instrument_id"],
                    "contract_id": row["contract_id"],
                    "timeframe": row["timeframe"],
                    "window_id": row["window_id"],
                    "total_return": row["total_return"],
                    "sharpe": row["sharpe"],
                    "sortino": row["sortino"],
                    "calmar": row["calmar"],
                    "max_drawdown": row["max_drawdown"],
                    "profit_factor": row["profit_factor"],
                    "win_rate": row["win_rate"],
                    "trade_count": row["trade_count"],
                    "turnover": row["turnover"],
                    "commission_total": row["commission_total"],
                    "slippage_total": row["slippage_total"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                }
                for row in _read_projected_delta_records(
                    stats_path,
                    columns=[
                        "campaign_run_id",
                        "backtest_run_id",
                        "strategy_instance_id",
                        "strategy_template_id",
                        "family_id",
                        "family_key",
                        "dataset_version",
                        "instrument_id",
                        "contract_id",
                        "timeframe",
                        "window_id",
                        "total_return",
                        "sharpe",
                        "sortino",
                        "calmar",
                        "max_drawdown",
                        "profit_factor",
                        "win_rate",
                        "trade_count",
                        "turnover",
                        "commission_total",
                        "slippage_total",
                        "status",
                        "created_at",
                    ],
                )
            ],
        )
        published_paths["research_run_stats_index"] = (
            registry_root / "research_run_stats_index.delta"
        ).as_posix()
    if has_delta_log(ranking_path):
        append_rankings_index(
            registry_root=registry_root,
            rows=[
                {
                    "campaign_run_id": row["campaign_run_id"],
                    "ranking_id": row["ranking_id"],
                    "strategy_instance_id": row["strategy_instance_id"],
                    "backtest_run_id": row["backtest_run_id"],
                    "family_id": row["family_id"],
                    "family_key": row["family_key"],
                    "dataset_version": row["dataset_version"],
                    "timeframe": row["timeframe"],
                    "rank": row["rank"],
                    "objective_score": row["objective_score"],
                    "score_total": row["score_total"],
                    "qualifies_for_projection": row["qualifies_for_projection"],
                    "ranking_policy_version": row["ranking_policy_id"],
                    "rank_reason_json": row["rank_reason_json"],
                    "created_at": row["created_at"],
                }
                for row in _read_projected_delta_records(
                    ranking_path,
                    columns=[
                        "campaign_run_id",
                        "ranking_id",
                        "strategy_instance_id",
                        "backtest_run_id",
                        "family_id",
                        "family_key",
                        "dataset_version",
                        "timeframe",
                        "rank",
                        "objective_score",
                        "score_total",
                        "qualifies_for_projection",
                        "ranking_policy_id",
                        "rank_reason_json",
                        "created_at",
                    ],
                )
            ],
        )
        published_paths["research_rankings_index"] = (
            registry_root / "research_rankings_index.delta"
        ).as_posix()
    return published_paths


def _build_artifacts_index(
    output_paths: dict[str, Any], rows_by_table: dict[str, Any]
) -> dict[str, Any]:
    artifacts: list[dict[str, Any]] = []
    for name, raw_path in sorted(output_paths.items()):
        path = Path(str(raw_path))
        artifacts.append(
            {
                "name": name,
                "path": path.as_posix(),
                "row_count": int(rows_by_table.get(name, 0)),
                "size_bytes": _dir_size(path),
            }
        )
    return {"artifacts": artifacts}


def _prepare_run_root(
    *, repo_root: Path, runs_root_raw: str, campaign_name: str, run_id: str
) -> Path:
    runs_root = resolve_path(repo_root=repo_root, raw=runs_root_raw)
    run_root = runs_root / campaign_name / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    (run_root / "logs").mkdir(parents=True, exist_ok=True)
    return run_root


def _prepare_provisional_run_root(
    *,
    repo_root: Path,
    raw_config: dict[str, Any],
    run_id: str,
) -> tuple[Path, str, list[str]]:
    warnings: list[str] = []
    campaign_name = _raw_campaign_name(raw_config)
    runs_root = _blocked_runs_root(repo_root=repo_root, raw_config=raw_config)
    if runs_root == (repo_root / "artifacts" / "research-campaign-invalid").resolve():
        warnings.append(
            "runs_root missing or invalid; blocked evidence stored under repo-local blocked root"
        )
    run_root = runs_root / campaign_name / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    (run_root / "logs").mkdir(parents=True, exist_ok=True)
    return run_root, campaign_name, warnings


def _write_terminal_artifacts(*, run_root: Path, summary: dict[str, Any]) -> None:
    write_json(run_root / "run-summary.json", summary)


def _write_status(path: Path, *, run_id: str, campaign_name: str, status: str) -> None:
    if status not in STATUS_VALUES:
        raise CampaignBlockedError(f"unsupported campaign status `{status}`")
    write_json(
        path,
        {
            "run_id": run_id,
            "campaign_name": campaign_name,
            "status": status,
            "updated_at": utc_now_iso(),
        },
    )


def _write_logs(*, run_root: Path, stdout_lines: list[str], stderr_lines: list[str]) -> None:
    (run_root / "logs" / "stdout.log").write_text(
        ("\n".join(stdout_lines).strip() + "\n") if stdout_lines else "",
        encoding="utf-8",
    )
    (run_root / "logs" / "stderr.log").write_text(
        ("\n".join(stderr_lines).strip() + "\n") if stderr_lines else "",
        encoding="utf-8",
    )


def _normalize_dataset(*, payload: dict[str, Any]) -> dict[str, Any]:
    timeframes = _sorted_unique_strings(payload["timeframes"], sort_key=_timeframe_sort_key)
    continuous_front_policy = ContinuousFrontPolicy.from_config(
        dict(payload.get("continuous_front_policy") or payload.get("continuous_front") or {})
    ).to_config_dict()
    return {
        "dataset_version": _normalized_non_empty(payload["dataset_version"]),
        "dataset_name": _normalized_non_empty(payload["dataset_name"]),
        "universe_id": _normalized_non_empty(payload["universe_id"]),
        "series_mode": _normalized_enum(
            payload["series_mode"], {"contract", "continuous_front"}, field="dataset.series_mode"
        ),
        "continuous_front_policy": continuous_front_policy,
        "timeframes": timeframes,
        "base_timeframe": _normalized_non_empty(payload["base_timeframe"]),
        "start_ts": _normalized_optional_string(payload.get("start_ts")),
        "end_ts": _normalized_optional_string(payload.get("end_ts")),
        "warmup_bars": _normalized_non_negative_int(
            payload["warmup_bars"], field="dataset.warmup_bars"
        ),
        "split_method": _normalized_enum(
            payload["split_method"],
            {"full", "holdout", "walk_forward"},
            field="dataset.split_method",
        ),
        "contract_ids": _sorted_unique_strings(payload["contract_ids"]),
        "instrument_ids": _sorted_unique_strings(payload["instrument_ids"]),
    }


def _normalize_profiles(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "indicator_set_version": _normalized_non_empty(payload["indicator_set_version"]),
        "indicator_profile_version": _normalized_non_empty(payload["indicator_profile_version"]),
        "derived_indicator_set_version": _normalized_non_empty(
            payload["derived_indicator_set_version"]
        ),
        "derived_indicator_profile_version": _normalized_non_empty(
            payload["derived_indicator_profile_version"]
        ),
    }


def _normalize_volume_profile(payload: object, *, repo_root: Path) -> dict[str, Any]:
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise CampaignBlockedError("volume_profile must be an object")
    raw_1m_table_path = (
        _resolved_optional_path(repo_root=repo_root, raw=payload.get("raw_1m_table_path")) or None
    )
    raw_tick_sizes = payload.get("tick_size_by_instrument", {})
    if raw_tick_sizes is None:
        raw_tick_sizes = {}
    if not isinstance(raw_tick_sizes, dict):
        raise CampaignBlockedError("volume_profile.tick_size_by_instrument must be an object")
    tick_size_by_instrument: dict[str, float] = {}
    for raw_instrument_id, raw_tick_size in sorted(
        raw_tick_sizes.items(), key=lambda item: str(item[0])
    ):
        instrument_id = _normalized_non_empty(raw_instrument_id)
        try:
            tick_size = float(raw_tick_size)
        except (TypeError, ValueError) as exc:
            raise CampaignBlockedError(
                "volume_profile.tick_size_by_instrument values must be numeric"
            ) from exc
        if not math.isfinite(tick_size) or tick_size <= 0:
            raise CampaignBlockedError(
                "volume_profile.tick_size_by_instrument values must be finite and > 0"
            )
        tick_size_by_instrument[instrument_id] = tick_size
    return {
        "raw_1m_table_path": raw_1m_table_path,
        "tick_size_by_instrument": tick_size_by_instrument,
    }


def _normalize_validation(
    payload: object,
    *,
    dataset: dict[str, Any],
    backtest: dict[str, Any],
) -> dict[str, Any]:
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise CampaignBlockedError("validation must be an object")

    scheme = _normalized_enum(
        payload.get("scheme", NESTED_WALK_FORWARD_SCHEME),
        {NESTED_WALK_FORWARD_SCHEME},
        field="validation.scheme",
    )
    outer_folds = _optional_dict(payload.get("outer_folds"), field="validation.outer_folds")
    inner_folds = _optional_dict(payload.get("inner_folds"), field="validation.inner_folds")
    leakage_controls = _optional_dict(
        payload.get("leakage_controls"), field="validation.leakage_controls"
    )
    verdicts = _optional_dict(payload.get("verdicts"), field="validation.verdicts")

    normalized = {
        "scheme": scheme,
        "outer_folds": {
            "train_months": _normalized_positive_int(
                outer_folds.get("train_months", 18),
                field="validation.outer_folds.train_months",
            ),
            "confirmation_months": _normalized_positive_int(
                outer_folds.get("confirmation_months", 3),
                field="validation.outer_folds.confirmation_months",
            ),
            "step_months": _normalized_positive_int(
                outer_folds.get("step_months", 3),
                field="validation.outer_folds.step_months",
            ),
        },
        "inner_folds": {
            "train_months": _normalized_positive_int(
                inner_folds.get("train_months", 9),
                field="validation.inner_folds.train_months",
            ),
            "validation_months": _normalized_positive_int(
                inner_folds.get("validation_months", 3),
                field="validation.inner_folds.validation_months",
            ),
            "step_months": _normalized_positive_int(
                inner_folds.get("step_months", 3),
                field="validation.inner_folds.step_months",
            ),
        },
        "leakage_controls": {
            "warmup_bars": _normalized_non_negative_int(
                leakage_controls.get("warmup_bars", dataset["warmup_bars"]),
                field="validation.leakage_controls.warmup_bars",
            ),
            "purge_bars": _normalized_non_negative_int(
                leakage_controls.get("purge_bars"),
                field="validation.leakage_controls.purge_bars",
            ),
            "embargo_bars": _normalized_non_negative_int(
                leakage_controls.get("embargo_bars"),
                field="validation.leakage_controls.embargo_bars",
            ),
        },
        "verdicts": {
            "walk_forward_reoptimization": bool(verdicts.get("walk_forward_reoptimization", True)),
            "latest_frozen_param_confirmation": bool(
                verdicts.get("latest_frozen_param_confirmation", True)
            ),
        },
    }
    try:
        _build_validation_plan_from_parts(
            validation=normalized,
            dataset=dataset,
            backtest=backtest,
        )
    except ValueError as exc:
        raise CampaignBlockedError(str(exc)) from exc
    return normalized


def _optional_dict(value: object, *, field: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise CampaignBlockedError(f"{field} must be an object")
    return {str(key): item for key, item in value.items()}


def _normalize_strategy_space(payload: dict[str, Any]) -> dict[str, Any]:
    retired_fields = sorted(
        field
        for field in (
            "include_instance_ids",
            "exclude_manifest_hashes",
            "materialize_instances",
            "max_instance_count",
        )
        if field in payload
    )
    if retired_fields:
        raise CampaignBlockedError(
            "strategy_space uses retired per-instance fields; "
            "use family/template search fields instead: " + ", ".join(retired_fields)
        )
    search_space_overrides = payload.get("search_space_overrides", {})
    if not isinstance(search_space_overrides, dict):
        raise CampaignBlockedError("strategy_space.search_space_overrides must be an object")
    normalized_overrides: dict[str, dict[str, list[object]]] = {}
    for selector, raw_override in search_space_overrides.items():
        if not isinstance(raw_override, dict):
            raise CampaignBlockedError(
                "strategy_space.search_space_overrides entries must be objects"
            )
        normalized_overrides[str(selector)] = {
            str(param): list(values if isinstance(values, list) else [values])
            for param, values in raw_override.items()
        }
    family_keys = _sorted_unique_strings(payload["family_keys"])
    template_ids = _sorted_unique_strings(payload["template_ids"])
    if not family_keys and not template_ids:
        raise CampaignBlockedError("strategy_space must include family_keys or template_ids")
    normalized = {
        "family_keys": family_keys,
        "template_ids": template_ids,
        "exclude_template_manifest_hashes": _sorted_unique_strings(
            payload["exclude_template_manifest_hashes"]
        ),
        "max_parameter_combinations": _normalized_positive_int(
            payload["max_parameter_combinations"],
            field="strategy_space.max_parameter_combinations",
        ),
        "search_space_overrides": normalized_overrides,
    }
    if "optimizer" in payload:
        optimizer = payload["optimizer"]
        if not isinstance(optimizer, dict):
            raise CampaignBlockedError("strategy_space.optimizer must be an object")
        normalized["optimizer"] = _normalize_strategy_optimizer(
            {str(key): value for key, value in optimizer.items()}
        )
    return normalized


def _normalize_strategy_optimizer(payload: dict[str, Any]) -> dict[str, Any]:
    engine = _normalized_enum(
        payload.get("engine", "grid"), {"grid", "optuna"}, field="strategy_space.optimizer.engine"
    )
    if engine == "grid":
        return {"engine": "grid"}
    normalized = {
        "engine": "optuna",
        "sampler": _normalized_enum(
            payload.get("sampler", "tpe"), {"tpe"}, field="strategy_space.optimizer.sampler"
        ),
        "seed": _normalized_non_negative_int(
            payload.get("seed", 0), field="strategy_space.optimizer.seed"
        ),
        "n_trials": _normalized_positive_int(
            payload.get("n_trials", 0), field="strategy_space.optimizer.n_trials"
        ),
        "objective": _normalized_enum(
            payload.get("objective", "robust_oos_trial_v1"),
            {"robust_oos_trial_v1"},
            field="strategy_space.optimizer.objective",
        ),
        "direction": _normalized_enum(
            payload.get("direction", "maximize"),
            {"maximize"},
            field="strategy_space.optimizer.direction",
        ),
        "top_k": _normalized_positive_int(
            payload.get("top_k", 8), field="strategy_space.optimizer.top_k"
        ),
        "radius": _normalized_non_negative_int(
            payload.get("radius", 1), field="strategy_space.optimizer.radius"
        ),
        "max_neighborhood_trials": _normalized_non_negative_int(
            payload.get("max_neighborhood_trials", 64),
            field="strategy_space.optimizer.max_neighborhood_trials",
        ),
    }
    if "ranking_policy" in payload:
        raw_policy = payload["ranking_policy"]
        if not isinstance(raw_policy, dict):
            raise CampaignBlockedError(
                "`strategy_space.optimizer.ranking_policy` must be an object"
            )
        normalized["ranking_policy"] = _normalize_ranking_policy_core(
            {str(item_key): item for item_key, item in raw_policy.items()},
            field="strategy_space.optimizer.ranking_policy",
        )
    return normalized


def _normalize_backtest(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "param_batch_size": _normalized_positive_int(
            payload["param_batch_size"], field="backtest.param_batch_size"
        ),
        "series_batch_size": _normalized_positive_int(
            payload["series_batch_size"], field="backtest.series_batch_size"
        ),
        "backtest_timeframe": _normalized_non_empty(payload["backtest_timeframe"]),
        "fees_bps": _normalized_non_negative_number(payload["fees_bps"], field="backtest.fees_bps"),
        "slippage_bps": _normalized_non_negative_number(
            payload["slippage_bps"], field="backtest.slippage_bps"
        ),
        "allow_short": bool(payload["allow_short"]),
        "window_count": _normalized_positive_int(
            payload["window_count"], field="backtest.window_count"
        ),
    }


def _normalize_ranking_policy_core(payload: dict[str, Any], *, field: str) -> dict[str, Any]:
    metric_order = _unique_strings_preserve_order(payload["metric_order"])
    if not metric_order:
        raise CampaignBlockedError(f"{field}.metric_order must not be empty")
    return {
        "policy_id": _normalized_non_empty(payload["policy_id"]),
        "metric_order": metric_order,
        "require_out_of_sample_pass": bool(payload["require_out_of_sample_pass"]),
        "min_trade_count": _normalized_positive_int(
            payload["min_trade_count"], field=f"{field}.min_trade_count"
        ),
        "min_trade_count_per_fold": _normalized_positive_int(
            payload["min_trade_count_per_fold"],
            field=f"{field}.min_trade_count_per_fold",
        ),
        "min_fold_count": _normalized_positive_int(
            payload["min_fold_count"],
            field=f"{field}.min_fold_count",
        ),
        "max_drawdown_cap": _normalized_non_negative_number(
            payload["max_drawdown_cap"], field=f"{field}.max_drawdown_cap"
        ),
        "min_positive_fold_ratio": _normalized_ratio(
            payload["min_positive_fold_ratio"], field=f"{field}.min_positive_fold_ratio"
        ),
        "stress_slippage_bps": _normalized_non_negative_number(
            payload["stress_slippage_bps"], field=f"{field}.stress_slippage_bps"
        ),
        "min_parameter_stability": _normalized_ratio(
            payload["min_parameter_stability"], field=f"{field}.min_parameter_stability"
        ),
        "min_slippage_score": _normalized_ratio(
            payload["min_slippage_score"], field=f"{field}.min_slippage_score"
        ),
    }


def _normalize_ranking_policy(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_ranking_policy_core(payload, field="ranking_policy")


def _normalize_projection_policy(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "selection_policy": _normalized_non_empty(payload["selection_policy"]),
        "max_candidates_per_partition": _normalized_positive_int(
            payload["max_candidates_per_partition"],
            field="projection_policy.max_candidates_per_partition",
        ),
        "min_robust_score": _normalized_ratio(
            payload["min_robust_score"], field="projection_policy.min_robust_score"
        ),
        "decision_lag_bars_max": _normalized_non_negative_int(
            payload["decision_lag_bars_max"],
            field="projection_policy.decision_lag_bars_max",
        ),
    }


def _normalize_execution(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "force_rematerialize": bool(payload["force_rematerialize"]),
        "raise_on_error": bool(payload["raise_on_error"]),
        "continuous_front_indicator_qc_mode": _normalized_enum(
            payload.get("continuous_front_indicator_qc_mode", "hot_path"),
            {"hot_path", "audit"},
            field="execution.continuous_front_indicator_qc_mode",
        ),
        "continuous_front_indicator_sidecar_materialization_mode": _normalized_enum(
            payload.get("continuous_front_indicator_sidecar_materialization_mode", "auto"),
            {"auto", "spark"},
            field="execution.continuous_front_indicator_sidecar_materialization_mode",
        ),
        "spark_master": str(payload.get("spark_master") or ""),
    }


def _expect_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise CampaignBlockedError(f"`{key}` must be an object")
    return {str(item_key): item for item_key, item in value.items()}


def _normalized_non_empty(value: object) -> str:
    text = str(value).strip()
    if not text:
        raise CampaignBlockedError("expected non-empty string")
    return text


def _raw_campaign_name(raw_config: dict[str, Any]) -> str:
    candidate = str(raw_config.get("campaign_name", "")).strip()
    if not candidate:
        return "invalid-campaign"
    return candidate.replace("\\", "_").replace("/", "_")


def _blocked_runs_root(*, repo_root: Path, raw_config: dict[str, Any]) -> Path:
    raw = raw_config.get("runs_root")
    if isinstance(raw, str) and raw.strip():
        try:
            return resolve_path(repo_root=repo_root, raw=raw.strip())
        except Exception:  # noqa: BLE001
            pass
    return (repo_root / "artifacts" / "research-campaign-invalid").resolve()


def _resolved_optional_path(*, repo_root: Path, raw: object) -> str:
    if not isinstance(raw, str) or not raw.strip():
        return ""
    try:
        return resolve_path(repo_root=repo_root, raw=raw.strip()).as_posix()
    except Exception:  # noqa: BLE001
        return raw.strip()


def _raw_target_stage(raw_config: dict[str, Any]) -> str:
    candidate = str(raw_config.get("target_stage", "")).strip()
    return candidate or "unknown"


def _raw_dataset_version(raw_config: dict[str, Any]) -> str:
    dataset = raw_config.get("dataset")
    if isinstance(dataset, dict):
        return str(dataset.get("dataset_version", "")).strip()
    return ""


def _raw_config_fingerprint(*, raw_config: dict[str, Any], config_path: Path) -> str:
    if raw_config:
        return _stable_hash(raw_config)
    return _stable_hash({"config_path": config_path.as_posix()})


def _normalized_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalized_enum(value: object, allowed: set[str], *, field: str) -> str:
    text = _normalized_non_empty(value)
    if text not in allowed:
        raise CampaignBlockedError(f"{field} must be one of: {', '.join(sorted(allowed))}")
    return text


def _normalized_positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CampaignBlockedError(f"{field} must be a positive integer")
    return int(value)


def _normalized_non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CampaignBlockedError(f"{field} must be a non-negative integer")
    return int(value)


def _normalized_non_negative_number(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) < 0.0:
        raise CampaignBlockedError(f"{field} must be a non-negative number")
    return float(value)


def _normalized_ratio(value: object, *, field: str) -> float:
    normalized = _normalized_non_negative_number(value, field=field)
    if normalized > 1.0:
        raise CampaignBlockedError(f"{field} must be between 0.0 and 1.0")
    return normalized


def _sorted_unique_strings(values: object, *, sort_key: Any = None) -> list[str]:
    if not isinstance(values, list):
        raise CampaignBlockedError("expected list of strings")
    return sorted({str(item).strip() for item in values if str(item).strip()}, key=sort_key)


def _unique_strings_preserve_order(values: object) -> list[str]:
    if not isinstance(values, list):
        raise CampaignBlockedError("expected list of strings")
    result: list[str] = []
    for value in values:
        item = str(value).strip()
        if item and item not in result:
            result.append(item)
    return result


def _timeframe_sort_key(value: str) -> tuple[int, str]:
    if value in TIMEFRAME_ORDER:
        return TIMEFRAME_ORDER[value], value
    return 1_000_000, value


def _stable_hash(payload: object) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _materialization_lock_path(materialized_root: Path) -> Path:
    return materialized_root / MATERIALIZATION_LOCK_FILENAME


def _read_materialization_lock(materialized_root: Path) -> dict[str, object] | None:
    lock_path = _materialization_lock_path(materialized_root)
    if not lock_path.exists():
        return None
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _has_reusable_materialization(
    materialized_root: Path,
    *,
    materialization_key: str | None = None,
) -> bool:
    if not all(
        has_delta_log(materialized_root / f"{table_name}.delta") for table_name in DATA_PREP_TABLES
    ):
        return False
    if materialization_key is None:
        return True
    lock = _read_materialization_lock(materialized_root)
    return lock is not None and str(lock.get("materialization_key", "")) == materialization_key


def _write_materialization_lock(
    *,
    materialized_root: Path,
    materialization_key: str,
    normalized_config: dict[str, Any],
    campaign_id: str,
    campaign_run_id: str,
    report: dict[str, Any],
) -> None:
    dataset = dict(normalized_config["dataset"])
    profiles = dict(normalized_config["profiles"])
    volume_profile = dict(normalized_config.get("volume_profile", {}))
    validation = dict(normalized_config.get("validation", {}))
    validation_plan = _build_validation_plan(normalized_config)
    data_prep_output_paths = {
        table_name: path
        for table_name, path in dict(report.get("output_paths", {})).items()
        if table_name in DATA_PREP_TABLES
    }
    write_json(
        _materialization_lock_path(materialized_root),
        {
            "materialization_key": materialization_key,
            "config_fingerprint": build_config_fingerprint(normalized_config),
            "campaign_id": campaign_id,
            "campaign_run_id": campaign_run_id,
            "canonical_output_dir": str(normalized_config["canonical_output_dir"]),
            "materialized_root": materialized_root.as_posix(),
            "dataset_version": str(dataset["dataset_version"]),
            "dataset_name": str(dataset["dataset_name"]),
            "series_mode": str(dataset["series_mode"]),
            "continuous_front_policy": dict(dataset["continuous_front_policy"]),
            "timeframes": list(dataset["timeframes"]),
            "indicator_set_version": str(profiles["indicator_set_version"]),
            "indicator_profile_version": str(profiles["indicator_profile_version"]),
            "derived_indicator_set_version": str(profiles["derived_indicator_set_version"]),
            "derived_indicator_profile_version": str(profiles["derived_indicator_profile_version"]),
            "volume_profile": volume_profile,
            "validation": validation,
            "validation_plan": validation_plan,
            "rows_by_table": dict(report.get("rows_by_table", {})),
            "output_paths": data_prep_output_paths,
            "created_at": utc_now_iso(),
        },
    )


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
