from __future__ import annotations

import hashlib
import json
import traceback
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import yaml

from trading_advisor_3000.dagster_defs import (
    materialize_phase2b_backtest_assets,
    materialize_phase2b_bootstrap_assets,
    materialize_phase2b_projection_assets,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import (
    load_schema,
    validate_schema,
)
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows
from trading_advisor_3000.product_plane.research.jobs._common import validate_phase2b_contracts, write_json


CAMPAIGN_SCHEMA = Path("src/trading_advisor_3000/product_plane/contracts/schemas/research_campaign.v1.json")
RUN_SUMMARY_SCHEMA = Path("src/trading_advisor_3000/product_plane/contracts/schemas/research_run_summary.v1.json")
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
    "bootstrap": ("bootstrap",),
    "backtest": ("bootstrap", "backtest", "ranking"),
    "projection": ("bootstrap", "backtest", "ranking", "projection"),
}
BOOTSTRAP_TABLES = (
    "research_datasets",
    "research_bar_views",
    "research_indicator_frames",
    "research_feature_frames",
)


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
        raise CampaignBlockedError(f"campaign config must decode into an object: {config_path.as_posix()}")
    return {str(key): value for key, value in payload.items()}


def normalize_campaign_config(*, repo_root: Path, raw: dict[str, Any]) -> dict[str, Any]:
    schema = load_schema(repo_root / CAMPAIGN_SCHEMA)
    validate_schema(schema, raw)

    normalized = {
        "campaign_name": _normalized_non_empty(raw["campaign_name"]),
        "target_stage": _normalized_enum(raw["target_stage"], {"bootstrap", "backtest", "projection"}, field="target_stage"),
        "canonical_output_dir": resolve_path(repo_root=repo_root, raw=str(raw["canonical_output_dir"])).as_posix(),
        "materialized_root": resolve_path(repo_root=repo_root, raw=str(raw["materialized_root"])).as_posix(),
        "runs_root": resolve_path(repo_root=repo_root, raw=str(raw["runs_root"])).as_posix(),
        "dataset": _normalize_dataset(payload=_expect_dict(raw, "dataset")),
        "profiles": _normalize_profiles(_expect_dict(raw, "profiles")),
        "backtest": _normalize_backtest(_expect_dict(raw, "backtest")),
        "ranking_policy": _normalize_ranking_policy(_expect_dict(raw, "ranking_policy")),
        "projection_policy": _normalize_projection_policy(_expect_dict(raw, "projection_policy")),
        "execution": _normalize_execution(_expect_dict(raw, "execution")),
    }
    if normalized["dataset"]["base_timeframe"] not in normalized["dataset"]["timeframes"]:
        raise CampaignBlockedError("dataset.base_timeframe must be included in dataset.timeframes")
    if normalized["backtest"]["backtest_timeframe"] not in normalized["dataset"]["timeframes"]:
        raise CampaignBlockedError("backtest.backtest_timeframe must be included in dataset.timeframes")
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
        "indicator_set_version": profiles["indicator_set_version"],
        "indicator_profile_version": profiles["indicator_profile_version"],
        "feature_set_version": profiles["feature_set_version"],
        "feature_profile_version": profiles["feature_profile_version"],
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
    _write_status(run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="queued")
    results_root = run_root / "results"

    if raw_config_error is not None:
        stderr_lines.append(raw_config_traceback)
        _write_prevalidation_lock(
            run_root=run_root,
            run_id=run_id,
            resolved_config_path=resolved_config_path,
            raw_config=raw_config,
            results_root=results_root,
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
        _write_status(run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="blocked")
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
            results_root=results_root,
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
        _write_status(run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="blocked")
        return summary

    campaign_name = str(normalized_config["campaign_name"])
    config_fingerprint = build_config_fingerprint(normalized_config)
    materialization_key = build_materialization_key(normalized_config)
    materialized_root = Path(str(normalized_config["materialized_root"])) / materialization_key
    materialization_exists = _has_reusable_materialization(materialized_root)
    reuse_existing_materialization = materialization_exists and not bool(normalized_config["execution"]["force_rematerialize"])
    write_json(
        run_root / "campaign.lock.json",
        {
            "run_id": run_id,
            "config_path": resolved_config_path.as_posix(),
            "config_fingerprint": config_fingerprint,
            "materialization_key": materialization_key,
            "materialized_output_dir": materialized_root.as_posix(),
            "results_output_dir": results_root.as_posix(),
            "reuse_existing_materialization": reuse_existing_materialization,
            "campaign": normalized_config,
            "created_at": utc_now_iso(),
        },
    )
    _write_status(run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="running")

    warnings: list[str] = []
    if reuse_existing_materialization:
        warnings.append("materialized phase2b bootstrap layer reused from matching materialization_key")

    stage = str(normalized_config["target_stage"])
    stage_steps = list(TARGET_STEPS[stage])
    reused_steps = ["bootstrap"] if reuse_existing_materialization else []
    executed_steps = [step for step in stage_steps if step not in reused_steps]
    if stage == "bootstrap" and reuse_existing_materialization:
        executed_steps = []

    try:
        report = _dispatch_campaign(
            normalized_config=normalized_config,
            materialized_root=materialized_root,
            results_root=results_root,
            reuse_existing_materialization=reuse_existing_materialization,
        )
        contract_validation = validate_phase2b_contracts(
            output_paths=dict(report["output_paths"]),
            materialized_assets=list(report["materialized_assets"]),
            rows_by_table=dict(report.get("rows_by_table", {})),
        )
        warnings.extend(list(contract_validation.get("warnings", [])))
        if contract_validation["status"] != "passed":
            raise CampaignBlockedError("; ".join(contract_validation["errors"]) or "phase2b contract validation failed")

        summary = _build_terminal_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_name=campaign_name,
            target_stage=stage,
            canonical_output_dir=str(normalized_config["canonical_output_dir"]),
            materialization_key=materialization_key,
            materialized_root=materialized_root.as_posix(),
            run_root=run_root.as_posix(),
            dataset_version=str(normalized_config["dataset"]["dataset_version"]),
            config_fingerprint=config_fingerprint,
            executed_steps=executed_steps,
            reused_steps=reused_steps,
            durations={"total_seconds": round(perf_counter() - started, 6)},
            rows_by_table=dict(report.get("rows_by_table", {})),
            output_paths=dict(report["output_paths"]),
            dagster_selected_assets=list(report["selected_assets"]),
            dagster_materialized_assets=list(report["materialized_assets"]),
            warnings=warnings,
            status="success",
            error=None,
            result_digest=_build_result_digest(target_stage=stage, results_root=results_root),
        )
        write_json(run_root / "artifacts-index.json", _build_artifacts_index(report["output_paths"], report.get("rows_by_table", {})))
        _write_logs(run_root=run_root, stdout_lines=stdout_lines, stderr_lines=stderr_lines)
        _write_terminal_artifacts(run_root=run_root, summary=summary)
        _write_status(run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="success")
        return summary
    except CampaignBlockedError as exc:
        stderr_lines.append(traceback.format_exc())
        summary = _build_failure_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_name=campaign_name,
            normalized_config=normalized_config,
            materialization_key=materialization_key,
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
        _write_status(run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="blocked")
        return summary
    except Exception as exc:  # noqa: BLE001
        stderr_lines.append(traceback.format_exc())
        summary = _build_failure_summary(
            repo_root=repo_root,
            run_id=run_id,
            campaign_name=campaign_name,
            normalized_config=normalized_config,
            materialization_key=materialization_key,
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
        _write_status(run_root / "status.json", run_id=run_id, campaign_name=campaign_name, status="failed")
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
        campaign_name=campaign_name,
        target_stage=_raw_target_stage(raw_config),
        canonical_output_dir=_resolved_optional_path(repo_root=repo_root, raw=raw_config.get("canonical_output_dir")),
        materialization_key="",
        materialized_root=_resolved_optional_path(repo_root=repo_root, raw=raw_config.get("materialized_root")),
        run_root=run_root.as_posix(),
        dataset_version=_raw_dataset_version(raw_config),
        config_fingerprint=_raw_config_fingerprint(raw_config=raw_config, config_path=resolved_config_path),
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
) -> dict[str, Any]:
    common = _dagster_common_kwargs(
        normalized_config=normalized_config,
        materialized_root=materialized_root,
        results_root=results_root,
        reuse_existing_materialization=reuse_existing_materialization,
    )
    stage = str(normalized_config["target_stage"])
    raise_on_error = bool(normalized_config["execution"]["raise_on_error"])
    if stage == "bootstrap":
        return materialize_phase2b_bootstrap_assets(**common, raise_on_error=raise_on_error)
    if stage == "backtest":
        return materialize_phase2b_backtest_assets(**common, raise_on_error=raise_on_error)
    if stage == "projection":
        return materialize_phase2b_projection_assets(**common, raise_on_error=raise_on_error)
    raise CampaignBlockedError(f"unsupported target_stage `{stage}`")


def _dagster_common_kwargs(
    *,
    normalized_config: dict[str, Any],
    materialized_root: Path,
    results_root: Path,
    reuse_existing_materialization: bool,
) -> dict[str, Any]:
    dataset = dict(normalized_config["dataset"])
    profiles = dict(normalized_config["profiles"])
    backtest = dict(normalized_config["backtest"])
    ranking = dict(normalized_config["ranking_policy"])
    projection = dict(normalized_config["projection_policy"])
    return {
        "canonical_output_dir": Path(str(normalized_config["canonical_output_dir"])),
        "materialized_output_dir": materialized_root,
        "results_output_dir": results_root,
        "dataset_version": str(dataset["dataset_version"]),
        "dataset_name": str(dataset["dataset_name"]),
        "universe_id": str(dataset["universe_id"]),
        "timeframes": tuple(str(item) for item in dataset["timeframes"]),
        "base_timeframe": str(dataset["base_timeframe"]),
        "start_ts": str(dataset["start_ts"] or ""),
        "end_ts": str(dataset["end_ts"] or ""),
        "warmup_bars": int(dataset["warmup_bars"]),
        "split_method": str(dataset["split_method"]),
        "series_mode": str(dataset["series_mode"]),
        "dataset_contract_ids": tuple(str(item) for item in dataset["contract_ids"]),
        "dataset_instrument_ids": tuple(str(item) for item in dataset["instrument_ids"]),
        "indicator_set_version": str(profiles["indicator_set_version"]),
        "indicator_profile_version": str(profiles["indicator_profile_version"]),
        "feature_set_version": str(profiles["feature_set_version"]),
        "feature_profile_version": str(profiles["feature_profile_version"]),
        "code_version": "product-plane-run-campaign",
        "strategy_versions": tuple(str(item) for item in backtest["strategy_versions"]),
        "combination_count": int(backtest["combination_count"]),
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


def _build_terminal_summary(
    *,
    repo_root: Path,
    run_id: str,
    campaign_name: str,
    target_stage: str,
    canonical_output_dir: str,
    materialization_key: str,
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
        "campaign_name": campaign_name,
        "target_stage": target_stage,
        "status": status,
        "canonical_output_dir": canonical_output_dir,
        "materialization_key": materialization_key,
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
    campaign_name: str,
    normalized_config: dict[str, Any],
    materialization_key: str,
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
        campaign_name=campaign_name,
        target_stage=str(normalized_config["target_stage"]),
        canonical_output_dir=str(normalized_config["canonical_output_dir"]),
        materialization_key=materialization_key,
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
            "config_fingerprint": _raw_config_fingerprint(raw_config=raw_config, config_path=resolved_config_path),
            "materialization_key": "",
            "materialized_output_dir": _resolved_optional_path(repo_root=repo_root, raw=raw_config.get("materialized_root")),
            "results_output_dir": results_root.as_posix(),
            "reuse_existing_materialization": False,
            "campaign": raw_config,
            "validation_status": "blocked",
            "error": {"type": type(error).__name__, "message": str(error)},
            "created_at": utc_now_iso(),
        },
    )


def _build_result_digest(*, target_stage: str, results_root: Path) -> dict[str, Any] | None:
    if target_stage == "bootstrap":
        return None

    ranking_path = results_root / "research_strategy_rankings.delta"
    ranking_rows = read_delta_table_rows(ranking_path) if has_delta_log(ranking_path) else []
    ranking_sorted = sorted(
        ranking_rows,
        key=lambda row: (
            int(row.get("policy_pass", 0)),
            float(row.get("robust_score", 0.0) or 0.0),
            float(row.get("mean_total_return", 0.0) or 0.0),
        ),
        reverse=True,
    )
    digest: dict[str, Any] = {
        "policy_pass_count": sum(int(row.get("policy_pass", 0)) for row in ranking_rows),
        "positive_return_count": sum(
            1 for row in ranking_rows if float(row.get("mean_total_return", 0.0) or 0.0) > 0.0
        ),
        "ranking_top_rows": [
            {
                "strategy_version": str(row.get("strategy_version", "")),
                "contract_id": str(row.get("contract_id", "")),
                "timeframe": str(row.get("timeframe", "")),
                "selected_rank": int(row.get("selected_rank", 0)),
                "robust_score": round(float(row.get("robust_score", 0.0) or 0.0), 6),
                "mean_total_return": round(float(row.get("mean_total_return", 0.0) or 0.0), 6),
                "trade_count_total": int(row.get("trade_count_total", 0)),
            }
            for row in ranking_sorted[:5]
        ],
    }
    if target_stage == "projection":
        candidate_path = results_root / "research_signal_candidates.delta"
        candidate_rows = read_delta_table_rows(candidate_path) if has_delta_log(candidate_path) else []
        by_strategy: dict[str, int] = {}
        by_timeframe: dict[str, int] = {}
        for row in candidate_rows:
            strategy = str(row.get("strategy_version_id", ""))
            timeframe = str(row.get("timeframe", ""))
            by_strategy[strategy] = by_strategy.get(strategy, 0) + 1
            by_timeframe[timeframe] = by_timeframe.get(timeframe, 0) + 1
        digest["candidate_count"] = len(candidate_rows)
        digest["candidate_rows_by_strategy"] = dict(sorted(by_strategy.items()))
        digest["candidate_rows_by_timeframe"] = dict(sorted(by_timeframe.items()))
    return digest


def _build_artifacts_index(output_paths: dict[str, Any], rows_by_table: dict[str, Any]) -> dict[str, Any]:
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


def _prepare_run_root(*, repo_root: Path, runs_root_raw: str, campaign_name: str, run_id: str) -> Path:
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
    runs_root = _provisional_runs_root(repo_root=repo_root, raw_config=raw_config)
    if runs_root == (repo_root / "artifacts" / "research-campaign-invalid").resolve():
        warnings.append("runs_root missing or invalid; blocked evidence stored under repo-local fallback root")
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
    return {
        "dataset_version": _normalized_non_empty(payload["dataset_version"]),
        "dataset_name": _normalized_non_empty(payload["dataset_name"]),
        "universe_id": _normalized_non_empty(payload["universe_id"]),
        "series_mode": _normalized_enum(payload["series_mode"], {"contract", "continuous_front"}, field="dataset.series_mode"),
        "timeframes": timeframes,
        "base_timeframe": _normalized_non_empty(payload["base_timeframe"]),
        "start_ts": _normalized_optional_string(payload.get("start_ts")),
        "end_ts": _normalized_optional_string(payload.get("end_ts")),
        "warmup_bars": _normalized_non_negative_int(payload["warmup_bars"], field="dataset.warmup_bars"),
        "split_method": _normalized_enum(payload["split_method"], {"full", "holdout", "walk_forward"}, field="dataset.split_method"),
        "contract_ids": _sorted_unique_strings(payload["contract_ids"]),
        "instrument_ids": _sorted_unique_strings(payload["instrument_ids"]),
    }


def _normalize_profiles(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "indicator_set_version": _normalized_non_empty(payload["indicator_set_version"]),
        "indicator_profile_version": _normalized_non_empty(payload["indicator_profile_version"]),
        "feature_set_version": _normalized_non_empty(payload["feature_set_version"]),
        "feature_profile_version": _normalized_non_empty(payload["feature_profile_version"]),
    }


def _normalize_backtest(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_versions": _sorted_unique_strings(payload["strategy_versions"]),
        "combination_count": _normalized_positive_int(payload["combination_count"], field="backtest.combination_count"),
        "param_batch_size": _normalized_positive_int(payload["param_batch_size"], field="backtest.param_batch_size"),
        "series_batch_size": _normalized_positive_int(payload["series_batch_size"], field="backtest.series_batch_size"),
        "backtest_timeframe": _normalized_non_empty(payload["backtest_timeframe"]),
        "fees_bps": _normalized_non_negative_number(payload["fees_bps"], field="backtest.fees_bps"),
        "slippage_bps": _normalized_non_negative_number(payload["slippage_bps"], field="backtest.slippage_bps"),
        "allow_short": bool(payload["allow_short"]),
        "window_count": _normalized_positive_int(payload["window_count"], field="backtest.window_count"),
    }


def _normalize_ranking_policy(payload: dict[str, Any]) -> dict[str, Any]:
    metric_order = _unique_strings_preserve_order(payload["metric_order"])
    if not metric_order:
        raise CampaignBlockedError("ranking_policy.metric_order must not be empty")
    return {
        "policy_id": _normalized_non_empty(payload["policy_id"]),
        "metric_order": metric_order,
        "require_out_of_sample_pass": bool(payload["require_out_of_sample_pass"]),
        "min_trade_count": _normalized_positive_int(payload["min_trade_count"], field="ranking_policy.min_trade_count"),
        "max_drawdown_cap": _normalized_non_negative_number(payload["max_drawdown_cap"], field="ranking_policy.max_drawdown_cap"),
        "min_positive_fold_ratio": _normalized_ratio(payload["min_positive_fold_ratio"], field="ranking_policy.min_positive_fold_ratio"),
        "stress_slippage_bps": _normalized_non_negative_number(payload["stress_slippage_bps"], field="ranking_policy.stress_slippage_bps"),
        "min_parameter_stability": _normalized_ratio(payload["min_parameter_stability"], field="ranking_policy.min_parameter_stability"),
        "min_slippage_score": _normalized_ratio(payload["min_slippage_score"], field="ranking_policy.min_slippage_score"),
    }


def _normalize_projection_policy(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "selection_policy": _normalized_non_empty(payload["selection_policy"]),
        "max_candidates_per_partition": _normalized_positive_int(
            payload["max_candidates_per_partition"],
            field="projection_policy.max_candidates_per_partition",
        ),
        "min_robust_score": _normalized_ratio(payload["min_robust_score"], field="projection_policy.min_robust_score"),
        "decision_lag_bars_max": _normalized_non_negative_int(
            payload["decision_lag_bars_max"],
            field="projection_policy.decision_lag_bars_max",
        ),
    }


def _normalize_execution(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "force_rematerialize": bool(payload["force_rematerialize"]),
        "raise_on_error": bool(payload["raise_on_error"]),
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


def _provisional_runs_root(*, repo_root: Path, raw_config: dict[str, Any]) -> Path:
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


def _has_reusable_materialization(materialized_root: Path) -> bool:
    return all(has_delta_log(materialized_root / f"{table_name}.delta") for table_name in BOOTSTRAP_TABLES)


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
