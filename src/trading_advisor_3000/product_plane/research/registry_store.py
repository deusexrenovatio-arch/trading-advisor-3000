from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.research.strategies.manifests import canonical_manifest_json
from trading_advisor_3000.product_plane.research.strategies.storage import phase_stg01_strategy_store_contract


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def research_registry_root(*, canonical_output_dir: Path) -> Path:
    return canonical_output_dir.resolve() / "research-registry"


def research_registry_store_contract() -> dict[str, dict[str, object]]:
    return {
        **phase_stg01_strategy_store_contract(),
        "research_campaigns": {
            "format": "delta",
            "partition_by": ["campaign_name"],
            "constraints": ["unique(campaign_id)"],
            "columns": {
                "campaign_id": "string",
                "campaign_name": "string",
                "target_stage": "string",
                "config_fingerprint": "string",
                "dataset_version": "string",
                "indicator_set_version": "string",
                "indicator_profile_version": "string",
                "feature_set_version": "string",
                "feature_profile_version": "string",
                "strategy_space_json": "json",
                "backtest_policy_json": "json",
                "ranking_policy_json": "json",
                "projection_policy_json": "json",
                "execution_policy_json": "json",
                "campaign_config_json": "json",
                "status": "string",
                "created_at": "timestamp",
            },
        },
        "research_campaign_runs": {
            "format": "delta",
            "partition_by": ["campaign_name", "status"],
            "constraints": ["unique(campaign_run_id)"],
            "columns": {
                "campaign_run_id": "string",
                "campaign_id": "string",
                "campaign_name": "string",
                "target_stage": "string",
                "config_fingerprint": "string",
                "strategy_space_id": "string",
                "materialization_key": "string",
                "materialized_output_dir": "string",
                "results_output_dir": "string",
                "reuse_existing_materialization": "bool",
                "executed_steps": "array<string>",
                "reused_steps": "array<string>",
                "rows_by_table_json": "json",
                "output_paths_json": "json",
                "warnings_json": "json",
                "status": "string",
                "result_digest": "json",
                "started_at": "timestamp",
                "finished_at": "timestamp",
                "created_at": "timestamp",
            },
        },
        "research_run_stats_index": {
            "format": "delta",
            "partition_by": ["family_key", "timeframe"],
            "constraints": ["unique(campaign_run_id, backtest_run_id)"],
            "columns": {
                "campaign_run_id": "string",
                "backtest_run_id": "string",
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "dataset_version": "string",
                "instrument_id": "string",
                "contract_id": "string",
                "timeframe": "string",
                "window_id": "string",
                "total_return": "double",
                "sharpe": "double",
                "sortino": "double",
                "calmar": "double",
                "max_drawdown": "double",
                "profit_factor": "double",
                "win_rate": "double",
                "trade_count": "int",
                "turnover": "double",
                "commission_total": "double",
                "slippage_total": "double",
                "status": "string",
                "created_at": "timestamp",
            },
        },
        "research_rankings_index": {
            "format": "delta",
            "partition_by": ["family_key", "timeframe"],
            "constraints": ["unique(campaign_run_id, ranking_id)"],
            "columns": {
                "campaign_run_id": "string",
                "ranking_id": "string",
                "strategy_instance_id": "string",
                "backtest_run_id": "string",
                "family_id": "string",
                "family_key": "string",
                "dataset_version": "string",
                "timeframe": "string",
                "rank": "int",
                "objective_score": "double",
                "score_total": "double",
                "qualifies_for_projection": "bool",
                "ranking_policy_version": "string",
                "rank_reason_json": "json",
                "created_at": "timestamp",
            },
        },
        "research_strategy_notes": {
            "format": "delta",
            "partition_by": ["entity_type", "note_kind"],
            "constraints": ["unique(note_id)"],
            "columns": {
                "note_id": "string",
                "entity_type": "string",
                "entity_id": "string",
                "note_kind": "string",
                "severity": "string",
                "summary": "string",
                "evidence_json": "json",
                "author": "string",
                "source": "string",
                "tags": "array<string>",
                "created_at": "timestamp",
            },
        },
    }


def stable_registry_id(*, prefix: str, payload: object) -> str:
    digest = canonical_manifest_json(payload)
    return f"{prefix}_{_sha256_digest(digest)}"


def build_campaign_id(*, config_fingerprint: str) -> str:
    return stable_registry_id(prefix="camp", payload={"config_fingerprint": config_fingerprint})


def build_campaign_run_id(*, campaign_id: str, run_id: str) -> str:
    return stable_registry_id(prefix="crun", payload={"campaign_id": campaign_id, "run_id": run_id})


def build_strategy_space_id(
    *,
    selected_template_ids: tuple[str, ...],
    strategy_space: dict[str, Any],
    backtest_policy: dict[str, Any],
    execution_policy: dict[str, Any],
) -> str:
    return stable_registry_id(
        prefix="sspace",
        payload={
            "selected_template_ids": list(selected_template_ids),
            "strategy_space": strategy_space,
            "backtest_policy": backtest_policy,
            "execution_policy": execution_policy,
        },
    )


@dataclass(frozen=True)
class RegistryTableWrite:
    table_name: str
    table_path: str
    row_count: int


def _sha256_digest(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def _table_path(*, registry_root: Path, table_name: str) -> Path:
    return registry_root / f"{table_name}.delta"


def _read_rows(path: Path) -> list[dict[str, object]]:
    if not has_delta_log(path):
        return []
    return read_delta_table_rows(path)


def _replace_rows(rows: list[dict[str, object]], *, row: dict[str, object], key_fields: tuple[str, ...]) -> list[dict[str, object]]:
    def _matches(candidate: dict[str, object]) -> bool:
        return all(candidate.get(field) == row.get(field) for field in key_fields)

    preserved = [candidate for candidate in rows if not _matches(candidate)]
    return [*preserved, row]


def _append_rows(
    rows: list[dict[str, object]],
    *,
    new_rows: list[dict[str, object]],
    key_fields: tuple[str, ...],
) -> list[dict[str, object]]:
    merged = list(rows)
    seen_keys = {
        tuple(candidate.get(field) for field in key_fields)
        for candidate in merged
    }
    for row in new_rows:
        key = tuple(row.get(field) for field in key_fields)
        if key in seen_keys:
            continue
        merged.append(row)
        seen_keys.add(key)
    return merged


def _write_table(
    *,
    registry_root: Path,
    table_name: str,
    rows: list[dict[str, object]],
) -> RegistryTableWrite:
    contract = research_registry_store_contract()
    table_path = _table_path(registry_root=registry_root, table_name=table_name)
    write_delta_table_rows(
        table_path=table_path,
        rows=rows,
        columns=dict(contract[table_name]["columns"]),
    )
    return RegistryTableWrite(
        table_name=table_name,
        table_path=table_path.as_posix(),
        row_count=len(rows),
    )


def write_campaign_definition(
    *,
    registry_root: Path,
    campaign_id: str,
    campaign_name: str,
    target_stage: str,
    config_fingerprint: str,
    dataset_version: str,
    indicator_set_version: str,
    indicator_profile_version: str,
    feature_set_version: str,
    feature_profile_version: str,
    strategy_space: dict[str, Any],
    backtest_policy: dict[str, Any],
    ranking_policy: dict[str, Any],
    projection_policy: dict[str, Any],
    execution_policy: dict[str, Any],
    campaign_config: dict[str, Any],
    created_at: str | None = None,
) -> RegistryTableWrite:
    created_at = created_at or utc_now_iso()
    row = {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "target_stage": target_stage,
        "config_fingerprint": config_fingerprint,
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "indicator_profile_version": indicator_profile_version,
        "feature_set_version": feature_set_version,
        "feature_profile_version": feature_profile_version,
        "strategy_space_json": strategy_space,
        "backtest_policy_json": backtest_policy,
        "ranking_policy_json": ranking_policy,
        "projection_policy_json": projection_policy,
        "execution_policy_json": execution_policy,
        "campaign_config_json": campaign_config,
        "status": "active",
        "created_at": created_at,
    }
    table_name = "research_campaigns"
    table_path = _table_path(registry_root=registry_root, table_name=table_name)
    rows = _replace_rows(_read_rows(table_path), row=row, key_fields=("campaign_id",))
    return _write_table(registry_root=registry_root, table_name=table_name, rows=rows)


def write_campaign_run(
    *,
    registry_root: Path,
    campaign_run_id: str,
    campaign_id: str,
    campaign_name: str,
    target_stage: str,
    config_fingerprint: str,
    strategy_space_id: str,
    materialization_key: str,
    materialized_output_dir: str,
    results_output_dir: str,
    reuse_existing_materialization: bool,
    executed_steps: list[str],
    reused_steps: list[str],
    rows_by_table: dict[str, Any],
    output_paths: dict[str, Any],
    warnings: list[str],
    status: str,
    started_at: str,
    finished_at: str | None = None,
    result_digest: dict[str, Any] | None = None,
    created_at: str | None = None,
) -> RegistryTableWrite:
    created_at = created_at or started_at
    row = {
        "campaign_run_id": campaign_run_id,
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "target_stage": target_stage,
        "config_fingerprint": config_fingerprint,
        "strategy_space_id": strategy_space_id,
        "materialization_key": materialization_key,
        "materialized_output_dir": materialized_output_dir,
        "results_output_dir": results_output_dir,
        "reuse_existing_materialization": reuse_existing_materialization,
        "executed_steps": list(executed_steps),
        "reused_steps": list(reused_steps),
        "rows_by_table_json": rows_by_table,
        "output_paths_json": output_paths,
        "warnings_json": warnings,
        "status": status,
        "result_digest": result_digest,
        "started_at": started_at,
        "finished_at": finished_at or started_at,
        "created_at": created_at,
    }
    table_name = "research_campaign_runs"
    table_path = _table_path(registry_root=registry_root, table_name=table_name)
    rows = _replace_rows(_read_rows(table_path), row=row, key_fields=("campaign_run_id",))
    return _write_table(registry_root=registry_root, table_name=table_name, rows=rows)


def append_run_stats_index(
    *,
    registry_root: Path,
    rows: list[dict[str, object]],
) -> RegistryTableWrite:
    table_name = "research_run_stats_index"
    table_path = _table_path(registry_root=registry_root, table_name=table_name)
    merged_rows = _append_rows(
        _read_rows(table_path),
        new_rows=rows,
        key_fields=("campaign_run_id", "backtest_run_id"),
    )
    return _write_table(registry_root=registry_root, table_name=table_name, rows=merged_rows)


def append_rankings_index(
    *,
    registry_root: Path,
    rows: list[dict[str, object]],
) -> RegistryTableWrite:
    table_name = "research_rankings_index"
    table_path = _table_path(registry_root=registry_root, table_name=table_name)
    merged_rows = _append_rows(
        _read_rows(table_path),
        new_rows=rows,
        key_fields=("campaign_run_id", "ranking_id"),
    )
    return _write_table(registry_root=registry_root, table_name=table_name, rows=merged_rows)


def write_strategy_note(
    *,
    registry_root: Path,
    entity_type: str,
    entity_id: str,
    note_kind: str,
    summary: str,
    severity: str | None = None,
    evidence: dict[str, Any] | None = None,
    author: str | None = None,
    source: str = "human",
    tags: tuple[str, ...] = (),
    created_at: str | None = None,
) -> RegistryTableWrite:
    created_at = created_at or utc_now_iso()
    note_id = stable_registry_id(
        prefix="note",
        payload={
            "entity_type": entity_type,
            "entity_id": entity_id,
            "note_kind": note_kind,
            "summary": summary,
            "severity": severity,
            "source": source,
            "tags": list(tags),
            "created_at": created_at,
        },
    )
    row = {
        "note_id": note_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "note_kind": note_kind,
        "severity": severity,
        "summary": summary,
        "evidence_json": evidence,
        "author": author,
        "source": source,
        "tags": list(tags),
        "created_at": created_at,
    }
    table_name = "research_strategy_notes"
    table_path = _table_path(registry_root=registry_root, table_name=table_name)
    rows = _append_rows(_read_rows(table_path), new_rows=[row], key_fields=("note_id",))
    return _write_table(registry_root=registry_root, table_name=table_name, rows=rows)


def read_registry_table(*, registry_root: Path, table_name: str) -> list[dict[str, object]]:
    return _read_rows(_table_path(registry_root=registry_root, table_name=table_name))


def registry_output_paths(*, registry_root: Path) -> dict[str, str]:
    return {
        table_name: _table_path(registry_root=registry_root, table_name=table_name).as_posix()
        for table_name in research_registry_store_contract()
    }
