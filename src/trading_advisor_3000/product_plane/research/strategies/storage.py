from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows, write_delta_table_rows

from .manifests import (
    StrategyFamilyManifest,
    StrategyInstanceManifest,
    StrategyTemplateManifest,
    build_strategy_family_id,
    build_strategy_instance_identity,
    build_strategy_template_identity,
    canonical_manifest_json,
)


def phase_stg01_strategy_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_strategy_families": {
            "format": "delta",
            "partition_by": ["family_key"],
            "constraints": ["unique(family_id)"],
            "columns": {
                "family_id": "string",
                "family_key": "string",
                "family_version": "string",
                "hypothesis_family": "string",
                "description": "string",
                "default_direction_mode": "string",
                "allowed_execution_modes": "array<string>",
                "allowed_markets": "array<string>",
                "allowed_instrument_types": "array<string>",
                "allowed_signal_tfs": "array<string>",
                "module_builder_key": "string",
                "source_ref": "string",
                "status": "string",
                "created_at": "timestamp",
            },
        },
        "research_strategy_templates": {
            "format": "delta",
            "partition_by": ["family_key", "signal_tf"],
            "constraints": ["unique(template_manifest_hash)"],
            "columns": {
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "template_key": "string",
                "template_version": "string",
                "title": "string",
                "description": "string",
                "market": "string",
                "venue": "string",
                "instrument_type": "string",
                "universe_id": "string",
                "direction_mode": "string",
                "regime_tf": "string",
                "signal_tf": "string",
                "trigger_tf": "string",
                "execution_tf": "string",
                "bar_type": "string",
                "closed_bars_only": "bool",
                "execution_mode": "string",
                "required_feature_columns": "array<string>",
                "search_space_json": "json",
                "template_manifest_json": "string",
                "template_manifest_hash": "string",
                "author_source": "string",
                "source_ref": "string",
                "status": "string",
                "created_at": "timestamp",
                "deprecated_at": "timestamp",
            },
        },
        "research_strategy_template_modules": {
            "format": "delta",
            "partition_by": ["family_key", "module_role"],
            "constraints": ["unique(strategy_template_id, module_alias, module_role, order_index)"],
            "columns": {
                "strategy_template_id": "string",
                "family_key": "string",
                "template_key": "string",
                "template_version": "string",
                "module_role": "string",
                "module_alias": "string",
                "module_key": "string",
                "module_version": "string",
                "timeframe_scope": "string",
                "order_index": "int",
                "enabled": "bool",
                "params_json": "json",
                "search_space_json": "json",
                "created_at": "timestamp",
            },
        },
        "research_strategy_instances": {
            "format": "delta",
            "partition_by": ["family_key", "signal_tf", "execution_mode"],
            "constraints": ["unique(manifest_hash)"],
            "columns": {
                "strategy_instance_id": "string",
                "strategy_template_id": "string",
                "family_id": "string",
                "family_key": "string",
                "template_key": "string",
                "template_version": "string",
                "market": "string",
                "venue": "string",
                "instrument_type": "string",
                "universe_id": "string",
                "direction_mode": "string",
                "regime_tf": "string",
                "signal_tf": "string",
                "trigger_tf": "string",
                "execution_tf": "string",
                "bar_type": "string",
                "closed_bars_only": "bool",
                "execution_mode": "string",
                "parameter_values_json": "json",
                "risk_policy_json": "json",
                "required_feature_columns": "array<string>",
                "instance_manifest_json": "string",
                "manifest_hash": "string",
                "generated_by_campaign_id": "string",
                "generated_by_campaign_run_id": "string",
                "status": "string",
                "created_at": "timestamp",
            },
        },
        "research_strategy_instance_modules": {
            "format": "delta",
            "partition_by": ["family_key", "module_role"],
            "constraints": ["unique(strategy_instance_id, module_alias, module_role, module_key, module_version)"],
            "columns": {
                "strategy_instance_id": "string",
                "family_key": "string",
                "module_role": "string",
                "module_alias": "string",
                "module_key": "string",
                "module_version": "string",
                "timeframe_scope": "string",
                "resolved_params_json": "json",
                "derived_feature_refs": "array<string>",
                "created_at": "timestamp",
            },
        },
    }


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _template_identity_key_from_row(row: dict[str, object]) -> tuple[str, str, str]:
    return (
        str(row.get("family_key", "")),
        str(row.get("template_key", "")),
        str(row.get("template_version", "")),
    )


def _template_identity_key_from_manifest(template_manifest: StrategyTemplateManifest) -> tuple[str, str, str]:
    return (
        template_manifest.family_key,
        template_manifest.template_key,
        template_manifest.template_version,
    )


def _select_canonical_template_row(
    *,
    incoming_row: dict[str, object],
    existing_identity_rows: list[dict[str, object]],
) -> tuple[dict[str, object], bool]:
    incoming_source = str(incoming_row.get("author_source", ""))
    if incoming_source == "repo_seed":
        return incoming_row, True

    repo_seed_rows = [
        row
        for row in existing_identity_rows
        if str(row.get("author_source", "")) == "repo_seed"
    ]
    if not repo_seed_rows:
        return incoming_row, True

    canonical_existing = max(
        repo_seed_rows,
        key=lambda row: (
            str(row.get("created_at", "")),
            str(row.get("template_manifest_hash", "")),
            str(row.get("strategy_template_id", "")),
        ),
    )
    return canonical_existing, False


def materialize_strategy_template_manifest(
    *,
    registry_root: Path,
    family_manifest: StrategyFamilyManifest,
    template_manifest: StrategyTemplateManifest,
    created_at: str | None = None,
) -> dict[str, object]:
    if template_manifest.family_key != family_manifest.family_key:
        raise ValueError("template_manifest.family_key must match family_manifest.family_key")
    created_at = created_at or _utc_now_iso()
    contract = phase_stg01_strategy_store_contract()
    family_table = _table_path(registry_root=registry_root, table_name="research_strategy_families")
    template_table = _table_path(registry_root=registry_root, table_name="research_strategy_templates")
    template_modules_table = _table_path(registry_root=registry_root, table_name="research_strategy_template_modules")

    family_id = build_strategy_family_id(
        family_key=family_manifest.family_key,
        family_version=family_manifest.family_version,
        hypothesis_family=family_manifest.hypothesis_family,
    )
    family_row = {
        "family_id": family_id,
        **family_manifest.to_manifest_dict(),
        "created_at": created_at,
    }
    existing_family_rows = _read_rows(family_table)
    write_delta_table_rows(
        table_path=family_table,
        rows=_replace_rows(existing_family_rows, row=family_row, key_fields=("family_id",)),
        columns=contract["research_strategy_families"]["columns"],
    )

    identity = build_strategy_template_identity(template_manifest)
    identity_key = _template_identity_key_from_manifest(template_manifest)
    existing_template_rows = _read_rows(template_table)
    template_payload = template_manifest.to_manifest_dict()
    incoming_template_row = {
        "strategy_template_id": identity.strategy_template_id,
        "family_id": family_id,
        "family_key": template_manifest.family_key,
        "template_key": template_manifest.template_key,
        "template_version": template_manifest.template_version,
        "title": template_manifest.title,
        "description": template_manifest.description,
        "market": template_manifest.market,
        "venue": template_manifest.venue,
        "instrument_type": template_manifest.instrument_type,
        "universe_id": template_manifest.universe_id,
        "direction_mode": template_manifest.direction_mode,
        "regime_tf": template_manifest.regime_tf,
        "signal_tf": template_manifest.signal_tf,
        "trigger_tf": template_manifest.trigger_tf,
        "execution_tf": template_manifest.execution_tf,
        "bar_type": template_manifest.bar_type,
        "closed_bars_only": template_manifest.closed_bars_only,
        "execution_mode": template_manifest.execution_mode,
        "required_feature_columns": list(template_manifest.required_feature_columns),
        "search_space_json": template_payload.get("search_space"),
        "template_manifest_json": identity.canonical_manifest_json,
        "template_manifest_hash": identity.template_manifest_hash,
        "author_source": template_manifest.author_source,
        "source_ref": template_manifest.source_ref,
        "status": template_manifest.status,
        "created_at": created_at,
        "deprecated_at": None,
    }
    existing_identity_rows = [
        row
        for row in existing_template_rows
        if _template_identity_key_from_row(row) == identity_key
    ]
    canonical_template_row, incoming_is_canonical = _select_canonical_template_row(
        incoming_row=incoming_template_row,
        existing_identity_rows=existing_identity_rows,
    )
    strategy_template_id = str(canonical_template_row["strategy_template_id"])
    template_manifest_hash = str(canonical_template_row["template_manifest_hash"])

    preserved_template_rows = [
        row
        for row in existing_template_rows
        if _template_identity_key_from_row(row) != identity_key
    ]
    write_delta_table_rows(
        table_path=template_table,
        rows=[*preserved_template_rows, canonical_template_row],
        columns=contract["research_strategy_templates"]["columns"],
    )

    incoming_template_module_rows = [
        {
            "strategy_template_id": strategy_template_id,
            "family_key": template_manifest.family_key,
            "template_key": template_manifest.template_key,
            "template_version": template_manifest.template_version,
            "module_role": module.role,
            "module_alias": module.alias,
            "module_key": module.module_key,
            "module_version": module.module_version,
            "timeframe_scope": module.timeframe_scope,
            "order_index": module.order_index,
            "enabled": module.enabled,
            "params_json": json.loads(canonical_manifest_json(dict(module.params))),
            "search_space_json": (
                None
                if module.search_space is None
                else json.loads(canonical_manifest_json(dict(module.search_space)))
            ),
            "created_at": created_at,
        }
        for module in template_manifest.modules
    ]
    existing_template_modules = _read_rows(template_modules_table)
    preserved_template_modules = [
        row
        for row in existing_template_modules
        if _template_identity_key_from_row(row) != identity_key
    ]
    if incoming_is_canonical:
        canonical_template_module_rows = incoming_template_module_rows
    else:
        canonical_template_module_rows = [
            row
            for row in existing_template_modules
            if _template_identity_key_from_row(row) == identity_key
            and str(row.get("strategy_template_id", "")) == strategy_template_id
        ]
        if not canonical_template_module_rows:
            raise RuntimeError(
                "canonical repo_seed template row is missing template modules for "
                f"identity={identity_key}"
            )
    write_delta_table_rows(
        table_path=template_modules_table,
        rows=[*preserved_template_modules, *canonical_template_module_rows],
        columns=contract["research_strategy_template_modules"]["columns"],
    )
    return {
        "family_id": family_id,
        "strategy_template_id": strategy_template_id,
        "template_manifest_hash": template_manifest_hash,
        "template_manifest_json": str(canonical_template_row["template_manifest_json"]),
        "template_module_count": len(canonical_template_module_rows),
        "output_paths": {
            "research_strategy_families": family_table.as_posix(),
            "research_strategy_templates": template_table.as_posix(),
            "research_strategy_template_modules": template_modules_table.as_posix(),
        },
    }


def materialize_strategy_instance_manifest(
    *,
    registry_root: Path,
    instance_manifest: StrategyInstanceManifest,
    created_at: str | None = None,
) -> dict[str, object]:
    created_at = created_at or _utc_now_iso()
    contract = phase_stg01_strategy_store_contract()
    instance_table = _table_path(registry_root=registry_root, table_name="research_strategy_instances")
    instance_modules_table = _table_path(registry_root=registry_root, table_name="research_strategy_instance_modules")

    identity = build_strategy_instance_identity(instance_manifest)
    existing_instance_rows = _read_rows(instance_table)
    matched_instance = next(
        (
            row
            for row in existing_instance_rows
            if str(row.get("manifest_hash", "")) == identity.manifest_hash
        ),
        None,
    )
    strategy_instance_id = (
        str(matched_instance["strategy_instance_id"])
        if matched_instance is not None
        else identity.strategy_instance_id
    )
    instance_payload = instance_manifest.to_manifest_dict()
    instance_row = {
        "strategy_instance_id": strategy_instance_id,
        "strategy_template_id": instance_manifest.strategy_template_id,
        "family_id": instance_manifest.family_id,
        "family_key": instance_manifest.family_key,
        "template_key": instance_manifest.template_key,
        "template_version": instance_manifest.template_version,
        "market": instance_manifest.market,
        "venue": instance_manifest.venue,
        "instrument_type": instance_manifest.instrument_type,
        "universe_id": instance_manifest.universe_id,
        "direction_mode": instance_manifest.direction_mode,
        "regime_tf": instance_manifest.regime_tf,
        "signal_tf": instance_manifest.signal_tf,
        "trigger_tf": instance_manifest.trigger_tf,
        "execution_tf": instance_manifest.execution_tf,
        "bar_type": instance_manifest.bar_type,
        "closed_bars_only": instance_manifest.closed_bars_only,
        "execution_mode": instance_manifest.execution_mode,
        "parameter_values_json": json.loads(canonical_manifest_json(dict(instance_manifest.parameter_values))),
        "risk_policy_json": json.loads(canonical_manifest_json(dict(instance_manifest.risk_policy))),
        "required_feature_columns": list(instance_manifest.required_feature_columns),
        "instance_manifest_json": identity.canonical_manifest_json,
        "manifest_hash": identity.manifest_hash,
        "generated_by_campaign_id": instance_manifest.generated_by_campaign_id,
        "generated_by_campaign_run_id": instance_manifest.generated_by_campaign_run_id,
        "status": instance_manifest.status,
        "created_at": created_at,
    }
    existing_instance_rows = [
        row
        for row in existing_instance_rows
        if str(row.get("strategy_instance_id", "")) != strategy_instance_id
        and str(row.get("manifest_hash", "")) != identity.manifest_hash
    ]
    write_delta_table_rows(
        table_path=instance_table,
        rows=[*existing_instance_rows, instance_row],
        columns=contract["research_strategy_instances"]["columns"],
    )

    instance_module_rows = [
        {
            "strategy_instance_id": strategy_instance_id,
            "family_key": instance_manifest.family_key,
            "module_role": module.role,
            "module_alias": module.alias,
            "module_key": module.module_key,
            "module_version": module.module_version,
            "timeframe_scope": module.timeframe_scope,
            "resolved_params_json": json.loads(canonical_manifest_json(dict(module.resolved_params))),
            "derived_feature_refs": list(module.derived_feature_refs),
            "created_at": created_at,
        }
        for module in instance_manifest.resolved_modules
    ]
    existing_instance_modules = [
        row
        for row in _read_rows(instance_modules_table)
        if str(row.get("strategy_instance_id", "")) != strategy_instance_id
    ]
    write_delta_table_rows(
        table_path=instance_modules_table,
        rows=[*existing_instance_modules, *instance_module_rows],
        columns=contract["research_strategy_instance_modules"]["columns"],
    )
    return {
        "strategy_instance_id": strategy_instance_id,
        "manifest_hash": identity.manifest_hash,
        "instance_manifest_json": identity.canonical_manifest_json,
        "instance_module_count": len(instance_module_rows),
        "output_paths": {
            "research_strategy_instances": instance_table.as_posix(),
            "research_strategy_instance_modules": instance_modules_table.as_posix(),
        },
    }


def load_strategy_instance_graph(*, registry_root: Path, strategy_instance_id: str) -> dict[str, object]:
    instance_table = _table_path(registry_root=registry_root, table_name="research_strategy_instances")
    modules_table = _table_path(registry_root=registry_root, table_name="research_strategy_instance_modules")
    instance_rows = _read_rows(instance_table)
    matching_rows = [
        row
        for row in instance_rows
        if str(row.get("strategy_instance_id", "")) == strategy_instance_id
    ]
    if not matching_rows:
        raise KeyError(f"strategy_instance_id not found: {strategy_instance_id}")
    instance_row = matching_rows[0]
    manifest_payload = json.loads(str(instance_row["instance_manifest_json"]))
    module_rows = [
        row
        for row in _read_rows(modules_table)
        if str(row.get("strategy_instance_id", "")) == strategy_instance_id
    ]
    module_rows = sorted(
        module_rows,
        key=lambda row: (
            str(row.get("module_role", "")),
            str(row.get("module_alias", "")),
            str(row.get("module_key", "")),
            str(row.get("module_version", "")),
        ),
    )
    return {
        "strategy_instance_id": strategy_instance_id,
        "manifest_hash": str(instance_row["manifest_hash"]),
        "instance_manifest": manifest_payload,
        "module_graph": module_rows,
    }
