from __future__ import annotations

import json
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.research.registry_store import build_strategy_space_id
from trading_advisor_3000.product_plane.research.strategies.compiler_bridge import materialize_strategy_template_seed_registry
from trading_advisor_3000.product_plane.research.strategies.families import phase_stg02_family_adapters
from trading_advisor_3000.product_plane.research.strategies.manifests import StrategyInstanceManifest, StrategyResolvedModule, build_strategy_instance_identity
from trading_advisor_3000.product_plane.research.strategies.storage import materialize_strategy_instance_manifest
from trading_advisor_3000.product_plane.research.strategies.storage import phase_stg01_strategy_store_contract
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows


@dataclass(frozen=True)
class StrategyExecutionInstance:
    strategy_instance_id: str
    strategy_template_id: str
    family_id: str
    family_key: str
    strategy_version_label: str
    execution_mode: str
    parameter_values: dict[str, object]
    manifest_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_template_id": self.strategy_template_id,
            "family_id": self.family_id,
            "family_key": self.family_key,
            "strategy_version_label": self.strategy_version_label,
            "execution_mode": self.execution_mode,
            "parameter_values": dict(self.parameter_values),
            "manifest_hash": self.manifest_hash,
        }


@dataclass(frozen=True)
class PreparedStrategySpace:
    strategy_space_id: str
    selected_template_ids: tuple[str, ...]
    execution_instances: tuple[StrategyExecutionInstance, ...]
    row_counts: dict[str, int]
    output_paths: dict[str, str]


def _read_rows(path: Path) -> list[dict[str, object]]:
    if not has_delta_log(path):
        return []
    return read_delta_table_rows(path)


def _table_path(*, registry_root: Path, table_name: str) -> Path:
    return registry_root / f"{table_name}.delta"


def _parse_json(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        payload = json.loads(value)
        if isinstance(payload, dict):
            return {str(key): item for key, item in payload.items()}
    return {}


def _parse_list_json(value: object) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(str(item) for item in value)
    if isinstance(value, str) and value.strip():
        payload = json.loads(value)
        if isinstance(payload, list):
            return tuple(str(item) for item in payload)
    return tuple()


def _version_label_by_family() -> dict[str, str]:
    return {
        adapter.family_manifest.family_key: adapter.strategy_spec.version
        for adapter in phase_stg02_family_adapters()
    }


def _selected_template_rows(
    *,
    template_rows: list[dict[str, object]],
    strategy_space: dict[str, Any],
) -> list[dict[str, object]]:
    family_keys = {str(item) for item in strategy_space.get("family_keys", [])}
    template_ids = {str(item) for item in strategy_space.get("template_ids", [])}
    if not family_keys and not template_ids:
        raise ValueError("strategy_space must select at least one family_keys or template_ids entry")

    selected = [
        row
        for row in template_rows
        if str(row.get("family_key", "")) in family_keys or str(row.get("strategy_template_id", "")) in template_ids
    ]
    if not selected:
        raise ValueError("strategy_space selection resolved to 0 templates")
    return sorted(selected, key=lambda row: (str(row.get("family_key", "")), str(row.get("strategy_template_id", ""))))


def _override_keys(*, template_row: dict[str, object], version_label: str) -> tuple[str, ...]:
    template_key = str(template_row.get("template_key", ""))
    template_version = str(template_row.get("template_version", ""))
    return (
        str(template_row.get("family_key", "")),
        version_label,
        template_key,
        f"{template_key}:{template_version}",
        str(template_row.get("strategy_template_id", "")),
    )


def _effective_search_space(
    *,
    template_row: dict[str, object],
    strategy_space: dict[str, Any],
    version_label: str,
) -> dict[str, tuple[object, ...]]:
    template_space = {
        str(key): tuple(value if isinstance(value, list) else [value])
        for key, value in _parse_json(template_row.get("search_space_json")).items()
    }
    overrides = {
        str(key): value
        for key, value in _parse_json(strategy_space.get("search_space_overrides", {})).items()
    }
    effective: dict[str, tuple[object, ...]] = dict(template_space)
    for selector_key in _override_keys(template_row=template_row, version_label=version_label):
        override_payload = overrides.get(selector_key)
        if not isinstance(override_payload, dict):
            continue
        for name, values in override_payload.items():
            if isinstance(values, list):
                effective[str(name)] = tuple(values)
            else:
                effective[str(name)] = (values,)
    return effective


def _parameter_combinations(search_space: dict[str, tuple[object, ...]]) -> tuple[dict[str, object], ...]:
    if not search_space:
        return ({},)
    ordered = sorted(search_space.items(), key=lambda item: item[0])
    names = [name for name, _ in ordered]
    grids = [values for _, values in ordered]
    return tuple(
        {name: value for name, value in zip(names, combo, strict=True)}
        for combo in product(*grids)
    )


def _resolved_modules(
    *,
    template_module_rows: list[dict[str, object]],
    strategy_template_id: str,
    parameter_values: dict[str, object],
) -> tuple[StrategyResolvedModule, ...]:
    selected_rows = [
        row
        for row in template_module_rows
        if str(row.get("strategy_template_id", "")) == strategy_template_id
    ]
    selected_rows.sort(key=lambda row: (int(row.get("order_index", 0) or 0), str(row.get("module_alias", ""))))
    modules: list[StrategyResolvedModule] = []
    for row in selected_rows:
        params = _parse_json(row.get("params_json"))
        search_space = _parse_json(row.get("search_space_json"))
        resolved_params = dict(params)
        for name, value in parameter_values.items():
            if name in search_space or name in resolved_params:
                resolved_params[name] = value
        modules.append(
            StrategyResolvedModule(
                role=str(row["module_role"]),
                alias=str(row["module_alias"]),
                module_key=str(row["module_key"]),
                module_version=str(row["module_version"]),
                resolved_params=resolved_params,
                timeframe_scope=None if row.get("timeframe_scope") in {None, ""} else str(row["timeframe_scope"]),
                derived_feature_refs=tuple(),
            )
        )
    return tuple(modules)


def _build_instance_manifest(
    *,
    template_row: dict[str, object],
    template_manifest: dict[str, Any],
    template_module_rows: list[dict[str, object]],
    parameter_values: dict[str, object],
    campaign_id: str,
    campaign_run_id: str,
) -> StrategyInstanceManifest:
    market_scope = dict(template_manifest["market_scope"])
    clock_profile = dict(template_manifest["clock_profile"])
    execution_policy = dict(template_manifest["execution_policy"])
    risk_policy = dict(template_manifest["risk_policy"])
    return StrategyInstanceManifest(
        strategy_template_id=str(template_row["strategy_template_id"]),
        family_id=str(template_row["family_id"]),
        family_key=str(template_row["family_key"]),
        template_key=str(template_row["template_key"]),
        template_version=str(template_row["template_version"]),
        market=str(market_scope["market"]),
        venue=None if market_scope.get("venue") in {None, ""} else str(market_scope["venue"]),
        instrument_type=str(market_scope["instrument_type"]),
        universe_id=str(market_scope["universe_id"]),
        direction_mode=str(market_scope["direction_mode"]),
        regime_tf=str(clock_profile["regime_tf"]),
        signal_tf=str(clock_profile["signal_tf"]),
        trigger_tf=str(clock_profile["trigger_tf"]),
        execution_tf=str(clock_profile["execution_tf"]),
        bar_type=str(clock_profile["bar_type"]),
        closed_bars_only=bool(clock_profile["closed_bars_only"]),
        execution_mode=str(execution_policy["execution_mode"]),
        parameter_values=parameter_values,
        resolved_modules=_resolved_modules(
            template_module_rows=template_module_rows,
            strategy_template_id=str(template_row["strategy_template_id"]),
            parameter_values=parameter_values,
        ),
        risk_policy=risk_policy,
        required_feature_columns=tuple(_parse_list_json(template_row.get("required_feature_columns"))),
        generated_by_campaign_id=campaign_id,
        generated_by_campaign_run_id=campaign_run_id,
        status="active",
    )


def prepare_strategy_space(
    *,
    registry_root: Path,
    strategy_space: dict[str, Any],
    backtest_policy: dict[str, Any],
    execution_policy: dict[str, Any],
    campaign_id: str,
    campaign_run_id: str,
    created_at: str,
) -> PreparedStrategySpace:
    materialize_strategy_template_seed_registry(
        registry_root=registry_root,
        created_at=created_at,
    )
    contract = phase_stg01_strategy_store_contract()
    template_rows = _read_rows(_table_path(registry_root=registry_root, table_name="research_strategy_templates"))
    template_module_rows = _read_rows(_table_path(registry_root=registry_root, table_name="research_strategy_template_modules"))
    selected_template_rows = _selected_template_rows(template_rows=template_rows, strategy_space=strategy_space)
    version_labels = _version_label_by_family()
    excluded_manifest_hashes = {str(item) for item in strategy_space.get("exclude_manifest_hashes", [])}
    materialize_instances = bool(strategy_space.get("materialize_instances", True))
    max_instance_count = int(strategy_space.get("max_instance_count", 0) or 0)
    if max_instance_count <= 0:
        raise ValueError("strategy_space.max_instance_count must be positive")

    prospective_count = len(tuple(str(item) for item in strategy_space.get("include_instance_ids", [])))
    template_payloads: list[tuple[dict[str, object], dict[str, Any], str, tuple[dict[str, object], ...]]] = []
    for template_row in selected_template_rows:
        template_manifest = _parse_json(template_row.get("template_manifest_json"))
        version_label = version_labels.get(str(template_row["family_key"]), str(template_row["template_key"]))
        combos = _parameter_combinations(
            _effective_search_space(
                template_row=template_row,
                strategy_space=strategy_space,
                version_label=version_label,
            )
        )
        template_payloads.append((template_row, template_manifest, version_label, combos))
        prospective_count += len(combos) if materialize_instances else 0
    if prospective_count > max_instance_count:
        raise ValueError(
            "strategy_space.max_instance_count exceeded: "
            f"{prospective_count} > {max_instance_count}"
        )

    if materialize_instances:
        for template_row, template_manifest, _, combos in template_payloads:
            for parameter_values in combos:
                instance_manifest = _build_instance_manifest(
                    template_row=template_row,
                    template_manifest=template_manifest,
                    template_module_rows=template_module_rows,
                    parameter_values=parameter_values,
                    campaign_id=campaign_id,
                    campaign_run_id=campaign_run_id,
                )
                identity = build_strategy_instance_identity(instance_manifest)
                if identity.manifest_hash in excluded_manifest_hashes:
                    continue
                materialize_strategy_instance_manifest(
                    registry_root=registry_root,
                    instance_manifest=instance_manifest,
                    created_at=created_at,
                )

    instance_rows = _read_rows(_table_path(registry_root=registry_root, table_name="research_strategy_instances"))
    selected_instance_ids = {str(item) for item in strategy_space.get("include_instance_ids", [])}
    execution_instances: list[StrategyExecutionInstance] = []
    selected_template_ids = {str(row["strategy_template_id"]) for row in selected_template_rows}
    for row in instance_rows:
        manifest_hash = str(row.get("manifest_hash", ""))
        if manifest_hash in excluded_manifest_hashes:
            continue
        if str(row.get("strategy_instance_id", "")) in selected_instance_ids or str(row.get("strategy_template_id", "")) in selected_template_ids:
            execution_instances.append(
                StrategyExecutionInstance(
                    strategy_instance_id=str(row["strategy_instance_id"]),
                    strategy_template_id=str(row["strategy_template_id"]),
                    family_id=str(row["family_id"]),
                    family_key=str(row["family_key"]),
                    strategy_version_label=version_labels.get(str(row["family_key"]), str(row["template_key"])),
                    execution_mode=str(row["execution_mode"]),
                    parameter_values=_parse_json(row.get("parameter_values_json")),
                    manifest_hash=manifest_hash,
                )
            )
    execution_instances.sort(key=lambda item: (item.family_key, item.strategy_instance_id))
    if not execution_instances:
        raise ValueError("strategy_space resolved to 0 executable strategy instances")

    output_paths = {
        table_name: _table_path(registry_root=registry_root, table_name=table_name).as_posix()
        for table_name in (
            "research_strategy_families",
            "research_strategy_templates",
            "research_strategy_template_modules",
            "research_strategy_instances",
            "research_strategy_instance_modules",
        )
    }
    row_counts = {
        table_name: len(_read_rows(_table_path(registry_root=registry_root, table_name=table_name)))
        for table_name in output_paths
    }
    return PreparedStrategySpace(
        strategy_space_id=build_strategy_space_id(
            selected_template_ids=tuple(sorted(selected_template_ids)),
            strategy_space=strategy_space,
            backtest_policy=backtest_policy,
            execution_policy=execution_policy,
        ),
        selected_template_ids=tuple(sorted(selected_template_ids)),
        execution_instances=tuple(execution_instances),
        row_counts=row_counts,
        output_paths=output_paths,
    )
