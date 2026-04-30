from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.research.registry_store import build_strategy_space_id
from trading_advisor_3000.product_plane.research.backtests.engine import (
    StrategyFamilySearchSpec,
    resolve_parameter_rows,
    strategy_spec_to_search_spec,
)
from trading_advisor_3000.product_plane.research.strategies.compiler_bridge import materialize_strategy_template_seed_registry
from trading_advisor_3000.product_plane.research.strategies.families import phase_stg02_family_adapters
from trading_advisor_3000.product_plane.research.strategies.registry import build_strategy_registry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows


@dataclass(frozen=True)
class StrategySearchSpecPayload:
    search_spec: StrategyFamilySearchSpec
    strategy_template_id: str
    family_id: str
    family_key: str
    strategy_version_label: str

    def to_dict(self) -> dict[str, object]:
        payload = self.search_spec.to_dict()
        return {
            **payload,
            "strategy_template_id": self.strategy_template_id,
            "family_id": self.family_id,
            "family_key": self.family_key,
            "strategy_version_label": self.strategy_version_label,
        }


@dataclass(frozen=True)
class PreparedStrategySpace:
    strategy_space_id: str
    selected_template_ids: tuple[str, ...]
    family_search_specs: tuple[StrategySearchSpecPayload, ...]
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
    excluded_hashes = {str(item) for item in strategy_space.get("exclude_template_manifest_hashes", [])}
    if not family_keys and not template_ids:
        raise ValueError("strategy_space must select at least one family_keys or template_ids entry")

    selected = [
        row
        for row in template_rows
        if str(row.get("family_key", "")) in family_keys or str(row.get("strategy_template_id", "")) in template_ids
        if str(row.get("template_manifest_hash", "")) not in excluded_hashes
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
    retired_fields = sorted(
        field
        for field in ("include_instance_ids", "exclude_manifest_hashes", "materialize_instances", "max_instance_count")
        if field in strategy_space
    )
    if retired_fields:
        raise ValueError(
            "strategy_space uses retired per-instance fields; use family/template search fields instead: "
            + ", ".join(retired_fields)
        )
    materialize_strategy_template_seed_registry(
        registry_root=registry_root,
        created_at=created_at,
    )
    template_rows = _read_rows(_table_path(registry_root=registry_root, table_name="research_strategy_templates"))
    selected_template_rows = _selected_template_rows(template_rows=template_rows, strategy_space=strategy_space)
    version_labels = _version_label_by_family()
    max_parameter_combinations = int(strategy_space.get("max_parameter_combinations", 0) or 0)
    if max_parameter_combinations <= 0:
        raise ValueError("strategy_space.max_parameter_combinations must be positive")
    optimizer = strategy_space.get("optimizer", {})
    optimizer_engine = str(optimizer.get("engine", "grid")) if isinstance(optimizer, dict) else "grid"

    registry = build_strategy_registry()
    prospective_count = 0
    search_specs: list[StrategySearchSpecPayload] = []
    for template_row in selected_template_rows:
        version_label = version_labels.get(str(template_row["family_key"]), str(template_row["template_key"]))
        effective_search_space = _effective_search_space(
            template_row=template_row,
            strategy_space=strategy_space,
            version_label=version_label,
        )
        base_spec = strategy_spec_to_search_spec(
            registry.get(version_label),
            template_key=str(template_row["template_key"]),
            max_parameter_combinations=max_parameter_combinations,
        )
        search_spec = StrategyFamilySearchSpec(
            search_spec_version=base_spec.search_spec_version,
            family_key=base_spec.family_key,
            template_key=base_spec.template_key,
            strategy_version_label=base_spec.strategy_version_label,
            intent=base_spec.intent,
            allowed_clock_profiles=base_spec.allowed_clock_profiles,
            allowed_market_states=base_spec.allowed_market_states,
            required_price_inputs=base_spec.required_price_inputs,
            required_materialized_indicators=base_spec.required_materialized_indicators,
            required_materialized_derived=base_spec.required_materialized_derived,
            signal_surface_key=base_spec.signal_surface_key,
            signal_surface_mode=base_spec.signal_surface_mode,
            parameter_mode="product",
            parameter_space={key: tuple(value) for key, value in effective_search_space.items()},
            parameter_constraints=base_spec.parameter_constraints,
            clock_profile=base_spec.clock_profile,
            required_inputs_by_clock=base_spec.required_inputs_by_clock,
            parameter_space_by_role=base_spec.parameter_space_by_role,
            parameter_clock_map=base_spec.parameter_clock_map,
            optional_indicator_plan=base_spec.optional_indicator_plan,
            exit_parameter_space=base_spec.exit_parameter_space,
            risk_parameter_space=base_spec.risk_parameter_space,
            execution_assumptions=base_spec.execution_assumptions,
            max_parameter_combinations=max_parameter_combinations,
            chunking_policy=base_spec.chunking_policy,
            selection_policy=base_spec.selection_policy,
        )
        if optimizer_engine == "optuna":
            if not isinstance(optimizer, dict):
                raise ValueError("strategy_space.optimizer must be an object")
            prospective_count += int(optimizer.get("n_trials", 0) or 0)
        else:
            prospective_count += len(resolve_parameter_rows(search_spec))
        search_specs.append(
            StrategySearchSpecPayload(
                search_spec=search_spec,
                strategy_template_id=str(template_row["strategy_template_id"]),
                family_id=str(template_row["family_id"]),
                family_key=str(template_row["family_key"]),
                strategy_version_label=version_label,
            )
        )
    if prospective_count > max_parameter_combinations:
        raise ValueError(
            "strategy_space.max_parameter_combinations exceeded by family search parameter table: "
            f"{prospective_count} > {max_parameter_combinations}"
        )
    selected_template_ids = {str(row["strategy_template_id"]) for row in selected_template_rows}
    search_specs.sort(key=lambda item: (item.family_key, item.strategy_template_id))
    if not search_specs:
        raise ValueError("strategy_space resolved to 0 family search specs")

    output_paths = {
        table_name: _table_path(registry_root=registry_root, table_name=table_name).as_posix()
        for table_name in (
            "research_strategy_families",
            "research_strategy_templates",
            "research_strategy_template_modules",
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
        family_search_specs=tuple(search_specs),
        row_counts=row_counts,
        output_paths=output_paths,
    )
