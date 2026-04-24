from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows

from .adapter_contracts import StrategyFamilyAdapter
from .families import phase_stg02_family_adapters
from .manifests import build_strategy_family_id, build_strategy_template_identity
from .storage import materialize_strategy_template_manifest, phase_stg01_strategy_store_contract

REQUIRED_STG02_ADAPTER_KEYS = (
    "ma_cross",
    "breakout",
    "mean_reversion",
    "mtf_pullback",
    "squeeze_release",
)


@dataclass(frozen=True)
class StrategyTemplateCompilationMapping:
    adapter_key: str
    adapter_version: str
    source_ref: str
    strategy_version: str
    family_id: str
    strategy_template_id: str
    template_manifest_hash: str
    template_key: str
    template_version: str
    template_manifest_json: str

    def to_dict(self) -> dict[str, str]:
        return {
            "adapter_key": self.adapter_key,
            "adapter_version": self.adapter_version,
            "source_ref": self.source_ref,
            "strategy_version": self.strategy_version,
            "family_id": self.family_id,
            "strategy_template_id": self.strategy_template_id,
            "template_manifest_hash": self.template_manifest_hash,
            "template_key": self.template_key,
            "template_version": self.template_version,
            "template_manifest_json": self.template_manifest_json,
        }


def _required_adapter_inventory(
    adapters: tuple[StrategyFamilyAdapter, ...] | None = None,
) -> tuple[StrategyFamilyAdapter, ...]:
    selected = phase_stg02_family_adapters() if adapters is None else adapters
    by_key: dict[str, StrategyFamilyAdapter] = {}
    for adapter in selected:
        if adapter.adapter_key in by_key:
            raise ValueError(f"duplicate adapter key: {adapter.adapter_key}")
        by_key[adapter.adapter_key] = adapter

    missing = [key for key in REQUIRED_STG02_ADAPTER_KEYS if key not in by_key]
    unexpected = sorted(key for key in by_key if key not in REQUIRED_STG02_ADAPTER_KEYS)
    if missing or unexpected:
        raise ValueError(
            "phase STG-02 adapter inventory mismatch: "
            f"missing={missing}, unexpected={unexpected}"
        )
    return tuple(by_key[key] for key in REQUIRED_STG02_ADAPTER_KEYS)


def compile_strategy_template_mappings(
    *,
    adapters: tuple[StrategyFamilyAdapter, ...] | None = None,
) -> tuple[StrategyTemplateCompilationMapping, ...]:
    mappings: list[StrategyTemplateCompilationMapping] = []
    for adapter in _required_adapter_inventory(adapters):
        family_id = build_strategy_family_id(
            family_key=adapter.family_manifest.family_key,
            family_version=adapter.family_manifest.family_version,
            hypothesis_family=adapter.family_manifest.hypothesis_family,
        )
        template_identity = build_strategy_template_identity(adapter.template_manifest)
        mappings.append(
            StrategyTemplateCompilationMapping(
                adapter_key=adapter.adapter_key,
                adapter_version=adapter.adapter_version,
                source_ref=adapter.source_ref,
                strategy_version=adapter.strategy_spec.version,
                family_id=family_id,
                strategy_template_id=template_identity.strategy_template_id,
                template_manifest_hash=template_identity.template_manifest_hash,
                template_key=adapter.template_manifest.template_key,
                template_version=adapter.template_manifest.template_version,
                template_manifest_json=template_identity.canonical_manifest_json,
            )
        )
    return tuple(mappings)


def _find_template_row(
    *,
    template_rows: list[dict[str, object]],
    strategy_template_id: str,
) -> dict[str, object]:
    matching_rows = [
        row
        for row in template_rows
        if str(row.get("strategy_template_id", "")) == strategy_template_id
    ]
    if not matching_rows:
        raise KeyError(f"template row not found for strategy_template_id={strategy_template_id}")
    return matching_rows[0]


def materialize_strategy_template_seed_registry(
    *,
    registry_root: Path,
    created_at: str | None = None,
    adapters: tuple[StrategyFamilyAdapter, ...] | None = None,
) -> dict[str, object]:
    inventory = _required_adapter_inventory(adapters)
    mappings = compile_strategy_template_mappings(adapters=inventory)
    materialized_rows: list[dict[str, object]] = []

    for adapter, mapping in zip(inventory, mappings, strict=True):
        result = materialize_strategy_template_manifest(
            registry_root=registry_root,
            family_manifest=adapter.family_manifest,
            template_manifest=adapter.template_manifest,
            created_at=created_at,
        )
        if str(result["strategy_template_id"]) != mapping.strategy_template_id:
            raise RuntimeError(
                "compiler bridge wrote unexpected strategy_template_id "
                f"for adapter={adapter.adapter_key}"
            )
        if str(result["template_manifest_hash"]) != mapping.template_manifest_hash:
            raise RuntimeError(
                "compiler bridge wrote unexpected template_manifest_hash "
                f"for adapter={adapter.adapter_key}"
            )
        materialized_rows.append(
            {
                **mapping.to_dict(),
                "delta_rows": {
                    "research_strategy_families": str(result["output_paths"]["research_strategy_families"]),
                    "research_strategy_templates": str(result["output_paths"]["research_strategy_templates"]),
                    "research_strategy_template_modules": str(
                        result["output_paths"]["research_strategy_template_modules"]
                    ),
                },
            }
        )

    contract = phase_stg01_strategy_store_contract()
    families_table = registry_root / "research_strategy_families.delta"
    templates_table = registry_root / "research_strategy_templates.delta"
    template_modules_table = registry_root / "research_strategy_template_modules.delta"
    family_rows = read_delta_table_rows(families_table)
    template_rows = read_delta_table_rows(templates_table)
    template_module_rows = read_delta_table_rows(template_modules_table)

    seed_snapshot = []
    for mapping in mappings:
        template_row = _find_template_row(
            template_rows=template_rows,
            strategy_template_id=mapping.strategy_template_id,
        )
        seed_snapshot.append(
            {
                "adapter_key": mapping.adapter_key,
                "source_ref": mapping.source_ref,
                "strategy_template_id": mapping.strategy_template_id,
                "template_manifest_hash": mapping.template_manifest_hash,
                "family_id": mapping.family_id,
                "template_key": str(template_row["template_key"]),
                "template_version": str(template_row["template_version"]),
            }
        )

    return {
        "schema_version": "stg02.strategy-template-seed-report.v1",
        "proof_class": "integration",
        "inventory": {
            "required_adapter_keys": list(REQUIRED_STG02_ADAPTER_KEYS),
            "adapter_count": len(inventory),
        },
        "seed_rows": materialized_rows,
        "seed_snapshot": seed_snapshot,
        "delta_table_counts": {
            "research_strategy_families": len(family_rows),
            "research_strategy_templates": len(template_rows),
            "research_strategy_template_modules": len(template_module_rows),
        },
        "output_paths": {
            "research_strategy_families": families_table.as_posix(),
            "research_strategy_templates": templates_table.as_posix(),
            "research_strategy_template_modules": template_modules_table.as_posix(),
        },
        "contract_tables": sorted(contract.keys()),
    }


def write_strategy_template_seed_report(
    *,
    registry_root: Path,
    report_path: Path,
    created_at: str | None = None,
    adapters: tuple[StrategyFamilyAdapter, ...] | None = None,
) -> dict[str, object]:
    report = materialize_strategy_template_seed_registry(
        registry_root=registry_root,
        created_at=created_at,
        adapters=adapters,
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    return report
