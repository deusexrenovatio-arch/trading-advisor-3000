from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.research.strategies.compiler_bridge import (
    REQUIRED_STG02_ADAPTER_KEYS,
    compile_strategy_template_mappings,
    materialize_strategy_template_seed_registry,
    write_strategy_template_seed_report,
)
from trading_advisor_3000.product_plane.research.strategies.families import phase_stg02_family_adapters
from trading_advisor_3000.product_plane.research.strategies.manifests import build_strategy_template_identity
from trading_advisor_3000.product_plane.research.strategies.storage import materialize_strategy_template_manifest


def test_stg02_compiler_bridge_emits_template_manifests_for_current_family_adapters() -> None:
    adapters = phase_stg02_family_adapters()
    assert tuple(adapter.adapter_key for adapter in adapters) == REQUIRED_STG02_ADAPTER_KEYS

    mappings = compile_strategy_template_mappings(adapters=adapters)
    assert len(mappings) == len(REQUIRED_STG02_ADAPTER_KEYS)

    for adapter, mapping in zip(adapters, mappings, strict=True):
        expected = build_strategy_template_identity(adapter.template_manifest)
        assert mapping.strategy_template_id == expected.strategy_template_id
        assert mapping.template_manifest_hash == expected.template_manifest_hash
        assert mapping.template_manifest_json == expected.canonical_manifest_json
        assert adapter.template_manifest.author_source == "python_adapter"
        assert adapter.template_manifest.source_ref == mapping.source_ref


def test_stg02_compiler_bridge_rejects_explicit_empty_adapter_inventory() -> None:
    with pytest.raises(ValueError, match="phase STG-02 adapter inventory mismatch"):
        compile_strategy_template_mappings(adapters=())


def test_stg02_compiler_bridge_rejects_filesystem_source_ref_in_adapter_contract() -> None:
    adapter = phase_stg02_family_adapters()[0]
    bad_source_ref = "python_adapter:src/trading_advisor_3000/product_plane/research/strategies/families/ma_cross.py"
    with pytest.raises(ValueError, match="source_ref must use import-path notation"):
        replace(
            adapter,
            source_ref=bad_source_ref,
            template_manifest=replace(adapter.template_manifest, source_ref=bad_source_ref),
        )


def test_stg02_seed_registry_snapshot_is_deterministic_across_reruns(tmp_path: Path) -> None:
    first = materialize_strategy_template_seed_registry(registry_root=tmp_path)
    second = materialize_strategy_template_seed_registry(registry_root=tmp_path)

    assert first["inventory"]["adapter_count"] == len(REQUIRED_STG02_ADAPTER_KEYS)
    assert first["seed_snapshot"] == second["seed_snapshot"]
    assert first["delta_table_counts"]["research_strategy_families"] == len(REQUIRED_STG02_ADAPTER_KEYS)
    assert first["delta_table_counts"]["research_strategy_templates"] == len(REQUIRED_STG02_ADAPTER_KEYS)
    assert first["delta_table_counts"]["research_strategy_template_modules"] >= len(REQUIRED_STG02_ADAPTER_KEYS)


def test_stg02_seed_registry_prunes_retired_family_rows(tmp_path: Path) -> None:
    adapter = phase_stg02_family_adapters()[0]
    materialize_strategy_template_manifest(
        registry_root=tmp_path,
        family_manifest=replace(adapter.family_manifest, family_key="retired_alias"),
        template_manifest=replace(
            adapter.template_manifest,
            family_key="retired_alias",
            template_key="retired_alias_core",
        ),
    )
    assert {
        str(row["family_key"])
        for row in read_delta_table_rows(tmp_path / "research_strategy_families.delta")
    } == {"retired_alias"}

    report = materialize_strategy_template_seed_registry(registry_root=tmp_path)

    assert report["delta_table_counts"]["research_strategy_families"] == len(REQUIRED_STG02_ADAPTER_KEYS)
    assert "retired_alias" not in {
        str(row["family_key"])
        for row in read_delta_table_rows(tmp_path / "research_strategy_families.delta")
    }
    assert "retired_alias" not in {
        str(row["family_key"])
        for row in read_delta_table_rows(tmp_path / "research_strategy_templates.delta")
    }
    assert "retired_alias" not in {
        str(row["family_key"])
        for row in read_delta_table_rows(tmp_path / "research_strategy_template_modules.delta")
    }


def test_stg02_seed_mapping_report_tracks_adapter_source_to_delta_rows(tmp_path: Path) -> None:
    registry_root = tmp_path / "seed-registry"
    report_path = tmp_path / "seed-report.json"
    report = write_strategy_template_seed_report(
        registry_root=registry_root,
        report_path=report_path,
        created_at="2026-04-16T00:00:00Z",
    )
    assert report_path.exists()
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["inventory"]["adapter_count"] == len(REQUIRED_STG02_ADAPTER_KEYS)
    assert len(persisted["seed_rows"]) == len(REQUIRED_STG02_ADAPTER_KEYS)

    template_rows = read_delta_table_rows(registry_root / "research_strategy_templates.delta")
    rows_by_template_id = {
        str(row["strategy_template_id"]): row
        for row in template_rows
    }

    for mapping in report["seed_rows"]:
        row = rows_by_template_id[mapping["strategy_template_id"]]
        assert str(row["template_manifest_hash"]) == mapping["template_manifest_hash"]
        assert str(row["source_ref"]) == mapping["source_ref"]


def test_stg02_regression_preserves_stg01_template_identity_hash_rules(tmp_path: Path) -> None:
    report = materialize_strategy_template_seed_registry(registry_root=tmp_path)
    template_rows = read_delta_table_rows(tmp_path / "research_strategy_templates.delta")
    rows_by_template_id = {
        str(row["strategy_template_id"]): row
        for row in template_rows
    }

    for mapping in report["seed_rows"]:
        row = rows_by_template_id[mapping["strategy_template_id"]]
        canonical_manifest_json = str(row["template_manifest_json"])
        digest = hashlib.sha256(canonical_manifest_json.encode("utf-8")).hexdigest()
        assert mapping["strategy_template_id"] == f"stpl_{digest}"
        assert mapping["template_manifest_hash"] == f"sha256:{digest}"


def test_stg02_seed_generation_does_not_depend_on_strategy_catalog(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import trading_advisor_3000.product_plane.research.strategies.catalog as catalog_module

    def _fail_if_called() -> object:
        raise AssertionError("default_strategy_catalog must not be called by stg02 seed generation")

    monkeypatch.setattr(catalog_module, "default_strategy_catalog", _fail_if_called)
    report = materialize_strategy_template_seed_registry(registry_root=tmp_path)
    assert report["inventory"]["adapter_count"] == len(REQUIRED_STG02_ADAPTER_KEYS)


def test_stg02_repo_seed_precedence_keeps_single_canonical_template_row(tmp_path: Path) -> None:
    adapter = phase_stg02_family_adapters()[0]
    first = materialize_strategy_template_manifest(
        registry_root=tmp_path,
        family_manifest=adapter.family_manifest,
        template_manifest=adapter.template_manifest,
        created_at="2026-04-16T00:00:00Z",
    )
    repo_seed_manifest = replace(
        adapter.template_manifest,
        author_source="repo_seed",
        source_ref=f"repo_seed:tests/fixtures/{adapter.adapter_key}.yaml",
    )
    second = materialize_strategy_template_manifest(
        registry_root=tmp_path,
        family_manifest=adapter.family_manifest,
        template_manifest=repo_seed_manifest,
        created_at="2026-04-16T00:01:00Z",
    )
    assert first["strategy_template_id"] != second["strategy_template_id"]

    template_rows = read_delta_table_rows(tmp_path / "research_strategy_templates.delta")
    identity_rows = [
        row
        for row in template_rows
        if str(row.get("family_key", "")) == adapter.template_manifest.family_key
        and str(row.get("template_key", "")) == adapter.template_manifest.template_key
        and str(row.get("template_version", "")) == adapter.template_manifest.template_version
    ]
    assert len(identity_rows) == 1
    canonical = identity_rows[0]
    assert str(canonical["author_source"]) == "repo_seed"
    assert str(canonical["source_ref"]) == repo_seed_manifest.source_ref
    assert str(canonical["strategy_template_id"]) == second["strategy_template_id"]

    module_rows = read_delta_table_rows(tmp_path / "research_strategy_template_modules.delta")
    identity_module_rows = [
        row
        for row in module_rows
        if str(row.get("family_key", "")) == adapter.template_manifest.family_key
        and str(row.get("template_key", "")) == adapter.template_manifest.template_key
        and str(row.get("template_version", "")) == adapter.template_manifest.template_version
    ]
    assert len(identity_module_rows) == len(repo_seed_manifest.modules)
    assert {str(row["strategy_template_id"]) for row in identity_module_rows} == {second["strategy_template_id"]}
