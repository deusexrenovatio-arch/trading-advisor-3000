from __future__ import annotations

import ast
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from deltalake import DeltaTable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.research.strategies import (
    StrategyFamilyManifest,
    StrategyInstanceManifest,
    StrategyResolvedModule,
    StrategyTemplateManifest,
    StrategyTemplateModule,
    build_strategy_instance_identity,
    build_strategy_template_identity,
)
from trading_advisor_3000.product_plane.research.strategies.storage import (
    load_strategy_instance_graph,
    materialize_strategy_instance_manifest,
    materialize_strategy_template_manifest,
    phase_stg01_strategy_store_contract,
)


ROOT = Path(__file__).resolve().parents[3]
MANIFESTS_SOURCE = ROOT / "src/trading_advisor_3000/product_plane/research/strategies/manifests.py"
STORAGE_SOURCE = ROOT / "src/trading_advisor_3000/product_plane/research/strategies/storage.py"


def _family_manifest() -> StrategyFamilyManifest:
    return StrategyFamilyManifest(
        family_key="mtf_pullback",
        family_version="mtf-pullback-family-v2",
        hypothesis_family="trend_continuation",
        description="MTF pullback continuation family.",
        default_direction_mode="long_short",
        allowed_execution_modes=("signals", "order_func"),
        allowed_markets=("futures",),
        allowed_instrument_types=("continuous_future",),
        allowed_signal_tfs=("15m", "1h", "4h"),
    )


def _template_manifest() -> StrategyTemplateManifest:
    return StrategyTemplateManifest(
        family_key="mtf_pullback",
        hypothesis_family="trend_continuation",
        family_version="mtf-pullback-family-v2",
        template_key="mtf_pullback_1d_4h_1h",
        template_version="v1",
        title="MTF Pullback Core",
        description="Primary MTF pullback template.",
        market="futures",
        venue="moex",
        instrument_type="continuous_future",
        universe_id="liq_futures_core",
        direction_mode="long_short",
        regime_tf="1d",
        signal_tf="4h",
        trigger_tf="1h",
        execution_tf="15m",
        bar_type="time",
        closed_bars_only=True,
        execution_mode="signals",
        modules=(
            StrategyTemplateModule(
                role="regime_filter",
                alias="htf_trend",
                module_key="indicator.htf_trend_state",
                module_version="v1",
                params={},
                search_space=None,
                order_index=0,
            ),
            StrategyTemplateModule(
                role="entry",
                alias="pullback_entry",
                module_key="signal.mtf_pullback",
                module_version="v2",
                params={"pullback_depth": 0.5, "confirmation_bars": 2},
                search_space={
                    "pullback_depth": [0.25, 0.5, 0.75],
                    "confirmation_bars": [1, 2, 3],
                },
                order_index=1,
            ),
        ),
        risk_policy={
            "stop_model": "atr",
            "stop_atr_multiple": 1.1,
            "target_model": "atr",
            "target_atr_multiple": 2.2,
        },
        validation_policy={
            "split_method": "walk_forward",
            "preferred_metrics": ["calmar", "sharpe", "total_return"],
        },
        required_indicator_columns=(
            "ema_20",
            "ema_50",
            "atr_14",
            "htf_trend_state_code",
            "distance_to_session_vwap",
        ),
        author_source="repo_seed",
        status="active",
    )


def _instance_manifest(*, strategy_template_id: str, family_id: str) -> StrategyInstanceManifest:
    return StrategyInstanceManifest(
        strategy_template_id=strategy_template_id,
        family_id=family_id,
        family_key="mtf_pullback",
        template_key="mtf_pullback_1d_4h_1h",
        template_version="v1",
        market="futures",
        venue="moex",
        instrument_type="continuous_future",
        universe_id="liq_futures_core",
        direction_mode="long_short",
        regime_tf="1d",
        signal_tf="4h",
        trigger_tf="1h",
        execution_tf="15m",
        bar_type="time",
        closed_bars_only=True,
        execution_mode="signals",
        parameter_values={
            "pullback_depth": 0.5,
            "confirmation_bars": 2,
            "min_htf_rsi": 55,
        },
        resolved_modules=(
            StrategyResolvedModule(
                role="regime_filter",
                alias="htf_trend",
                module_key="indicator.htf_trend_state",
                module_version="v1",
                resolved_params={},
                derived_indicator_refs=("htf_trend_state_code",),
            ),
            StrategyResolvedModule(
                role="entry",
                alias="pullback_entry",
                module_key="signal.mtf_pullback",
                module_version="v2",
                resolved_params={
                    "pullback_depth": 0.5,
                    "confirmation_bars": 2,
                    "min_htf_rsi": 55,
                },
                derived_indicator_refs=("ema_20", "ema_50", "atr_14"),
            ),
        ),
        risk_policy={
            "stop_model": "atr",
            "stop_atr_multiple": 1.1,
            "target_model": "atr",
            "target_atr_multiple": 2.2,
        },
        required_indicator_columns=(
            "ema_20",
            "ema_50",
            "atr_14",
            "htf_trend_state_code",
            "distance_to_session_vwap",
        ),
        status="active",
    )


def _runtime_command(version: str) -> list[str]:
    launcher = shutil.which("py")
    if launcher is None:
        raise RuntimeError("python launcher `py` is required for cross-runtime STG-01 vectors")
    probe = subprocess.run(
        [launcher, f"-{version}", "-c", "import sys; print(sys.executable)"],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if probe.returncode != 0:
        raise RuntimeError(
            f"python {version} runtime is required for STG-01 cross-runtime proof but is unavailable"
        )
    return [launcher, f"-{version}"]


def _canonicalize_with_runtime(runtime_command: list[str], variant_path: Path) -> dict[str, str]:
    script = """
import hashlib
import json
import sys
from trading_advisor_3000.product_plane.research.strategies.manifests import canonical_manifest_json

payload = json.loads(open(sys.argv[1], encoding='utf-8').read())
canonical = canonical_manifest_json(payload)
digest = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
print(json.dumps({
    "python_version": sys.version.split(' ', 1)[0],
    "canonical_manifest_json": canonical,
    "sha256": digest,
    "strategy_template_id": f"stpl_{digest}",
    "template_manifest_hash": f"sha256:{digest}",
}, ensure_ascii=False))
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "src")
    result = subprocess.run(
        [*runtime_command, "-c", script, variant_path.as_posix()],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip())
    assert isinstance(payload, dict)
    return {str(key): str(value) for key, value in payload.items()}


def _import_targets(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


def test_stg01_contract_declares_strategy_registry_tables() -> None:
    contract = phase_stg01_strategy_store_contract()
    assert set(contract) == {
        "research_strategy_families",
        "research_strategy_templates",
        "research_strategy_template_modules",
        "research_strategy_instances",
        "research_strategy_instance_modules",
    }
    assert contract["research_strategy_templates"]["partition_by"] == ["family_key", "signal_tf"]
    assert contract["research_strategy_instances"]["partition_by"] == ["family_key", "signal_tf", "execution_mode"]
    assert contract["research_strategy_templates"]["columns"]["closed_bars_only"] == "bool"
    assert contract["research_strategy_template_modules"]["columns"]["enabled"] == "bool"


def test_template_identity_is_deterministic_and_cross_runtime_stable(tmp_path: Path) -> None:
    template = _template_manifest()
    identity = build_strategy_template_identity(template)

    payload_variant = {
        "schema_version": "strategy-template.v1",
        "family": {
            "family_version": "mtf-pullback-family-v2",
            "family_key": "mtf_pullback",
            "hypothesis_family": "trend_continuation",
        },
        "template": {
            "template_version": "v1",
            "description": "Primary MTF pullback template.",
            "title": "MTF Pullback Core",
            "template_key": "mtf_pullback_1d_4h_1h",
        },
        "market_scope": {
            "instrument_type": "continuous_future",
            "market": "futures",
            "universe_id": "liq_futures_core",
            "direction_mode": "long_short",
            "venue": "moex",
        },
        "clock_profile": {
            "closed_bars_only": True,
            "signal_tf": "4h",
            "regime_tf": "1d",
            "execution_tf": "15m",
            "trigger_tf": "1h",
            "bar_type": "time",
        },
        "modules": [
            {
                "alias": "htf_trend",
                "enabled": True,
                "module_key": "indicator.htf_trend_state",
                "module_version": "v1",
                "order_index": 0.0,
                "params": {},
                "role": "regime_filter",
                "search_space": None,
                "timeframe_scope": None,
            },
            {
                "module_key": "signal.mtf_pullback",
                "module_version": "v2",
                "order_index": 1.0,
                "params": {"confirmation_bars": 2.0, "pullback_depth": 0.5000},
                "search_space": {
                    "confirmation_bars": [1.0, 2.0, 3.0],
                    "pullback_depth": [0.2500, 0.5000, 0.7500],
                },
                "alias": "pullback_entry",
                "role": "entry",
                "enabled": True,
                "timeframe_scope": None,
            },
        ],
        "risk_policy": {
            "stop_model": "atr",
            "stop_atr_multiple": 1.1000,
            "target_model": "atr",
            "target_atr_multiple": 2.2000,
        },
        "execution_policy": {"execution_mode": "signals"},
        "validation_policy": {
            "preferred_metrics": ["calmar", "sharpe", "total_return"],
            "split_method": "walk_forward",
        },
        "required_indicator_columns": [
            "ema_20",
            "ema_50",
            "atr_14",
            "htf_trend_state_code",
            "distance_to_session_vwap",
        ],
        "search_space": None,
        "source_ref": None,
        "author_source": "repo_seed",
        "status": "active",
    }
    variant_path = tmp_path / "template-variant.json"
    variant_path.write_text(json.dumps(payload_variant, ensure_ascii=False), encoding="utf-8")
    runtime_311 = _runtime_command("3.11")
    runtime_312 = _runtime_command("3.12")
    identity_311 = _canonicalize_with_runtime(runtime_311, variant_path)
    identity_312 = _canonicalize_with_runtime(runtime_312, variant_path)

    assert identity_311["canonical_manifest_json"] == identity.canonical_manifest_json
    assert identity_312["canonical_manifest_json"] == identity.canonical_manifest_json
    assert identity_311["sha256"] == identity_312["sha256"]
    assert identity_311["strategy_template_id"] == identity_312["strategy_template_id"]
    assert identity_311["template_manifest_hash"] == identity_312["template_manifest_hash"]
    assert identity.strategy_template_id.startswith("stpl_")
    assert identity.template_manifest_hash.startswith("sha256:")


def test_strategy_registry_materialization_is_deduplicated_and_queryable(tmp_path: Path) -> None:
    family = _family_manifest()
    template = _template_manifest()

    first_template = materialize_strategy_template_manifest(
        registry_root=tmp_path,
        family_manifest=family,
        template_manifest=template,
    )
    second_template = materialize_strategy_template_manifest(
        registry_root=tmp_path,
        family_manifest=family,
        template_manifest=template,
    )

    assert first_template["strategy_template_id"] == second_template["strategy_template_id"]
    template_rows = read_delta_table_rows(tmp_path / "research_strategy_templates.delta")
    template_module_rows = read_delta_table_rows(tmp_path / "research_strategy_template_modules.delta")
    assert len(template_rows) == 1
    assert len(template_module_rows) == len(template.modules)

    instance = _instance_manifest(
        strategy_template_id=str(first_template["strategy_template_id"]),
        family_id=str(first_template["family_id"]),
    )
    first_instance = materialize_strategy_instance_manifest(registry_root=tmp_path, instance_manifest=instance)
    second_instance = materialize_strategy_instance_manifest(registry_root=tmp_path, instance_manifest=instance)

    assert first_instance["strategy_instance_id"] == second_instance["strategy_instance_id"]
    assert str(first_instance["strategy_instance_id"]).startswith("sinst_")
    assert str(first_instance["manifest_hash"]).startswith("sha256:")

    instance_rows = read_delta_table_rows(tmp_path / "research_strategy_instances.delta")
    instance_module_rows = read_delta_table_rows(tmp_path / "research_strategy_instance_modules.delta")
    assert len(instance_rows) == 1
    assert len(instance_module_rows) == len(instance.resolved_modules)

    lookup = load_strategy_instance_graph(
        registry_root=tmp_path,
        strategy_instance_id=str(first_instance["strategy_instance_id"]),
    )
    assert lookup["manifest_hash"] == first_instance["manifest_hash"]
    assert lookup["instance_manifest"]["template_id"] == first_template["strategy_template_id"]
    assert len(lookup["module_graph"]) == len(instance.resolved_modules)
    assert {row["module_alias"] for row in lookup["module_graph"]} == {"htf_trend", "pullback_entry"}


def test_strategy_registry_persists_timestamp_columns_as_delta_timestamps(tmp_path: Path) -> None:
    family = _family_manifest()
    template = _template_manifest()
    template_result = materialize_strategy_template_manifest(
        registry_root=tmp_path,
        family_manifest=family,
        template_manifest=template,
    )
    instance = _instance_manifest(
        strategy_template_id=str(template_result["strategy_template_id"]),
        family_id=str(template_result["family_id"]),
    )
    materialize_strategy_instance_manifest(registry_root=tmp_path, instance_manifest=instance)

    contract = phase_stg01_strategy_store_contract()
    table_names = (
        "research_strategy_families",
        "research_strategy_templates",
        "research_strategy_template_modules",
        "research_strategy_instances",
        "research_strategy_instance_modules",
    )
    for table_name in table_names:
        table_path = tmp_path / f"{table_name}.delta"
        schema = DeltaTable(str(table_path)).to_pyarrow_table().schema
        field_types = {field.name: str(field.type) for field in schema}
        timestamp_columns = [
            column_name
            for column_name, type_name in contract[table_name]["columns"].items()
            if type_name == "timestamp"
        ]
        for column_name in timestamp_columns:
            assert column_name in field_types
            assert field_types[column_name].startswith("timestamp")

    template_rows = read_delta_table_rows(tmp_path / "research_strategy_templates.delta")
    assert isinstance(template_rows[0]["created_at"], str)
    assert isinstance(template_rows[0]["deprecated_at"], (str, type(None)))


def test_instance_identity_is_stable_for_reordered_numeric_payloads() -> None:
    template_identity = build_strategy_template_identity(_template_manifest())
    family_id = "sfam_" + "1" * 64
    instance = _instance_manifest(
        strategy_template_id=template_identity.strategy_template_id,
        family_id=family_id,
    )
    identity = build_strategy_instance_identity(instance)

    variant = StrategyInstanceManifest(
        strategy_template_id=template_identity.strategy_template_id,
        family_id=family_id,
        family_key="mtf_pullback",
        template_key="mtf_pullback_1d_4h_1h",
        template_version="v1",
        market="futures",
        venue="moex",
        instrument_type="continuous_future",
        universe_id="liq_futures_core",
        direction_mode="long_short",
        regime_tf="1d",
        signal_tf="4h",
        trigger_tf="1h",
        execution_tf="15m",
        bar_type="time",
        closed_bars_only=True,
        execution_mode="signals",
        parameter_values={
            "min_htf_rsi": 55.0,
            "confirmation_bars": 2.0,
            "pullback_depth": 0.5000,
        },
        resolved_modules=instance.resolved_modules,
        risk_policy={
            "target_model": "atr",
            "target_atr_multiple": 2.2000,
            "stop_model": "atr",
            "stop_atr_multiple": 1.1000,
        },
        required_indicator_columns=instance.required_indicator_columns,
        status="active",
    )
    variant_identity = build_strategy_instance_identity(variant)
    raw_variant_canonical = json.dumps(variant.to_manifest_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert variant_identity.manifest_hash == identity.manifest_hash
    assert variant_identity.strategy_instance_id == identity.strategy_instance_id
    assert raw_variant_canonical != identity.canonical_manifest_json


def test_load_strategy_instance_graph_raises_for_unknown_instance(tmp_path: Path) -> None:
    with pytest.raises(KeyError):
        load_strategy_instance_graph(registry_root=tmp_path, strategy_instance_id="sinst_missing")


def test_stg01_storage_has_no_python_only_truth_imports() -> None:
    manifests_imports = _import_targets(MANIFESTS_SOURCE)
    storage_imports = _import_targets(STORAGE_SOURCE)
    forbidden_import_roots = {
        "trading_advisor_3000.product_plane.research.strategies.spec",
        "trading_advisor_3000.product_plane.research.strategies.catalog",
        "trading_advisor_3000.product_plane.research.strategies.registry",
    }
    assert manifests_imports.isdisjoint(forbidden_import_roots)
    assert storage_imports.isdisjoint(forbidden_import_roots)
