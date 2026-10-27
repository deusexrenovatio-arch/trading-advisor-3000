from __future__ import annotations

import copy
import json
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as snapshots
from trading_advisor_3000.product_plane.research import compute_contract, hash_compatibility

FROZEN = Path(__file__).parents[1] / "fixtures/research/accepted-compute-20261025.json"


def accepted_contract():
    return json.loads(FROZEN.read_text())["contract"]


def test_actual_accepted_contract_cannot_be_aliased_to_tail_rollout():
    old = accepted_contract()
    compute_contract.validate_contract(old)
    current = compute_contract.semantic_compute_contract()
    assert old["digest"] == "47df71fdf1fad0d6d866f5ca5c78ed638673194859e4af8173eab68d925e7dc8"
    assert old["engine"] != current["engine"]
    assert {k for k in old["kernels"] if old["kernels"][k] != current["kernels"][k]} == {
        "cf",
        "l1",
        "l2",
    }
    with pytest.raises(ValueError, match="unsupported admission compute transition"):
        hash_compatibility.admission_contract_transition(old, current)


@pytest.fixture
def migration_source(tmp_path):
    canonical, raw, research = (tmp_path / name for name in ("canonical", "raw", "old-research"))
    tables = {}
    for root, name in ((canonical, "canonical_bars"), (raw, "raw_moex_history")):
        path = root / f"{name}.delta"
        write_deltalake(str(path), pa.table({"id": [1]}))
        tables[name] = snapshots.capture_table(path)
    data = snapshots.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="data",
        run_id="data",
        code_version="test",
        quality_checks={"data": "PASS"},
        parameters={"downstream_scope": {"version": 2, "full_refresh": False, "windows": []}},
    )
    data_root = canonical / ".data_snapshots"
    data_path = snapshots.save_snapshot(data_root, data)
    snapshots.publish_snapshot(data_root, data_path, expected_current_id=None)
    source = snapshots.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="old",
        run_id="old",
        code_version="test",
        parents=[data["snapshot_id"]],
        quality_checks={key: "PASS" for key in hash_compatibility.REQUIRED_QUALITY},
        parameters={
            "series_mode": "continuous_front",
            "materialized_output_dir": str(research),
            "semantic_compute_contract": accepted_contract(),
        },
    )
    registry = research / ".data_snapshots" / "scope"
    source_path = snapshots.save_snapshot(registry, source)
    snapshots.publish_snapshot(registry, source_path, expected_current_id=None)
    return {
        "accepted_research_manifest_path": source_path,
        "data_manifest_path": data_path,
        "target_root": tmp_path / "verification",
        "domain": {
            "dataset_version": "migration-proof",
            "timeframes": ["15m"],
            "dataset_instrument_ids": ["FUT_BR"],
            "start_ts": "2026-10-01T00:00:00Z",
            "end_ts": "2026-10-20T00:00:00Z",
        },
    }


@pytest.mark.parametrize("nested_research_root", [False, True])
def test_plan_uses_explicit_rebuild_without_touching_source_or_target(
    migration_source, nested_research_root
):
    from trading_advisor_3000.product_plane.research.contract_migration import (
        plan_contract_migration,
    )

    source = migration_source
    if nested_research_root:
        source["target_root"] = source["target_root"] / "research" / "isolated"
    before = source["accepted_research_manifest_path"].read_bytes()
    plan = plan_contract_migration(**source)
    configs = [entry["config"] for entry in plan["run_config"]["ops"].values()]
    assert configs and all(config == configs[0] for config in configs)
    config = configs[0]
    assert config["operation"] == "rebuild" and config["force_full_refresh"] is True
    assert config["data_snapshot_enabled"] is True
    assert config["changed_windows"] == [] and config["hash_compatibility"] == {}
    assert config["refresh_admission"]["research_parent"] is None
    assert config["reuse_existing_materialization"] is False
    assert config["start_strategy_cascade"] is False
    assert all(
        Path(config[key]).is_relative_to(source["target_root"])
        for key in ("materialized_output_dir", "results_output_dir", "registry_root")
    )
    assert {"engine", "kernels", "proof", "schemas"} <= set(plan["changed_components"])
    assert (
        plan["source_manifest_sha256"]
        == snapshots.load_snapshot(source["accepted_research_manifest_path"])["manifest_sha256"]
    )
    assert not source["target_root"].exists()
    assert source["accepted_research_manifest_path"].read_bytes() == before


@pytest.mark.parametrize("root", ["source-child", "source-parent", "existing", "alias"])
def test_destinations_cannot_overlap_or_reuse_source(migration_source, tmp_path, root):
    from trading_advisor_3000.product_plane.research.contract_migration import (
        plan_contract_migration,
    )

    source = migration_source
    canonical = source["data_manifest_path"].parents[2]
    if root == "source-child":
        destination = canonical / "new-output"
    elif root == "source-parent":
        destination = canonical.parent
    elif root == "alias":
        alias = tmp_path / "alias"
        alias.symlink_to(canonical, target_is_directory=True)
        destination = alias / "new-output"
    else:
        destination = source["target_root"]
        destination.mkdir()
    with pytest.raises(ValueError, match="overlap|fresh"):
        plan_contract_migration(**{**source, "target_root": destination})
    assert not (canonical / "new-output").exists()


@pytest.mark.parametrize(
    "key", ["changed_windows", "operation", "selection", "materialized_output_dir"]
)
def test_domain_cannot_override_migration_ownership(migration_source, key):
    from trading_advisor_3000.product_plane.research.contract_migration import (
        plan_contract_migration,
    )

    source = migration_source
    with pytest.raises(ValueError, match="domain"):
        plan_contract_migration(**{**source, "domain": {**source["domain"], key: "unsafe"}})
    assert not source["target_root"].exists()


@pytest.mark.parametrize("drift", ["target-contract", "input", "source", "plan", "consumed"])
def test_apply_rejects_drift_before_any_destination_write(migration_source, monkeypatch, drift):
    from trading_advisor_3000.product_plane.research import contract_migration as owner

    source = migration_source
    plan = owner.plan_contract_migration(**source)
    if drift == "target-contract":
        original = owner.semantic_compute_contract

        def changed(config):
            result = copy.deepcopy(original(config))
            result["digest"] = "0" * 64
            return result

        monkeypatch.setattr(owner, "semantic_compute_contract", changed)
    elif drift == "input":
        data = snapshots.load_snapshot(source["data_manifest_path"])
        write_deltalake(
            data["tables"]["canonical_bars"]["table_path"], pa.table({"id": [2]}), mode="append"
        )
    elif drift == "source":
        payload = json.loads(source["accepted_research_manifest_path"].read_text())
        payload["parameters"]["unreviewed"] = True
        source["accepted_research_manifest_path"].write_text(json.dumps(payload))
    elif drift == "plan":
        plan["request"]["domain"]["dataset_version"] = "tampered"
    else:
        source["target_root"].mkdir()
    with pytest.raises((ValueError, snapshots.SnapshotUnavailableError)):
        owner.apply_contract_migration(plan)
    assert not source["target_root"].exists() or not list(source["target_root"].iterdir())


def test_native_instance_directory_exists_and_failure_preserves_source(
    migration_source, monkeypatch
):
    from dagster import DagsterInstance

    from trading_advisor_3000.product_plane.research import contract_migration as owner

    source = migration_source
    plan = owner.plan_contract_migration(**source)
    before = source["accepted_research_manifest_path"].read_bytes()

    def fail_before_compute(directory):
        assert Path(directory).is_dir(), "Dagster requires an existing instance directory"
        raise RuntimeError("injected failure before native compute")

    monkeypatch.setattr(DagsterInstance, "local_temp", staticmethod(fail_before_compute))
    with pytest.raises(RuntimeError, match="injected failure"):
        owner.apply_contract_migration(plan)
    assert source["accepted_research_manifest_path"].read_bytes() == before
    assert not list(source["target_root"].rglob("published_current.json"))
