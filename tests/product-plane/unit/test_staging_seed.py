from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common.data_snapshots import (
    _hash,
    atomic_json,
    capture_table,
    create_snapshot,
    current_snapshot,
    load_snapshot,
    save_snapshot,
)
from trading_advisor_3000.product_plane.common.staging_seed import _digest, apply_seed, plan_seed


def frozen_fixture(tmp_path):
    options, _, _ = seed_fixture(tmp_path)
    old = load_snapshot(options["research"])
    parent = load_snapshot(options["data"])
    registry = options["research"].parent.parent
    updated = {**old, "snapshot_id": uuid4().hex, "run_id": "new-accepted-checkpoint"}
    updated["manifest_sha256"] = _hash({k: v for k, v in updated.items() if k != "manifest_sha256"})
    save_snapshot(registry, updated)
    atomic_json(
        registry / "published_current.json",
        {
            "schema_version": "data_snapshot_publication.v1",
            "snapshot_id": updated["snapshot_id"],
            "manifest_sha256": updated["manifest_sha256"],
        },
    )
    evidence = tmp_path / "accepted-source-observation.json"
    evidence.write_text(
        json.dumps(
            {
                "observed_at_utc": "2026-01-01T00:00:00Z",
                "readback": {
                    "item": {
                        "type": "CommandExecution",
                        "status": "completed",
                        "exit_code": 0,
                        "aggregated_output": json.dumps(
                            {
                                "research": [
                                    {
                                        "snapshot_id": old["snapshot_id"],
                                        "manifest_sha256": old["manifest_sha256"],
                                        "data_parent": parent["snapshot_id"],
                                    }
                                ]
                            }
                        ),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    control = tmp_path / "plan.json"
    task = json.loads(control.read_text())
    task["source"] = {
        "snapshot_id": old["snapshot_id"],
        "manifest_sha256": old["manifest_sha256"],
        "accepted_observation": {
            "evidence_path": str(evidence),
            "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
            "observed_at_utc": "2026-01-01T00:00:00Z",
        },
    }
    atomic_json(control, task)
    marker = options["target"] / ".ta3000-staging.json"
    binding = json.loads(marker.read_text())
    binding["plan_digest"] = _digest(task)
    atomic_json(marker, binding)
    return options, old, evidence, control


def test_frozen_native_observation_preserves_accepted_checkpoint_when_current_advances(tmp_path):
    options, old, _, _ = frozen_fixture(tmp_path)
    result = plan_seed(**options)
    assert result["heads"][1] == old["snapshot_id"]


def test_frozen_seed_rejects_changed_readback(tmp_path):
    options, _, evidence, _ = frozen_fixture(tmp_path)
    evidence.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="readback digest"):
        plan_seed(**options)


def test_frozen_seed_rejects_evidence_inside_writable_data(tmp_path):
    options, _, evidence, control = frozen_fixture(tmp_path)
    task = json.loads(control.read_text())
    # A candidate cannot replace the protected evidence with its writable output.
    task["source"]["accepted_observation"]["evidence_path"] = str(options["target"] / evidence.name)
    atomic_json(control, task)
    marker = options["target"] / ".ta3000-staging.json"
    binding = json.loads(marker.read_text())
    binding["plan_digest"] = _digest(task)
    atomic_json(marker, binding)
    with pytest.raises((ValueError, FileNotFoundError)):
        plan_seed(**options)


def seed_fixture(tmp_path):
    source, target = tmp_path / "source", tmp_path / "target"
    table = source / "raw" / "raw.delta"
    write_deltalake(str(table), pa.table({"value": [1]}))
    old = capture_table(table)
    write_deltalake(str(table), pa.table({"value": [2]}), mode="append")
    new = capture_table(table)
    data_root, research_root = (
        source / "canonical" / ".data_snapshots",
        source / "research" / ".data_snapshots",
    )

    def manifest(root, ref, parents=()):
        result = create_snapshot(
            tables={"raw": ref},
            required_tables=["raw"],
            dataset_version="tiny",
            run_id="fixture",
            code_version="fixture",
            quality_checks={"fixture": "PASS"},
            parents=parents,
        )
        result["tables"]["raw"]["table_path"] = str(target / "raw" / "raw.delta")
        result["manifest_sha256"] = _hash(
            {k: v for k, v in result.items() if k != "manifest_sha256"}
        )
        path = save_snapshot(root, result)
        atomic_json(
            root / "published_current.json",
            {
                "schema_version": "data_snapshot_publication.v1",
                "snapshot_id": result["snapshot_id"],
                "manifest_sha256": result["manifest_sha256"],
            },
        )
        return path, result

    _, first = manifest(data_root, old)
    data, data_manifest = manifest(data_root, new, [first["snapshot_id"]])
    research, _ = manifest(research_root, new, [data_manifest["snapshot_id"]])
    target.mkdir()
    control = tmp_path / "plan.json"
    fixture = Path(__file__).resolve().parents[1] / "fixtures/contracts/staging_acceptance.v1.json"
    task = json.loads(fixture.read_text())
    task.update(data_root=str(target), quota_home=str(tmp_path / "quota"))
    atomic_json(control, task)
    atomic_json(
        target / ".ta3000-staging.json",
        {
            "schema_version": "staging_root.v1",
            "plan_path": str(control),
            "plan_digest": _digest(task),
        },
    )
    options = dict(source=source, target=target, logical_root=target, data=data, research=research)
    return options, data_root, first


def test_seed_preserves_pinned_versions_ancestry_and_independent_files(tmp_path):
    options, _, first = seed_fixture(tmp_path)
    plan = plan_seed(**options)
    # Advance source after pin selection: neither its future log nor new active file is copied.
    write_deltalake(
        str(options["source"] / "raw" / "raw.delta"), pa.table({"value": [3]}), mode="append"
    )
    receipt = apply_seed(plan, max_bytes=plan["total_bytes"])
    table = DeltaTable(str(options["target"] / "raw" / "raw.delta"))
    assert table.version() == 1
    assert sorted(table.to_pyarrow_table().column("value").to_pylist()) == [1, 2]
    old = DeltaTable(str(options["target"] / "raw" / "raw.delta"), version=0)
    assert old.to_pyarrow_table().num_rows == 1
    assert first["snapshot_id"] in receipt["manifests"]
    for relative in plan["files"]:
        assert not (options["source"] / relative).samefile(options["target"] / relative)
    with pytest.raises(ValueError, match="empty"):
        apply_seed(plan, max_bytes=plan["total_bytes"])


def test_seed_publication_is_frozen_even_when_source_pointer_changes(tmp_path):
    options, data_root, _ = seed_fixture(tmp_path)
    plan = plan_seed(**options)
    (data_root / "published_current.json").write_text("{}")
    apply_seed(plan, max_bytes=plan["total_bytes"])
    assert (
        current_snapshot(options["target"] / "canonical" / ".data_snapshots")["snapshot_id"]
        == plan["heads"][0]
    )
    assert load_snapshot(options["target"] / options["data"].relative_to(options["source"]))[
        "tables"
    ]["raw"]["table_path"].startswith(str(options["target"]))


def test_seed_requires_budget_unchanged_control_and_disjoint_roots(tmp_path):
    options, _, _ = seed_fixture(tmp_path)
    plan = plan_seed(**options)
    with pytest.raises(ValueError, match="byte bound"):
        apply_seed(plan, max_bytes=1)
    marker = options["target"] / ".ta3000-staging.json"
    binding = json.loads(marker.read_text())
    Path(binding["plan_path"]).write_text('{"data_root":"changed"}')
    with pytest.raises(ValueError, match="invalid staging plan|binding changed"):
        apply_seed(plan, max_bytes=plan["total_bytes"])
    with pytest.raises(ValueError, match="disjoint"):
        plan_seed(**{**options, "target": options["source"] / "nested"})


def test_seed_current_research_can_pin_prior_data_during_handoff(tmp_path):
    options, data_root, _ = seed_fixture(tmp_path)
    selected = load_snapshot(options["data"])
    table = options["source"] / "raw" / "raw.delta"
    write_deltalake(str(table), pa.table({"value": [3]}), mode="append")
    advanced = create_snapshot(
        tables={"raw": capture_table(table)},
        required_tables=["raw"],
        dataset_version="tiny",
        run_id="advanced-data-fixture",
        code_version="fixture",
        quality_checks={"fixture": "PASS"},
        parents=[selected["snapshot_id"]],
    )
    advanced["tables"]["raw"]["table_path"] = str(options["target"] / "raw" / "raw.delta")
    advanced["manifest_sha256"] = _hash(
        {k: v for k, v in advanced.items() if k != "manifest_sha256"}
    )
    save_snapshot(data_root, advanced)
    atomic_json(
        data_root / "published_current.json",
        {
            "schema_version": "data_snapshot_publication.v1",
            "snapshot_id": advanced["snapshot_id"],
            "manifest_sha256": advanced["manifest_sha256"],
        },
    )
    plan = plan_seed(**options)
    assert plan["heads"][0] == selected["snapshot_id"]
    assert plan["source_current_data_snapshot_id"] == advanced["snapshot_id"]
    assert advanced["snapshot_id"] not in plan["manifests"]
    apply_seed(plan, max_bytes=plan["total_bytes"])
    assert DeltaTable(str(options["target"] / "raw" / "raw.delta")).version() == 1
    assert (
        current_snapshot(options["target"] / "canonical/.data_snapshots")["snapshot_id"]
        == selected["snapshot_id"]
    )
    # A historical Research selection still cannot substitute for its actual current head.
    newer_research = load_snapshot(options["research"])
    newer_research["snapshot_id"] = "f" * 32
    newer_research["manifest_sha256"] = _hash(
        {k: v for k, v in newer_research.items() if k != "manifest_sha256"}
    )
    save_snapshot(options["research"].parent.parent, newer_research)
    atomic_json(
        options["research"].parent.parent / "published_current.json",
        {
            "schema_version": "data_snapshot_publication.v1",
            "snapshot_id": newer_research["snapshot_id"],
            "manifest_sha256": newer_research["manifest_sha256"],
        },
    )
    with pytest.raises(ValueError, match="current accepted Research"):
        plan_seed(**options)


def prepared_seed_fixture(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh

    options, _, _ = seed_fixture(tmp_path)
    data = load_snapshot(options["data"])
    key = "prepared_raw_moex_contract_securities"
    data["tables"][key] = data["tables"].pop("raw")
    data["physical_schemas"][key] = data["physical_schemas"].pop("raw")
    data["required_tables"] = [key]
    data["manifest_sha256"] = _hash({k: v for k, v in data.items() if k != "manifest_sha256"})
    atomic_json(options["data"], data)
    atomic_json(
        options["data"].parent.parent / "published_current.json",
        {k: data[k] for k in ("snapshot_id", "manifest_sha256")}
        | {"schema_version": "data_snapshot_publication.v1"},
    )
    names = ("bounded-parameter-regimes.json", "dated-official-tc-evidence.json")
    for name in names:
        path = options["source"] / "provenance/prepared/economics" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    validated = []

    def registered(root):
        validated.append(root)
        paths = [root / "provenance/prepared/economics" / name for name in names]
        assert all(path.read_text("utf-8") == "{}" for path in paths)
        return {
            "economics_parameter_evidence": dict(
                zip(("regimes_path", "dated_official_tc_path"), map(str, paths), strict=True)
            )
        }

    monkeypatch.setattr(native_history_refresh, "registered_history_refresh_kwargs", registered)
    return options, names, validated


def test_seed_includes_registered_support_and_validates_target(tmp_path, monkeypatch):
    options, names, validated = prepared_seed_fixture(tmp_path, monkeypatch)
    plan = plan_seed(**options)
    assert len(plan["support_sha256"]) == 2
    for name in names:
        relative = f"provenance/prepared/economics/{name}"
        assert plan["files"][relative] == 2
        assert plan["support_sha256"][relative] == hashlib.sha256(b"{}").hexdigest()
    assert validated == [options["source"]]
    apply_seed(plan, max_bytes=plan["total_bytes"])
    assert validated == [options["source"], options["source"], options["target"]]


def test_seed_rejects_same_size_support_mutation(tmp_path, monkeypatch):
    import shutil

    options, names, _ = prepared_seed_fixture(tmp_path, monkeypatch)
    plan = plan_seed(**options)
    copy = shutil.copyfileobj

    def mutate_after_copy(reader, writer):
        copy(reader, writer)
        if Path(reader.name).name == names[0]:
            Path(reader.name).write_text("[]")

    monkeypatch.setattr(shutil, "copyfileobj", mutate_after_copy)
    with pytest.raises(ValueError, match="support.*changed"):
        apply_seed(plan, max_bytes=plan["total_bytes"])
    assert not (options["target"] / ".staging-seed.json").exists()


@pytest.mark.parametrize("advance_after_plan", [False, True])
def test_seed_rejects_historical_prepared_pin_before_copy(
    tmp_path, monkeypatch, advance_after_plan
):
    options, _, validated = prepared_seed_fixture(tmp_path, monkeypatch)
    plan = plan_seed(**options) if advance_after_plan else None
    write_deltalake(
        str(options["source"] / "raw/raw.delta"), pa.table({"value": [3]}), mode="append"
    )
    with pytest.raises(ValueError, match="prepared.*current"):
        if advance_after_plan:
            apply_seed(plan, max_bytes=plan["total_bytes"])
        else:
            plan_seed(**options)
    assert validated == ([options["source"]] if advance_after_plan else [])
    assert [p.name for p in options["target"].iterdir()] == [".ta3000-staging.json"]
