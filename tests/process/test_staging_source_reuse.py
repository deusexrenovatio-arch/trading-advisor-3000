"""Real signed/Git lineage fixtures; no native producer computation."""

import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402
from test_staging_acceptance_gate import receipt_repository as receipt_repository  # noqa: E402


def manifest(path, value):
    value = copy.deepcopy(value)
    value.pop("manifest_sha256", None)
    value["manifest_sha256"] = control.digest(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return value


@pytest.fixture
def lineage(receipt_repository, tmp_path, monkeypatch):
    root, _, head, private, payload = receipt_repository
    monkeypatch.setattr(control, "ROOT", root)
    private_path = tmp_path / "operator.key"
    private_path.write_bytes(private)
    monkeypatch.setenv("TA3000_STAGING_OPERATOR_PRIVATE_KEY", str(private_path))
    monkeypatch.setenv("TA3000_STAGING_OPERATOR_KEY_ID", "test-issuer")
    paths = {
        "HOST_STATE": tmp_path / "state",
        "DATA_ROOT": tmp_path / "data",
        "CANDIDATE_WORKTREE": root,
    }
    paths["HOST_STATE"].mkdir()
    tables = {
        "raw": {
            "table_id": "native",
            "schema_hash": "a" * 64,
            "layout_version": "b" * 64,
            "table_path": "/ta3000-data/moex-historical/raw.delta",
            "partition_columns": [],
            "delta_version": 31,
            "active_files_sha256": "c" * 64,
            "row_count": 1,
        }
    }
    data_root = paths["DATA_ROOT"] / "canonical/moex/baseline-4y-current/.data_snapshots/manifests"
    data = manifest(
        data_root / ("d" * 32 + ".json"),
        {"snapshot_id": "d" * 32, "tables": tables, "quality_checks": {"qc": "PASS"}},
    )
    research = manifest(
        paths["DATA_ROOT"] / "research.json",
        {
            "snapshot_id": "e" * 32,
            "parents": [data["snapshot_id"]],
            "tables": tables,
            "quality_checks": {"qc": "PASS"},
        },
    )
    payload["outputs"] = {
        "snapshot_id": research["snapshot_id"],
        "manifest_sha256": research["manifest_sha256"],
        "data_snapshot_id": data["snapshot_id"],
        "pins_sha256": control.digest(tables),
    }
    payload["consumer"]["snapshot_id"] = research["snapshot_id"]
    envelope = control.sign_receipt(payload, private, "test-issuer")
    cache = {
        "accepted_main_sha": head,
        "accepted_receipt": envelope,
        "accepted_outputs": control._source_pair(paths, research),
        "completed": {},
    }
    cache_path = paths["HOST_STATE"] / "ta3000-acceptance-reference-cycles.json"
    control._write_reference_cache(cache_path, cache)
    plan = {"candidate": {"base_sha": head}}
    return plan, paths, research, cache, cache_path


def test_next_candidate_and_reference_resume_use_only_latest_accepted_outputs(lineage):
    plan, paths, research, cache, path = lineage
    assert control._accepted_source(plan, paths, research)
    later = manifest(paths["DATA_ROOT"] / "research2.json", {**research, "snapshot_id": "f" * 32})
    with pytest.raises(ValueError, match="signed accepted"):
        control._accepted_source(plan, paths, later)
    pair = control._source_pair(paths, later)
    cache["completed"] = {
        "actual-root": {"status": "PASS", "schedule": {"timestamp": 2}, "outputs": pair}
    }
    control._write_reference_cache(path, cache)
    assert control._accepted_source(plan, paths, later) == pair
    with pytest.raises(ValueError, match="latest successful"):
        control._accepted_source(plan, paths, research)
    reference = {
        "mode": "reference",
        "reference": {"main_sha": cache["accepted_main_sha"], "receipt": cache["accepted_receipt"]},
    }
    assert control._accepted_source(reference, paths, later) == pair
    path.unlink()
    with pytest.raises(ValueError, match="signed accepted"):
        control._accepted_source(reference, paths, later)
    assert control._accepted_source(reference, paths, research)


@pytest.mark.parametrize("damage", ["signature", "unsigned", "failed", "tree", "pins"])
def test_unsigned_failed_or_changed_source_cannot_be_adopted(lineage, damage):
    plan, paths, research, cache, path = lineage
    if damage == "signature":
        cache["accepted_receipt"]["payload"]["changed_windows"] = 2
    elif damage == "failed":
        cache["completed"] = {
            "root": {
                "status": "BLOCKED",
                "schedule": {"timestamp": 2},
                "outputs": cache["accepted_outputs"],
            }
        }
    elif damage == "tree":
        cache["accepted_main_sha"] = cache["accepted_receipt"]["payload"]["candidate"]["base_sha"]
    else:
        research = copy.deepcopy(research)
        research["tables"]["raw"]["delta_version"] += 1
    if damage == "unsigned":
        path.write_text(json.dumps(cache))
    else:
        control._write_reference_cache(path, cache)
    with pytest.raises(ValueError):
        control._accepted_source(plan, paths, research)
