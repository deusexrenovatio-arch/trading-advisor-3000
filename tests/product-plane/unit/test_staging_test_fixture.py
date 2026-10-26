from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.common import staging_test_fixture as owner
from trading_advisor_3000.product_plane.data_plane.moex import staging_test_fixture as builder


@pytest.fixture(scope="module")
def prepared(tmp_path_factory):
    root = tmp_path_factory.mktemp("native-input-fixture") / "fixture"
    descriptor = builder.prepare_fixture(root)
    return root, descriptor


def test_fixture_has_native_pins_and_bounded_synthetic_inputs(prepared):
    root, descriptor = prepared
    assert owner.verify_fixture(root, descriptor["manifest_sha256"]) == descriptor
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["synthetic"] is True
    assert descriptor["timeframes"] == ["15m", "1h", "4h", "1d"]
    assert descriptor["warmup_bars"] == 512
    assert len(manifest["files"]) + 1 <= owner.MAX_FILES
    assert sum(v["bytes"] for v in manifest["files"].values()) < owner.MAX_BYTES
    assert sum(v["row_count"] for v in manifest["tables"].values()) <= owner.MAX_ROWS
    assert manifest["tables"]["canonical_bars"]["row_count"] == 4096
    assert manifest["tables"]["raw_moex_history"]["row_count"] == 14400
    assert descriptor["raw_1m_coverage"]["full_history"] is False
    assert all(v["delta_version"] == 0 for v in manifest["tables"].values())
    assert all(
        v["table_path"].startswith(owner.LOGICAL_ROOT + "/") for v in manifest["tables"].values()
    )
    assert not any("research" in key for key in manifest["tables"])


def test_input_builder_aligns_groups_and_declares_real_minute_coverage():
    rows = builder._inputs(3)
    bars, provenance = rows["canonical_bars"], rows["canonical_bar_provenance"]
    assert len(bars) == len(provenance) == 24
    groups = {
        (i, tf): [r for r in provenance if r["instrument_id"] == i and r["timeframe"] == tf]
        for i in owner.INSTRUMENTS
        for tf in owner.TIMEFRAMES
    }
    assert all(len(group) == 3 for group in groups.values())
    assert {max(r["bar_end_ts"] for r in group) for group in groups.values()} == {
        "2026-03-16T21:00:00Z"
    }
    assert {r["timeframe"] for r in rows["raw_moex_history"]} == {"1m"}


@pytest.mark.parametrize("change", ["bytes", "extra", "empty-directory", "missing", "wrong-hash"])
def test_fixture_rejects_file_tampering(prepared, tmp_path, change):
    source, descriptor = prepared
    target = tmp_path / "fixture"
    shutil.copytree(source, target)
    manifest = json.loads((target / "manifest.json").read_text())
    file = target / next(iter(manifest["files"]))
    if change == "bytes":
        file.write_bytes(file.read_bytes() + b" ")
    elif change == "extra":
        (target / "extra").write_text("unbound")
    elif change == "empty-directory":
        (target / "empty-extra").mkdir()
    elif change == "missing":
        file.unlink()
    else:
        descriptor = {**descriptor, "manifest_sha256": "0" * 64}
    with pytest.raises(ValueError):
        owner.verify_fixture(target, descriptor["manifest_sha256"])


@pytest.mark.parametrize("change", ["caps", "traversal", "delta-pin", "coverage"])
def test_fixture_rejects_rebound_metadata(prepared, tmp_path, change):
    source, _ = prepared
    target = tmp_path / "fixture"
    shutil.copytree(source, target)
    file = target / "manifest.json"
    manifest = json.loads(file.read_text())
    if change == "caps":
        manifest["limits"]["rows"] += 1
    elif change == "traversal":
        manifest["data_manifest_path"] = owner.LOGICAL_ROOT + "/../outside.json"
    elif change == "coverage":
        manifest["raw_1m_coverage"]["full_history"] = True
    else:
        manifest["tables"]["canonical_bars"]["delta_version"] += 1
    file.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        owner.verify_fixture(target, hashlib.sha256(file.read_bytes()).hexdigest())


def test_prepare_never_overwrites(prepared):
    root, _ = prepared
    with pytest.raises(ValueError, match="must be new"):
        builder.prepare_fixture(root)


def _branch_plan():
    return {
        "stage": "branch",
        "attempt_kind": "small",
        "mode": "candidate",
        "root_job": "research_data_prep_job",
        "require_scheduled": False,
        "jobs": {"research_data_prep_job": {"upstream_job": None, "max_runs": 1}},
        "native_steps": dict(owner._NATIVE_CAPS),
        "source": {
            "fixture": {
                "manifest_path": owner.LOGICAL_ROOT + "/manifest.json",
                "manifest_sha256": "a" * 64,
            }
        },
    }


@pytest.mark.parametrize(
    "change",
    ["stage", "attempt_kind", "root_job", "manifest_path", "mode", "require_scheduled", "jobs"],
)
def test_branch_binding_rejects_other_routes(change):
    plan = _branch_plan()
    if change == "manifest_path":
        plan["source"]["fixture"][change] = "/data/fixture/manifest.json"
    else:
        plan[change] = "other"
    with pytest.raises(ValueError, match="protected synthetic fixture"):
        owner.validate_branch_fixture(plan)


def test_branch_binding_uses_only_fixed_control_directory(monkeypatch):
    sentinel = {
        "verified": True,
        "canonical_dir": owner.LOGICAL_ROOT + "/canonical",
        "raw_dir": owner.LOGICAL_ROOT + "/raw",
    }
    calls = []

    def verify(path, sha):
        calls.append((path, sha))
        return sentinel

    monkeypatch.setattr(owner, "verify_fixture", verify)
    assert owner.validate_branch_fixture(_branch_plan()) is sentinel
    assert calls == [(Path(owner.LOGICAL_ROOT), "a" * 64)]


@pytest.mark.parametrize("valid_role", [True, False])
def test_host_fixture_root_keeps_role_checks_before_files(tmp_path, monkeypatch, valid_role):
    calls = []
    descriptor = {
        "canonical_dir": owner.LOGICAL_ROOT + "/canonical",
        "raw_dir": owner.LOGICAL_ROOT + "/raw",
    }

    def verify(path, sha):
        calls.append((path, sha))
        return descriptor

    monkeypatch.setattr(owner, "verify_fixture", verify)
    plan = _branch_plan()
    if not valid_role:
        plan["stage"] = "acceptance"
        with pytest.raises(ValueError, match="protected synthetic fixture"):
            owner.validate_branch_fixture(plan, fixture_root=tmp_path / "fixture")
        assert calls == []
    else:
        assert owner.validate_branch_fixture(plan, fixture_root=tmp_path / "fixture") == descriptor
        assert calls == [(tmp_path / "fixture", "a" * 64)]


@pytest.mark.parametrize("change", ["extra-owner", "too-many", "boolean", "missing"])
def test_branch_fixture_native_caps_are_finite(change):
    plan = _branch_plan()
    caps = plan["native_steps"]
    if change == "extra-owner":
        caps["run_moex_baseline_update"] = 1
    elif change == "too-many":
        caps["materialize_research_dataset"] = 3
    elif change == "boolean":
        caps["materialize_indicator_frames"] = True
    else:
        caps.pop("materialize_indicator_frames")
    with pytest.raises(ValueError, match="native caps"):
        owner.validate_branch_fixture(plan)


def test_fixture_prepare_failure_does_not_install_destination(tmp_path, monkeypatch):
    def blocked(**kwargs):
        raise RuntimeError("fixture input writer failed")

    monkeypatch.setattr(builder, "write_delta_table_rows", blocked)
    with pytest.raises(RuntimeError, match="writer failed"):
        builder.prepare_fixture(tmp_path / "fixture")
    assert list(tmp_path.iterdir()) == []


def test_inventory_rejects_hard_links(tmp_path):
    import os

    source = tmp_path / "one"
    source.write_text("fixture")
    os.link(source, tmp_path / "two")
    with pytest.raises(ValueError, match="hard links"):
        owner.fixture_inventory(tmp_path)


def test_inventory_rejects_byte_cap_before_hashing(tmp_path):
    file = tmp_path / "too-large"
    with file.open("wb") as stream:
        stream.truncate(owner.MAX_BYTES + 1)
    with pytest.raises(ValueError, match="cap"):
        owner.fixture_inventory(tmp_path)


def test_inventory_rejects_file_cap(tmp_path):
    for index in range(owner.MAX_FILES + 1):
        (tmp_path / str(index)).write_bytes(b"")
    with pytest.raises(ValueError, match="cap"):
        owner.fixture_inventory(tmp_path)


def test_inventory_rejects_reparse_metadata(tmp_path, monkeypatch):
    from types import SimpleNamespace

    (tmp_path / "link").write_text("not followed")
    monkeypatch.setattr(
        Path, "lstat", lambda _: SimpleNamespace(st_mode=0, st_file_attributes=0x400)
    )
    with pytest.raises(ValueError, match="links/reparse"):
        owner.fixture_inventory(tmp_path)
