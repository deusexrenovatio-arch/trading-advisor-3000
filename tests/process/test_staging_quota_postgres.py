"""Bootstrap refuses destructive recovery or exposure of privileged credentials."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "quota_bootstrap", ROOT / "scripts/bootstrap_staging_quota.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_metadata_plan_creates_no_credentials_or_database(tmp_path, monkeypatch):
    commands = []

    def metadata(sql, database="postgres"):
        commands.append(sql)
        return json.dumps({"database": False, "owner": False, "writer": False})

    monkeypatch.setattr(MODULE, "admin_sql", metadata)
    result = MODULE.bootstrap(tmp_path / "quota", tmp_path / "private", apply=False)
    assert result["status"] == "PLANNED"
    assert len(commands) == 1 and commands[0].startswith("SELECT")
    assert not list(tmp_path.iterdir())


def test_partial_existing_resources_are_never_dropped_or_rotated(tmp_path, monkeypatch):
    commands = []

    def metadata(sql, database="postgres"):
        commands.append(sql)
        return json.dumps({"database": True, "owner": True, "writer": False})

    monkeypatch.setattr(MODULE, "admin_sql", metadata)
    with pytest.raises(ValueError, match="complete protected"):
        MODULE.bootstrap(tmp_path / "quota", tmp_path / "private", apply=True)
    assert len(commands) == 1 and commands[0].startswith("SELECT")


def test_owner_secret_cannot_be_under_candidate_quota_mount(tmp_path):
    with pytest.raises(ValueError, match="outside mounted quota"):
        MODULE.bootstrap(tmp_path / "quota", tmp_path / "quota/private", apply=True)


def test_existing_credential_file_is_never_replaced(tmp_path):
    path = tmp_path / "credentials.json"
    path.write_text("original", encoding="utf-8")
    with pytest.raises(FileExistsError):
        MODULE.write_new(path, {"replacement": True})
    assert path.read_text("utf-8") == "original"
