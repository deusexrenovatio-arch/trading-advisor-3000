from __future__ import annotations

import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_pytest_uses_repo_local_base_temp_by_default() -> None:
    payload = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    addopts = (
        payload.get("tool", {})
        .get("pytest", {})
        .get("ini_options", {})
        .get("addopts", "")
    )

    assert isinstance(addopts, str)
    assert "--basetemp=.tmp/pytest" in addopts
