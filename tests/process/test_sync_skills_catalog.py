from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)


def _write_skill(root: Path, skill_id: str) -> None:
    skill_dir = root / ".codex" / "skills" / skill_id
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        f"""---
name: {skill_id}
description: Synthetic skill {skill_id}
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-DATA
scope: synthetic scope
routing_triggers:
  - "{skill_id}"
---
""",
        encoding="utf-8",
    )


def test_sync_skills_catalog_generate_and_check(tmp_path: Path) -> None:
    _write_skill(tmp_path, "alpha")
    _write_skill(tmp_path, "beta")
    catalog = tmp_path / "docs" / "agent" / "skills-catalog.md"

    generate = _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "sync_skills_catalog.py"),
            "--skills-root",
            str(tmp_path / ".codex" / "skills"),
            "--catalog-file",
            str(catalog),
        ],
        cwd=tmp_path,
    )
    assert generate.returncode == 0, generate.stdout + "\n" + generate.stderr
    text = catalog.read_text(encoding="utf-8")
    assert "generated-by: scripts/sync_skills_catalog.py" in text
    assert "`alpha`" in text and "`beta`" in text

    check = _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "sync_skills_catalog.py"),
            "--skills-root",
            str(tmp_path / ".codex" / "skills"),
            "--catalog-file",
            str(catalog),
            "--check",
        ],
        cwd=tmp_path,
    )
    assert check.returncode == 0, check.stdout + "\n" + check.stderr


def test_sync_skills_catalog_detects_drift_after_remove_and_rename(tmp_path: Path) -> None:
    _write_skill(tmp_path, "alpha")
    _write_skill(tmp_path, "beta")
    catalog = tmp_path / "docs" / "agent" / "skills-catalog.md"

    _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "sync_skills_catalog.py"),
            "--skills-root",
            str(tmp_path / ".cursor" / "skills"),
            "--catalog-file",
            str(catalog),
        ],
        cwd=tmp_path,
    )

    # remove beta and rename alpha -> gamma without regenerating catalog
    (tmp_path / ".codex" / "skills" / "beta" / "SKILL.md").unlink()
    (tmp_path / ".codex" / "skills" / "beta").rmdir()

    old_dir = tmp_path / ".codex" / "skills" / "alpha"
    new_dir = tmp_path / ".codex" / "skills" / "gamma"
    old_dir.rename(new_dir)
    (new_dir / "SKILL.md").write_text(
        (new_dir / "SKILL.md").read_text(encoding="utf-8").replace("name: alpha", "name: gamma"),
        encoding="utf-8",
    )

    drift = _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "sync_skills_catalog.py"),
            "--skills-root",
            str(tmp_path / ".codex" / "skills"),
            "--catalog-file",
            str(catalog),
            "--check",
        ],
        cwd=tmp_path,
    )
    assert drift.returncode != 0
    assert "drift" in (drift.stdout + drift.stderr).lower()
