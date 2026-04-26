from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)


def _write_skill(
    root: Path,
    skill_id: str,
    classification: str = "KEEP_CORE",
    owner_surface: str = "CTX-DATA",
) -> None:
    skill_dir = root / ".codex" / "skills" / skill_id
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        f"""---
name: {skill_id}
description: Synthetic skill {skill_id}
classification: {classification}
wave: WAVE_1
status: ACTIVE
owner_surface: {owner_surface}
scope: synthetic scope
routing_triggers:
  - "{skill_id}"
---
""",
        encoding="utf-8",
    )


def _write_required_docs(root: Path) -> None:
    (root / "docs" / "agent").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "workflows").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "planning").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "agent" / "skills-routing.md").write_text("# routing\n", encoding="utf-8")
    (root / "docs" / "workflows" / "skill-governance-sync.md").write_text("# workflow\n", encoding="utf-8")
    (root / "docs" / "planning" / "skills-roadmap.md").write_text("# roadmap\n", encoding="utf-8")
    (root / ".cursorignore").write_text(".cursor/skills/**\n.codex/skills/**\n", encoding="utf-8")


def test_validate_skills_strict_passes_on_repository_runtime() -> None:
    result = _run(
        [sys.executable, "scripts/validate_skills.py", "--strict"],
        cwd=ROOT,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_validate_skills_strict_detects_forbidden_class(tmp_path: Path) -> None:
    _write_required_docs(tmp_path)
    _write_skill(tmp_path, "alpha", classification="KEEP_OPTIONAL")
    (tmp_path / "docs" / "agent" / "skills-catalog.md").write_text(
        "# Skills Catalog\n<!-- generated-by: scripts/sync_skills_catalog.py -->\n",
        encoding="utf-8",
    )

    result = _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "validate_skills.py"),
            "--skills-root",
            str(tmp_path / ".codex" / "skills"),
            "--catalog-file",
            str(tmp_path / "docs" / "agent" / "skills-catalog.md"),
            "--governance-doc",
            str(tmp_path / "docs" / "workflows" / "skill-governance-sync.md"),
            "--routing-doc",
            str(tmp_path / "docs" / "agent" / "skills-routing.md"),
            "--roadmap-doc",
            str(tmp_path / "docs" / "planning" / "skills-roadmap.md"),
            "--strict",
        ],
        cwd=tmp_path,
    )
    assert result.returncode != 0
    text = (result.stdout + result.stderr).lower()
    assert "non-baseline classification" in text or "unexpected runtime skills" in text


def test_validate_skills_strict_detects_catalog_drift(tmp_path: Path) -> None:
    _write_required_docs(tmp_path)
    _write_skill(tmp_path, "alpha")
    catalog = tmp_path / "docs" / "agent" / "skills-catalog.md"
    _run(
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
    catalog.write_text(catalog.read_text(encoding="utf-8") + "\nmanual-drift\n", encoding="utf-8")

    result = _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "validate_skills.py"),
            "--skills-root",
            str(tmp_path / ".codex" / "skills"),
            "--catalog-file",
            str(catalog),
            "--governance-doc",
            str(tmp_path / "docs" / "workflows" / "skill-governance-sync.md"),
            "--routing-doc",
            str(tmp_path / "docs" / "agent" / "skills-routing.md"),
            "--roadmap-doc",
            str(tmp_path / "docs" / "planning" / "skills-roadmap.md"),
            "--strict",
        ],
        cwd=tmp_path,
    )
    assert result.returncode != 0
    assert "catalog drift" in (result.stdout + result.stderr).lower()


def test_validate_skills_strict_rejects_generic_repo_local_owner_surface(tmp_path: Path) -> None:
    _write_required_docs(tmp_path)
    _write_skill(tmp_path, "alpha", owner_surface="CTX-OPS")
    catalog = tmp_path / "docs" / "agent" / "skills-catalog.md"
    _run(
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

    result = _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "validate_skills.py"),
            "--skills-root",
            str(tmp_path / ".codex" / "skills"),
            "--catalog-file",
            str(catalog),
            "--governance-doc",
            str(tmp_path / "docs" / "workflows" / "skill-governance-sync.md"),
            "--routing-doc",
            str(tmp_path / "docs" / "agent" / "skills-routing.md"),
            "--roadmap-doc",
            str(tmp_path / "docs" / "planning" / "skills-roadmap.md"),
            "--strict",
        ],
        cwd=tmp_path,
    )
    assert result.returncode != 0
    assert "product-plane/data/compute scoped" in (result.stdout + result.stderr)
