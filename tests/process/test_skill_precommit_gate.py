from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)


def test_skill_precommit_gate_allows_content_only_skill_update() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/skill_precommit_gate.py",
            "--changed-files",
            ".codex/skills/ta3000-data-plane-proof/SKILL.md",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_skill_precommit_gate_passes_when_required_docs_present() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/skill_precommit_gate.py",
            "--changed-files",
            ".codex/skills/ta3000-data-plane-proof/SKILL.md",
            "docs/agent/skills-catalog.md",
            "docs/agent/skills-routing.md",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
