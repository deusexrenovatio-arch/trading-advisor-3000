from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)


def test_validate_codeowners_passes_on_repository_policy() -> None:
    result = _run([sys.executable, "scripts/validate_codeowners.py"], cwd=ROOT)
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_validate_codeowners_detects_unmapped_surface(tmp_path: Path) -> None:
    (tmp_path / "docs" / "agent").mkdir(parents=True, exist_ok=True)
    (tmp_path / "docs" / "agent" / "entrypoint.md").write_text("# test\n", encoding="utf-8")
    (tmp_path / "CODEOWNERS").write_text("* @team/all\n", encoding="utf-8")
    (tmp_path / "policy.yaml").write_text(
        """version: 1
required_patterns:
  - "*"
  - "/docs/agent/"
significant_paths:
  - docs/agent/
owner_token_regex: '^(@[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)?|[^@\\s]+@[^@\\s]+)$'
""",
        encoding="utf-8",
    )

    result = _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "validate_codeowners.py"),
            "--path",
            str(tmp_path / "CODEOWNERS"),
            "--policy",
            str(tmp_path / "policy.yaml"),
            "--repo-root",
            str(tmp_path),
        ],
        cwd=tmp_path,
    )
    assert result.returncode != 0
    text = (result.stdout + result.stderr).lower()
    assert "missing required codeowners pattern" in text or "unmapped governance surface" in text
