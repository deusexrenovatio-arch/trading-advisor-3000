from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_loop_gate_contract_surface_smoke() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_loop_gate.py",
            "--skip-session-check",
            "--changed-files",
            "plans/items/index.yaml",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "primary_surface=contracts" in result.stdout


def test_pr_gate_docs_smoke() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_pr_gate.py",
            "--skip-session-check",
            "--changed-files",
            "docs/README.md",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_nightly_gate_docs_smoke() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_nightly_gate.py",
            "--changed-files",
            "docs/README.md",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
