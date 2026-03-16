from __future__ import annotations

import json
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


def test_task_session_cli_help_lists_lifecycle_commands() -> None:
    result = _run([sys.executable, "scripts/task_session.py", "--help"])
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    output = result.stdout.lower()
    assert "begin" in output
    assert "status" in output
    assert "end" in output


def test_context_router_cli_json_contract() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/context_router.py",
            "--changed-files",
            "AGENTS.md",
            "--format",
            "json",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert "primary_context" in payload
    assert "contexts" in payload


def test_loop_gate_smoke_runtime() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_loop_gate.py",
            "--skip-session-check",
            "--changed-files",
            "docs/README.md",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
