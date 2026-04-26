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


def _assert_ok(command: list[str]) -> None:
    result = _run(command)
    if result.returncode != 0:
        raise AssertionError(result.stdout + "\n" + result.stderr)


def test_validate_plans_registry_passes() -> None:
    _assert_ok([sys.executable, "scripts/validate_plans.py"])


def test_validate_agent_memory_passes() -> None:
    _assert_ok([sys.executable, "scripts/validate_agent_memory.py"])


def test_validate_task_outcomes_passes() -> None:
    _assert_ok([sys.executable, "scripts/validate_task_outcomes.py"])


def test_validate_process_regressions_passes() -> None:
    _assert_ok([sys.executable, "scripts/validate_process_regressions.py"])


def test_validate_architecture_policy_passes() -> None:
    _assert_ok([sys.executable, "scripts/validate_architecture_policy.py"])


def test_validate_agent_contexts_passes() -> None:
    _assert_ok([sys.executable, "scripts/validate_agent_contexts.py"])


def test_validate_skills_passes() -> None:
    _assert_ok([sys.executable, "scripts/validate_skills.py"])


def test_skills_corpus_is_cold_by_default() -> None:
    text = (ROOT / ".cursorignore").read_text(encoding="utf-8")
    assert ".cursor/skills/**" in text
    assert ".codex/skills/**" in text


def test_skills_routing_declares_targeted_loading() -> None:
    text = (ROOT / "docs/agent/skills-routing.md").read_text(encoding="utf-8").lower()
    assert "cold-by-default" in text
    assert "repo-local skill" in text


def test_autonomy_kpi_report_runs() -> None:
    output = ROOT / ".tmp" / "autonomy-kpi-architecture-smoke.md"
    _assert_ok([sys.executable, "scripts/autonomy_kpi_report.py", "--output", str(output)])
