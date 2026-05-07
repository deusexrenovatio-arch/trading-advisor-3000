from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


REQUIRED_FILES = [
    "AGENTS.md",
    "CODEOWNERS",
    ".cursorignore",
    ".githooks/pre-push",
    "Makefile",
    "scripts/install_git_hooks.py",
    "scripts/critical_contours.py",
    "scripts/run_boring_checks.py",
    "scripts/run_loop_gate.py",
    "scripts/validate_solution_intent.py",
    "scripts/validate_critical_contour_closure.py",
    "scripts/validate_docs_links.py",
    "configs/critical_contours.yaml",
    "docs/README.md",
    "docs/DEV_WORKFLOW.md",
    "docs/session_handoff.md",
    "docs/agent/entrypoint.md",
    "docs/agent/domains.md",
    "docs/agent/checks.md",
    "docs/agent/critical-contours.md",
    "docs/agent/runtime.md",
    "docs/agent/skills-routing.md",
    "docs/agent/skills-catalog.md",
]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_phase1_required_files_exist() -> None:
    for rel in REQUIRED_FILES:
        assert (ROOT / rel).exists(), f"Missing required Phase 1 file: {rel}"


def test_agents_uses_neutral_emergency_env_vars() -> None:
    text = read_text(ROOT / "AGENTS.md")
    assert "AI_SHELL_EMERGENCY_MAIN_PUSH" in text
    assert "AI_SHELL_EMERGENCY_MAIN_PUSH_REASON" in text


def test_pre_push_enforces_loop_gate_name() -> None:
    text = read_text(ROOT / ".githooks/pre-push")
    assert "run_loop_gate.py" in text
    assert "AI_SHELL_SKIP_LOOP_GATE" not in text


def test_no_legacy_run_lean_reference_in_phase1_docs() -> None:
    files_to_scan = [
        ROOT / "AGENTS.md",
        ROOT / "docs/README.md",
        ROOT / "docs/DEV_WORKFLOW.md",
        ROOT / "docs/agent/entrypoint.md",
        ROOT / "docs/agent/domains.md",
        ROOT / "docs/agent/checks.md",
        ROOT / "docs/agent/runtime.md",
    ]
    for path in files_to_scan:
        content = read_text(path)
        assert "run_lean_gate.py" not in content, f"Legacy gate mention in {path}"


def test_docs_links_validate() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "scripts/validate_docs_links.py", "--roots", "AGENTS.md", "docs"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
