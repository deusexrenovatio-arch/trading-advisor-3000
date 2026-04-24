from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

PHASE_REQUIRED_FILES = [
    "scripts/task_session.py",
    "scripts/critical_contours.py",
    "scripts/validate_task_request_contract.py",
    "scripts/validate_solution_intent.py",
    "scripts/validate_critical_contour_closure.py",
    "scripts/validate_session_handoff.py",
    "scripts/context_router.py",
    "scripts/validate_agent_contexts.py",
    "scripts/compute_change_surface.py",
    "scripts/run_loop_gate.py",
    "scripts/run_pr_gate.py",
    "scripts/run_nightly_gate.py",
    "scripts/sync_state_layout.py",
    "scripts/validate_plans.py",
    "scripts/validate_agent_memory.py",
    "scripts/validate_task_outcomes.py",
    "scripts/validate_process_regressions.py",
    "scripts/validate_skills.py",
    "scripts/validate_architecture_policy.py",
    "scripts/run_shell_delivery_operational_proving.py",
    "docs/checklists/task-request-contract.md",
    "docs/agent/critical-contours.md",
    "docs/checklists/first-time-right-gate.md",
    "docs/architecture/product-plane/shell-delivery-operational-proving.md",
    "docs/checklists/app/shell-delivery-operational-proving-acceptance-checklist.md",
    "docs/checklists/app/data-integration-closure-passport.md",
    "docs/checklists/app/runtime-publication-closure-passport.md",
    "docs/runbooks/app/shell-delivery-operational-proving-runbook.md",
    "docs/agent-contexts/CTX-DOMAIN.md",
    "docs/agent-contexts/CTX-EXTERNAL-SOURCES.md",
    "docs/architecture/README.md",
    "docs/architecture/adr/0001-shell-boundaries.md",
    "configs/critical_contours.yaml",
    "plans/items/index.yaml",
    "plans/PLANS.yaml",
    "memory/agent_memory.yaml",
    "memory/task_outcomes.yaml",
    ".github/workflows/ci.yml",
]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _gate_smoke_changed_files() -> list[str]:
    handoff_path = ROOT / "docs" / "session_handoff.md"
    if not handoff_path.exists():
        return ["AGENTS.md"]

    lines = handoff_path.read_text(encoding="utf-8").splitlines()
    note_path = ""
    status = ""
    for raw in lines:
        stripped = raw.strip()
        lowered = stripped.lower()
        if lowered.startswith("- path:") and ":" in stripped:
            note_path = stripped.split(":", 1)[1].strip().strip("`")
        if lowered.startswith("- status:") and ":" in stripped:
            status = stripped.split(":", 1)[1].strip().strip("`").lower()

    normalized_note = note_path.replace("\\", "/").lower()
    if note_path and (
        normalized_note.startswith("docs/tasks/archive/")
        or status in {"completed", "archived"}
    ):
        return ["docs/session_handoff.md", note_path]

    return ["AGENTS.md"]


def test_phase_required_files_exist() -> None:
    for rel in PHASE_REQUIRED_FILES:
        assert (ROOT / rel).exists(), f"Missing phase-required file: {rel}"


def test_no_legacy_gate_alias_references() -> None:
    scan_paths = [
        ROOT / "AGENTS.md",
        ROOT / "docs",
        ROOT / "scripts",
        ROOT / ".githooks" / "pre-push",
    ]
    for path in scan_paths:
        if path.is_file():
            files = [path]
        else:
            files = [item for item in path.rglob("*") if item.is_file() and item.suffix in {".md", ".py", ""}]
        for item in files:
            text = item.read_text(encoding="utf-8")
            assert "run_lean_gate.py" not in text, f"Legacy gate alias in {item}"


def test_gate_stack_smoke() -> None:
    changed_files = _gate_smoke_changed_files()
    commands = [
        [
            sys.executable,
            "scripts/run_loop_gate.py",
            "--skip-session-check",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
            "--changed-files",
            *changed_files,
        ],
        [
            sys.executable,
            "scripts/run_pr_gate.py",
            "--skip-session-check",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
            "--changed-files",
            *changed_files,
        ],
        [
            sys.executable,
            "scripts/run_nightly_gate.py",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
            "--changed-files",
            *changed_files,
        ],
    ]
    for command in commands:
        result = _run(command)
        assert result.returncode == 0, result.stdout + "\n" + result.stderr
