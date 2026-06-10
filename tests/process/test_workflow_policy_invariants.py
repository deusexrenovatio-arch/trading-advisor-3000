from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIVE_POLICY_FILES = (
    ".github/workflows/ci.yml",
    ".github/workflows/branch-diagnostic.yml",
    ".github/workflows/nightly.yml",
    ".github/workflows/dashboard-refresh.yml",
    "AGENTS.md",
    "configs/change_surface_mapping.yaml",
    "docs/DEV_WORKFLOW.md",
    "docs/agent/checks.md",
    "docs/agent/entrypoint.md",
    "docs/agent/runtime.md",
    "docs/agent/skills-routing.md",
    "docs/runbooks/governance-remediation.md",
    "docs/workflows/skill-governance-sync.md",
    "scripts/context_router.py",
    "scripts/run_loop_gate.py",
    "scripts/run_pr_gate.py",
    "scripts/validate_pr_size.py",
    "scripts/run_nightly_gate.py",
    "scripts/run_shell_delivery_operational_proving.py",
)

FORBIDDEN_POLICY_TEXT = (
    "--skip-session-check",
    "--require-session-check",
    "--session-handoff-path",
    "codex_governed_entry.py",
    "codex_governed_bootstrap.py",
    "codex_phase_orchestrator.py",
    "codex_phase_policy.py",
    "build_governed_release_decision.py",
    "orchestration_quality",
    "validate_phase_planning_contract.py",
    "planning-gate-contract.md",
    "docs/codex/contracts",
    "docs/codex/modules",
    "docs/codex/prompts/phases",
    "task_session.py begin",
    "validate_session_handoff.py",
    "Governed/legacy loop",
    "ordinary or governed",
    "route is governed",
    "governed continuation",
    "Start task session:",
    "Otherwise start lifecycle directly",
    "before clarification, repository exploration",
    "before clarification, repo reading",
    "before clarification",
    "if there is even a small chance a Superpowers process skill applies",
    "Superpowers-First",
    "canonical Superpowers baseline",
    "Superpowers process skills",
    "superpowers:",
    "governed pass/block",
    "phase-acceptance-governor",
    "MCP Governance Contract",
    "validate_mcp_config.py",
    "mcp_preflight_smoke.py",
    "deployment/mcp",
    "mcp-wave-rollout",
)

ALLOWED_POLICY_TEXT_BY_FILE = {
    "scripts/validate_pr_size.py": (
        "codex_governed_entry.py",
        "codex_governed_bootstrap.py",
        "codex_phase_orchestrator.py",
        "codex_phase_policy.py",
        "build_governed_release_decision.py",
        "orchestration_quality",
        "validate_phase_planning_contract.py",
        "validate_session_handoff.py",
    ),
}

IGNORED_GENERATED_RUN_ROOTS = (
    "artifacts/codex/orchestration",
    "docs/agent/plugin-eval",
)


def test_live_workflow_policy_retired_defaults_stay_removed() -> None:
    offenders: list[str] = []
    for relative_path in LIVE_POLICY_FILES:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        allowed = ALLOWED_POLICY_TEXT_BY_FILE.get(relative_path, ())
        for forbidden in FORBIDDEN_POLICY_TEXT:
            if forbidden in text and forbidden not in allowed:
                offenders.append(f"{relative_path}: {forbidden}")

    assert offenders == []


def test_ignored_generated_run_roots_are_not_tracked() -> None:
    result = subprocess.run(
        ["git", "ls-files", *IGNORED_GENERATED_RUN_ROOTS],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == ""


def test_hot_docs_pin_local_mcp_default_policy() -> None:
    text = (ROOT / "docs" / "agent" / "runtime.md").read_text(encoding="utf-8")

    assert "`.codex/config.toml` is ignored machine-local state" in text
    assert '`["serena"]`' in text
    assert "not ordinary-chat defaults" in text


def test_pr_ci_has_single_required_pr_context_and_size_gate() -> None:
    workflow_text = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    mapping_text = (ROOT / "configs" / "change_surface_mapping.yaml").read_text(encoding="utf-8")

    assert "pr-lane:" in workflow_text
    assert "loop-lane:" not in workflow_text
    assert "needs: loop-lane" not in workflow_text
    assert "branch-lane:" not in workflow_text
    assert "scripts/run_pr_gate.py" in workflow_text
    assert "scripts/validate_pr_size.py" in mapping_text
