from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from gate_common import CommandSpec, scope_validate_command  # noqa: E402


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
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
            "--changed-files",
            "plans/items/index.yaml",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "primary_surface=contracts" in result.stdout
    assert "snapshot_mode=changed-files" in result.stdout
    assert "profile=none" in result.stdout


def test_loop_gate_reports_explicit_profile_metadata() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_loop_gate.py",
            "--skip-session-check",
            "--snapshot-mode",
            "changed-files",
            "--changed-files",
            "plans/items/index.yaml",
            "--profile",
            "ops",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "snapshot_mode=changed-files" in result.stdout
    assert "profile=ops" in result.stdout


def test_loop_gate_emits_deprecation_guidance_for_non_policy_critical_path() -> None:
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
    combined = result.stdout + "\n" + result.stderr
    assert "DEPRECATION implicit snapshot marker fallback is active" in combined
    assert "DEPRECATION implicit profile marker fallback is active" in combined


def test_loop_gate_fails_closed_without_explicit_markers_on_policy_critical_diff() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_loop_gate.py",
            "--skip-session-check",
            "--changed-files",
            "plans/items/index.yaml",
        ]
    )
    assert result.returncode == 2, result.stdout + "\n" + result.stderr
    combined = result.stdout + "\n" + result.stderr
    assert "explicit snapshot marker is required" in combined


def test_pr_gate_docs_smoke() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_pr_gate.py",
            "--skip-session-check",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
            "--changed-files",
            "docs/README.md",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "snapshot_mode=changed-files" in result.stdout
    assert "profile=none" in result.stdout


def test_pr_gate_reports_explicit_profile_metadata() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_pr_gate.py",
            "--skip-session-check",
            "--snapshot-mode",
            "changed-files",
            "--changed-files",
            "docs/README.md",
            "--profile",
            "ops",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "snapshot_mode=changed-files" in result.stdout
    assert "profile=ops" in result.stdout


def test_pr_gate_emits_deprecation_guidance_for_non_policy_critical_path() -> None:
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
    combined = result.stdout + "\n" + result.stderr
    assert "DEPRECATION implicit snapshot marker fallback is active" in combined
    assert "DEPRECATION implicit profile marker fallback is active" in combined


def test_pr_gate_fails_closed_without_explicit_markers_on_policy_critical_diff() -> None:
    result = _run(
        [
            sys.executable,
            "scripts/run_pr_gate.py",
            "--skip-session-check",
            "--changed-files",
            "scripts/run_loop_gate.py",
        ]
    )
    assert result.returncode == 2, result.stdout + "\n" + result.stderr
    combined = result.stdout + "\n" + result.stderr
    assert "explicit snapshot marker is required" in combined


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


def test_scope_validate_command_uses_stdin_for_task_outcomes() -> None:
    scoped = scope_validate_command(
        f"{sys.executable} scripts/validate_task_outcomes.py",
        base_sha=None,
        head_sha=None,
        changed_files=[
            "scripts/codex_governed_entry.py",
            *[f"artifacts/codex/package-intake/{index}/manifest.md" for index in range(300)],
        ],
    )

    assert isinstance(scoped, CommandSpec)
    assert "--stdin" in scoped.command
    assert "--changed-files" not in scoped.command
    assert scoped.stdin_text is not None
    assert "scripts/codex_governed_entry.py" in scoped.stdin_text


def test_scope_validate_command_uses_stdin_for_phase_planning_contract() -> None:
    scoped = scope_validate_command(
        f"{sys.executable} scripts/validate_phase_planning_contract.py",
        base_sha=None,
        head_sha=None,
        changed_files=[
            "docs/codex/contracts/f1-full-closure.execution-contract.md",
            "docs/codex/modules/f1-full-closure.phase-01.md",
        ],
    )

    assert isinstance(scoped, CommandSpec)
    assert "--stdin" in scoped.command
    assert scoped.stdin_text is not None
    assert "docs/codex/contracts/f1-full-closure.execution-contract.md" in scoped.stdin_text


def test_scope_validate_command_uses_stdin_for_legacy_namespace_growth() -> None:
    scoped = scope_validate_command(
        f"{sys.executable} scripts/validate_legacy_namespace_growth.py",
        base_sha=None,
        head_sha=None,
        changed_files=[
            "src/trading_advisor_3000/product_plane/runtime_bridge.py",
            "docs/architecture/README.md",
        ],
    )

    assert isinstance(scoped, CommandSpec)
    assert "--stdin" in scoped.command
    assert scoped.stdin_text is not None
    assert "src/trading_advisor_3000/product_plane/runtime_bridge.py" in scoped.stdin_text


def test_scope_validate_command_uses_stdin_for_surface_pr_matrix_runner() -> None:
    scoped = scope_validate_command(
        f"{sys.executable} scripts/run_surface_pr_matrix.py",
        base_sha=None,
        head_sha=None,
        changed_files=[
            "src/trading_advisor_3000/app/runtime/bootstrap.py",
            "tests/app/unit/test_phase6_fastapi_smoke.py",
        ],
    )

    assert isinstance(scoped, CommandSpec)
    assert "--stdin" in scoped.command
    assert scoped.stdin_text is not None
    assert "src/trading_advisor_3000/app/runtime/bootstrap.py" in scoped.stdin_text


def test_loop_gate_handles_large_scope_from_stdin() -> None:
    changed_files = "\n".join(
        [
            "scripts/codex_governed_entry.py",
            *[f"artifacts/codex/package-intake/{index}/manifest.md" for index in range(300)],
        ]
    )
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_loop_gate.py",
            "--skip-session-check",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
            "--stdin",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        input=changed_files,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
