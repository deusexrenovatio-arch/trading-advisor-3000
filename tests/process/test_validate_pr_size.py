from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_pr_size import DiffEntry, build_report  # noqa: E402


def test_validate_pr_size_allows_small_reviewable_scope() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/validate_pr_size.py",
            "--changed-files",
            "scripts/validate_pr_size.py",
            "docs/agent/checks.md",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "PR size gate: OK" in result.stdout


def test_validate_pr_size_fails_on_too_many_reviewable_files() -> None:
    changed_files = [
        f"src/trading_advisor_3000/product_plane/runtime/file_{index}.py" for index in range(101)
    ]
    result = subprocess.run(
        [
            sys.executable,
            "scripts/validate_pr_size.py",
            "--changed-files",
            *changed_files,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, result.stdout + "\n" + result.stderr
    assert "reviewable files: 101 / 100" in result.stdout


def test_validate_pr_size_excludes_deleted_cold_generated_artifacts() -> None:
    report = build_report(
        [
            DiffEntry(
                path="artifacts/codex/orchestration/old-run/state.json",
                additions=0,
                deletions=5000,
                status="D",
            ),
            DiffEntry(path="scripts/run_pr_gate.py", additions=10, deletions=5, status="M"),
        ]
    )

    assert report["status"] == "pass"
    assert report["reviewable_files"] == 1
    assert report["reviewable_line_changes"] == 15
    assert report["excluded_cold_generated_deletes"] == 1


def test_validate_pr_size_excludes_deleted_active_lifecycle_notes() -> None:
    report = build_report(
        [
            DiffEntry(
                path="docs/tasks/active/TASK-2026-05-07-stale-route.md",
                additions=0,
                deletions=900,
                status="D",
            ),
            DiffEntry(path="scripts/run_pr_gate.py", additions=20, deletions=5, status="M"),
        ]
    )

    assert report["status"] == "pass"
    assert report["reviewable_files"] == 1
    assert report["reviewable_line_changes"] == 25
    assert report["excluded_cold_generated_deletes"] == 1
