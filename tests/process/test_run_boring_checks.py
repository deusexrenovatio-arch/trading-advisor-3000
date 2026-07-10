from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import run_boring_checks  # noqa: E402
from run_boring_checks import (  # noqa: E402
    _changed_python_targets,
    _mypy_targets,
    _parse_pyproject,
    _python_targets,
    _run_test_audit_matrix_check,
)


def test_changed_python_targets_keep_active_python_files_only(tmp_path: Path) -> None:
    for path_text in (
        "scripts/run_boring_checks.py",
        ".githooks/pre-push",
        "docs/README.md",
        "codex_ai_delivery_shell_package/legacy.py",
        ".tmp/generated.py",
        "tests/process/test_run_boring_checks.py",
    ):
        path = tmp_path / path_text
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    targets = _changed_python_targets(
        tmp_path,
        [
            "scripts/run_boring_checks.py",
            ".githooks/pre-push",
            "docs/README.md",
            "codex_ai_delivery_shell_package/legacy.py",
            ".tmp/generated.py",
            "missing/new_file.py",
            "tests/process/test_run_boring_checks.py",
        ],
    )

    assert targets == [
        "scripts/run_boring_checks.py",
        ".githooks/pre-push",
        "tests/process/test_run_boring_checks.py",
    ]


def test_test_audit_matrix_check_runs_public_sync_cli(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    observed: dict[str, object] = {}

    def fake_run(
        command: list[str],
        *,
        repo_root: Path,
        timeout: int | None,
        env: dict[str, str] | None = None,
    ) -> int:
        observed.update(
            command=command,
            repo_root=repo_root,
            timeout=timeout,
            env=env,
        )
        return 0

    monkeypatch.delenv("AI_SHELL_BORING_SKIP_FAST_TESTS", raising=False)
    monkeypatch.setattr(run_boring_checks, "_run", fake_run)

    assert _run_test_audit_matrix_check(tmp_path, timeout=17) == 0
    assert observed == {
        "command": [sys.executable, "scripts/sync_test_audit_matrix.py", "--check"],
        "repo_root": tmp_path,
        "timeout": 17,
        "env": None,
    }


def test_pyproject_change_runs_config_smoke_target() -> None:
    targets = _python_targets(ROOT, scope="changed", changed_files=["pyproject.toml"])

    assert targets == ["scripts/run_boring_checks.py"]


def test_mypy_targets_skip_extensionless_hooks() -> None:
    targets = _mypy_targets(
        [
            "scripts/run_boring_checks.py",
            ".githooks/pre-push",
            "tests/process/test_run_boring_checks.py",
        ]
    )

    assert targets == [
        "scripts/run_boring_checks.py",
        "tests/process/test_run_boring_checks.py",
    ]


def test_parse_pyproject_reports_malformed_toml(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project\n", encoding="utf-8")

    assert _parse_pyproject(tmp_path) == 1
