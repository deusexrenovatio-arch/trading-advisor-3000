from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from run_boring_checks import (  # noqa: E402
    _changed_python_targets,
    _mypy_targets,
    _parse_pyproject,
    _python_targets,
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
