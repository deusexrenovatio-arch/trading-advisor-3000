from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from run_boring_checks import _changed_python_targets, _mypy_targets, _python_targets  # noqa: E402


def test_changed_python_targets_keep_active_python_files_only() -> None:
    targets = _changed_python_targets(
        ROOT,
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
