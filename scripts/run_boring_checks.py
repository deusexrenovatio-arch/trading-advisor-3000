#!/usr/bin/env python
"""Small boring engineering checks for changed or full repository scope."""

from __future__ import annotations

import argparse
import compileall
import os
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path

from gate_common import collect_changed_files

COLD_PREFIXES = (
    ".runlogs/",
    ".tmp/",
    ".tmp_openspace/",
    "artifacts/",
    "codex_ai_delivery_shell_package/",
    "docs/tasks/archive/",
    "memory/",
    "plans/",
)
ACTIVE_ROOTS = ("src", "scripts", "tests")
PYTHON_SUFFIXES = (".py", ".pyi")
PYTHON_ENTRYPOINTS = (".githooks/pre-push",)
PYPROJECT = "pyproject.toml"
IMPORT_LINTER_CONFIG = ".importlinter"
CONFIG_SMOKE_TARGET = "scripts/run_boring_checks.py"
FAST_TEST_TARGETS = ("tests/process", "tests/architecture")
DOCVET_PREFIXES = ("src/", "scripts/")
DOCVET_MINIMUM_PYTHON = (3, 12)


def _normalize(path_text: str) -> str:
    normalized = path_text.replace("\\", "/").strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _is_cold_path(path_text: str) -> bool:
    marker = _normalize(path_text).lower()
    return any(marker.startswith(prefix) for prefix in COLD_PREFIXES)


def _existing_changed_files(repo_root: Path, changed_files: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for path_text in changed_files:
        marker = _normalize(path_text)
        if not marker or _is_cold_path(marker) or marker.lower() in seen:
            continue
        if (repo_root / marker).exists():
            seen.add(marker.lower())
            normalized.append(marker)
    return normalized


def _all_python_targets(repo_root: Path) -> list[str]:
    targets: list[str] = []
    for root_text in ACTIVE_ROOTS:
        root = repo_root / root_text
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix in PYTHON_SUFFIXES:
                targets.append(path.relative_to(repo_root).as_posix())
    for entrypoint in PYTHON_ENTRYPOINTS:
        if (repo_root / entrypoint).exists():
            targets.append(entrypoint)
    return targets


def _changed_python_targets(repo_root: Path, changed_files: list[str]) -> list[str]:
    return [
        path_text
        for path_text in _existing_changed_files(repo_root, changed_files)
        if path_text.endswith(PYTHON_SUFFIXES) or path_text in PYTHON_ENTRYPOINTS
    ]


def _python_targets(repo_root: Path, *, scope: str, changed_files: list[str]) -> list[str]:
    if scope == "all":
        return _all_python_targets(repo_root)

    targets = _changed_python_targets(repo_root, changed_files)
    normalized = {_normalize(path_text).lower() for path_text in changed_files}
    smoke_target = repo_root / CONFIG_SMOKE_TARGET
    if PYPROJECT in normalized and CONFIG_SMOKE_TARGET not in targets and smoke_target.exists():
        targets.append(CONFIG_SMOKE_TARGET)
    return targets


def _mypy_targets(python_targets: list[str]) -> list[str]:
    return [path_text for path_text in python_targets if path_text.endswith(PYTHON_SUFFIXES)]


def _docvet_targets(python_targets: list[str]) -> list[str]:
    return [
        path_text
        for path_text in _mypy_targets(python_targets)
        if path_text.startswith(DOCVET_PREFIXES)
    ]


def _is_changed(path_text: str, changed_files: list[str]) -> bool:
    marker = _normalize(path_text).lower()
    return any(_normalize(candidate).lower() == marker for candidate in changed_files)


def _should_run_import_linter(
    repo_root: Path,
    *,
    scope: str,
    changed_files: list[str],
    python_targets: list[str],
) -> bool:
    if scope == "all":
        return True
    if python_targets:
        return True
    return _is_changed(IMPORT_LINTER_CONFIG, changed_files) or _is_changed(PYPROJECT, changed_files)


def _parse_pyproject(repo_root: Path) -> int:
    try:
        with (repo_root / PYPROJECT).open("rb") as file:
            tomllib.load(file)
    except tomllib.TOMLDecodeError as exc:
        print(f"[boring] pyproject.toml parse FAILED: {exc}")
        return 1
    print("[boring] pyproject.toml parse OK")
    return 0


def _run(
    command: list[str],
    *,
    repo_root: Path,
    timeout: int | None,
    env: dict[str, str] | None = None,
) -> int:
    printable = " ".join(command)
    print(f"[boring] {printable}")
    try:
        return subprocess.run(
            command,
            cwd=repo_root,
            check=False,
            env=env,
            timeout=timeout,
        ).returncode
    except subprocess.TimeoutExpired:
        print(f"[boring] command timed out after {timeout}s")
        return 124


def _with_src_pythonpath(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src_path if not existing else os.pathsep.join([src_path, existing])
    return env


def _required_tool(name: str) -> str | None:
    executable = shutil.which(name)
    if executable:
        return executable
    print(f"[boring] {name} missing; install the project quality extras before running this gate.")
    return None


def _run_import_linter(
    repo_root: Path,
    scope: str,
    changed_files: list[str],
    python_targets: list[str],
    *,
    timeout: int | None,
) -> int:
    if not _should_run_import_linter(
        repo_root,
        scope=scope,
        changed_files=changed_files,
        python_targets=python_targets,
    ):
        print("[boring] No import-linter targets.")
        return 0

    config_path = repo_root / IMPORT_LINTER_CONFIG
    if not config_path.exists():
        print(f"[boring] import-linter config missing: {IMPORT_LINTER_CONFIG}")
        return 1
    executable = _required_tool("lint-imports")
    if executable is None:
        return 1
    return _run(
        [executable, "--config", str(config_path), "--no-cache"],
        repo_root=repo_root / "src",
        timeout=timeout,
        env=_with_src_pythonpath(repo_root),
    )


def _run_docvet(repo_root: Path, python_targets: list[str], *, timeout: int | None) -> int:
    targets = _docvet_targets(python_targets)
    if not targets:
        print("[boring] No docvet targets.")
        return 0
    if sys.version_info < DOCVET_MINIMUM_PYTHON:
        print("[boring] docvet requires Python 3.12+; rerun this gate with Python 3.12 or newer.")
        return 1
    executable = _required_tool("docvet")
    if executable is None:
        return 1
    return _run(
        [executable, "check", "--quiet", *targets],
        repo_root=repo_root,
        timeout=timeout,
        env=_with_src_pythonpath(repo_root),
    )


def _run_ruff(
    repo_root: Path,
    python_targets: list[str],
    *,
    fix: bool,
    timeout: int | None,
) -> int:
    if not python_targets:
        print("[boring] No Python targets for ruff.")
        return 0

    format_command = [sys.executable, "-m", "ruff", "format", *python_targets]
    if not fix:
        format_command.insert(4, "--check")
    code = _run(format_command, repo_root=repo_root, timeout=timeout)
    if code != 0:
        return code

    check_command = [sys.executable, "-m", "ruff", "check", *python_targets]
    if fix:
        check_command.insert(4, "--fix")
    return _run(check_command, repo_root=repo_root, timeout=timeout)


def _run_compileall(repo_root: Path, python_targets: list[str]) -> int:
    if not python_targets:
        print("[boring] No Python targets for compileall.")
        return 0

    failed = [
        path_text
        for path_text in python_targets
        if not compileall.compile_file(str(repo_root / path_text), quiet=1)
    ]
    if failed:
        print("[boring] compileall FAILED")
        for path_text in failed:
            print(f"- {path_text}")
        return 1
    print(f"[boring] compileall OK (files={len(python_targets)})")
    return 0


def _run_fast_pytest(repo_root: Path, *, timeout: int | None) -> int:
    if os.getenv("AI_SHELL_BORING_SKIP_FAST_TESTS") == "1":
        print("[boring] fast pytest skipped for re-entrant gate subprocess")
        return 0
    env = os.environ.copy()
    env["AI_SHELL_BORING_SKIP_FAST_TESTS"] = "1"
    return _run(
        [
            sys.executable,
            "-m",
            "pytest",
            *FAST_TEST_TARGETS,
            "-q",
            "--basetemp=.tmp/pytest-boring",
        ],
        repo_root=repo_root,
        timeout=timeout,
        env=env,
    )


def _run_mypy(
    repo_root: Path,
    scope: str,
    python_targets: list[str],
    *,
    timeout: int | None,
) -> int:
    if scope == "all":
        targets = list(ACTIVE_ROOTS)
    else:
        targets = _mypy_targets(python_targets)
    if not targets:
        print("[boring] No Python targets for mypy.")
        return 0
    return _run([sys.executable, "-m", "mypy", *targets], repo_root=repo_root, timeout=timeout)


def _changed_files_for_args(args: argparse.Namespace) -> list[str]:
    default_changed_scope = (
        args.scope == "changed"
        and not args.from_git
        and not args.stdin
        and not args.changed_files
        and not args.base_ref
        and not args.head_ref
    )
    return collect_changed_files(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=bool(args.from_git or default_changed_scope),
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )


def _run_profile(repo_root: Path, args: argparse.Namespace, python_targets: list[str]) -> int:
    steps: list[Callable[[Path], int]] = [_parse_pyproject]
    if args.profile in {"quick", "code", "full"}:
        steps.append(
            lambda root: _run_import_linter(
                root,
                args.scope,
                list(args.changed_files),
                python_targets,
                timeout=args.timeout,
            )
        )
        steps.append(lambda root: _run_docvet(root, python_targets, timeout=args.timeout))
        steps.append(
            lambda root: _run_ruff(
                root,
                python_targets,
                fix=bool(args.fix),
                timeout=args.timeout,
            )
        )
        steps.append(lambda root: _run_compileall(root, python_targets))
    if args.profile in {"quick", "full"}:
        steps.append(lambda root: _run_fast_pytest(root, timeout=args.timeout))
    if args.profile in {"type", "full"}:
        steps.append(lambda root: _run_mypy(root, args.scope, python_targets, timeout=args.timeout))

    for step in steps:
        code = step(repo_root)
        if code != 0:
            print(f"boring checks: FAILED (profile={args.profile} scope={args.scope})")
            return code

    print(
        "boring checks: OK "
        f"(profile={args.profile} scope={args.scope} python_files={len(python_targets)})"
    )
    return 0


def main() -> int:
    """Run the configured boring-check profile from CLI arguments.

    Returns:
        Process exit code for the selected profile.
    """
    parser = argparse.ArgumentParser(description="Run boring engineering checks.")
    parser.add_argument("--profile", choices=("quick", "code", "type", "full"), default="quick")
    parser.add_argument("--scope", choices=("changed", "all"), default="changed")
    parser.add_argument("--fix", action="store_true")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", "--base-sha", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", "--head-sha", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    changed_files = _changed_files_for_args(args)
    args.changed_files = changed_files
    python_targets = _python_targets(repo_root, scope=args.scope, changed_files=changed_files)
    return _run_profile(repo_root, args, python_targets)


if __name__ == "__main__":
    raise SystemExit(main())
