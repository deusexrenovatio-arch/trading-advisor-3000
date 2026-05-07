#!/usr/bin/env python3
"""Install local Git hooks for the AI delivery shell."""

from __future__ import annotations

import argparse
import stat
import subprocess
from pathlib import Path


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Install .githooks as the repository hooks path.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root path. Defaults to the parent of scripts/.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print actions without changing Git config."
    )
    parser.add_argument(
        "--allow-no-git",
        action="store_true",
        help="Exit successfully when .git is not present (useful for bootstrap scaffolding).",
    )
    return parser.parse_args()


def ensure_hook_executable(hook_file: Path, dry_run: bool) -> None:
    if not hook_file.exists():
        raise FileNotFoundError(f"Hook source not found: {hook_file}")
    if dry_run:
        print(f"[dry-run] chmod +x {hook_file}")
        return
    current_mode = hook_file.stat().st_mode
    hook_file.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def configure_hooks_path(repo_root: Path, dry_run: bool) -> None:
    cmd = ["git", "config", "core.hooksPath", ".githooks"]
    if dry_run:
        print(f"[dry-run] {' '.join(cmd)} (cwd={repo_root})")
        return
    result = run(cmd, repo_root)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Failed to configure git hooksPath.")


def verify_hooks_path(repo_root: Path) -> str:
    result = run(["git", "config", "--get", "core.hooksPath"], repo_root)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Failed to read core.hooksPath.")
    return result.stdout.strip()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    git_dir = repo_root / ".git"
    hook_file = repo_root / ".githooks" / "pre-push"

    try:
        ensure_hook_executable(hook_file, dry_run=args.dry_run)
    except FileNotFoundError as exc:
        print(f"[error] {exc}")
        return 1

    if not git_dir.exists():
        message = f"[warn] No .git directory at {git_dir}."
        if args.allow_no_git:
            print(message)
            print("[ok] Hook install skipped because --allow-no-git is set.")
            return 0
        print(message)
        print("[hint] Initialize Git first or rerun with --allow-no-git for scaffolding mode.")
        return 1

    if args.dry_run:
        print(f"[dry-run] Repository root: {repo_root}")
        configure_hooks_path(repo_root, dry_run=True)
        print("[ok] Dry run complete.")
        return 0

    try:
        configure_hooks_path(repo_root, dry_run=False)
        configured = verify_hooks_path(repo_root)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        return 1

    print(f"[ok] Installed hooks for repository: {repo_root}")
    print(f"[ok] core.hooksPath={configured}")
    print("[next] Run a test push to confirm pre-push enforcement.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
