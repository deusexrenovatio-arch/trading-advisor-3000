from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

from compute_change_surface import compute_surface
from gate_common import CommandSpec, collect_changed_files, run_command, run_commands, write_summary
from run_pr_gate import _run_pr_checks

REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"


def _join_command(parts: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(parts)
    return shlex.join(parts)


def _build_pr_gate_command(
    *,
    mapping: str,
    from_git: bool,
    git_ref: str | None,
    base_ref: str | None,
    head_ref: str | None,
    explicit_changed_files: list[str] | None,
    snapshot_mode: str,
    profile: str,
    architecture_review_source: str | None = None,
) -> str | CommandSpec:
    parts = [
        sys.executable,
        "scripts/run_pr_gate.py",
        "--mapping",
        mapping,
        "--snapshot-mode",
        snapshot_mode,
        "--profile",
        profile,
        "--enforce-explicit-markers",
    ]
    if architecture_review_source:
        parts.extend(["--architecture-review-source", architecture_review_source])
    parts.extend(["--git-ref", git_ref or "HEAD"])
    if base_ref and head_ref:
        parts.extend(["--base-ref", base_ref, "--head-ref", head_ref])
    if explicit_changed_files is not None:
        parts.append("--stdin")
        stdin_text = "\n".join(explicit_changed_files)
        if stdin_text:
            stdin_text += "\n"
        return CommandSpec(command=_join_command(parts), stdin_text=stdin_text)
    if not (base_ref and head_ref):
        parts.append("--from-git")
    return _join_command(parts)


def _hosted_post_merge_hygiene(args: argparse.Namespace) -> bool:
    if (
        os.environ.get("GITHUB_ACTIONS") != "true"
        or os.environ.get("GITHUB_EVENT_NAME") not in {"schedule", "workflow_dispatch"}
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or not args.base_ref
        or not args.head_ref
        or args.stdin
        or args.changed_files
        or args.from_git
    ):
        return False

    def git(*parts: str) -> str:
        return subprocess.run(
            ["git", *parts], capture_output=True, text=True, check=True
        ).stdout.strip()

    try:
        git("rev-parse", "--verify", f"{args.base_ref}^{{commit}}")
        head = git("rev-parse", "--verify", f"{args.head_ref}^{{commit}}")
        return head == git("rev-parse", "HEAD") and not git("status", "--porcelain")
    except subprocess.CalledProcessError:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run nightly scoped governance gate.")
    parser.add_argument("--mapping", default="configs/change_surface_mapping.yaml")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--snapshot-mode", default="changed-files")
    parser.add_argument("--profile", default="none")
    parser.add_argument("--summary-file", default=None)
    parser.add_argument("--architecture-review-source", default=None)
    args = parser.parse_args()

    try:
        changed_files = collect_changed_files(
            base_ref=args.base_ref,
            head_ref=args.head_ref,
            git_ref=args.git_ref,
            from_git=args.from_git
            or not (args.base_ref or args.head_ref or args.stdin or args.changed_files),
            changed_files=list(args.changed_files),
            from_stdin=args.stdin,
        )
    except ValueError as exc:
        print(f"nightly gate: FAILED ({exc})")
        return 2
    surface = compute_surface(changed_files, mapping_path=Path(args.mapping))

    pr_command = _build_pr_gate_command(
        mapping=args.mapping,
        from_git=args.from_git,
        git_ref=args.git_ref,
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        explicit_changed_files=list(changed_files) if (args.stdin or args.changed_files) else None,
        snapshot_mode=str(args.snapshot_mode or "").strip() or "changed-files",
        profile=str(args.profile or "").strip() or "none",
        architecture_review_source=args.architecture_review_source,
    )
    if _hosted_post_merge_hygiene(args):
        pr_command = "post-merge hygiene; architecture admission not evaluated"
        print(pr_command)
        pr_code = _run_pr_checks(args, surface, changed_files, args.snapshot_mode, args.profile)
    else:
        pr_code = run_command(pr_command)
    if pr_code != 0:
        print(f"nightly gate: FAILED (command={pr_command})\nremediation: see {REMEDIATION_DOC}")
        return pr_code

    commands = [
        command
        for command in surface["commands"]["nightly"]
        if "scripts/run_pr_gate.py" not in command
    ]
    if surface["docs_only"]:
        # Docs-only changes should not fail nightly on global KPI drift unrelated
        # to the current diff.
        commands = [
            command
            for command in commands
            if "scripts/validate_process_regressions.py" not in command
        ]
    code, failed_command = run_commands(commands)
    write_summary(
        summary_file=args.summary_file,
        gate_name="nightly gate",
        surface_result=surface,
        commands=[pr_command, *commands],
    )
    if code != 0:
        print(
            f"nightly gate: FAILED (command={failed_command})\nremediation: see {REMEDIATION_DOC}"
        )
        return code

    print(
        "nightly gate: OK "
        f"(primary_surface={surface['primary_surface']} surfaces={','.join(surface['surfaces'])})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
