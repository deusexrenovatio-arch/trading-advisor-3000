from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

from compute_change_surface import compute_surface
from gate_marker_contract import GateMarkerError, resolve_gate_markers
from gate_common import (
    CommandSpec,
    collect_changed_files,
    is_non_trivial_diff,
    run_command,
    run_commands,
    scope_validate_command,
    write_summary,
)
from task_session import check_active_session


REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"


def _join_command(parts: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(parts)
    return shlex.join(parts)


def _ensure_active_session() -> int:
    code, message, _payload = check_active_session()
    if code == 0:
        return 0
    print(f"pr gate: FAILED (inactive task session: {message})")
    print("pr gate: run `python scripts/task_session.py begin --request \"<request>\"` first")
    print(f"remediation: see {REMEDIATION_DOC}")
    return code


def _build_loop_gate_command(
    *,
    mapping: str,
    from_git: bool,
    git_ref: str | None,
    base_ref: str | None,
    head_ref: str | None,
    explicit_changed_files: list[str] | None,
    snapshot_mode: str,
    profile: str,
) -> str | CommandSpec:
    parts = [
        sys.executable,
        "scripts/run_loop_gate.py",
        "--mapping",
        mapping,
        "--skip-session-check",
        "--snapshot-mode",
        snapshot_mode,
        "--profile",
        profile,
        "--enforce-explicit-markers",
    ]
    if explicit_changed_files is not None:
        parts.append("--stdin")
        stdin_text = "\n".join(explicit_changed_files)
        if stdin_text:
            stdin_text += "\n"
        return CommandSpec(command=_join_command(parts), stdin_text=stdin_text)
    if base_ref and head_ref:
        parts.extend(["--base-ref", base_ref, "--head-ref", head_ref])
    elif from_git:
        parts.extend(["--from-git", "--git-ref", git_ref or "HEAD"])
    else:
        parts.append("--from-git")
    return _join_command(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run PR-closeout scoped governance gate.")
    parser.add_argument("--mapping", default="configs/change_surface_mapping.yaml")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--snapshot-mode", default=None)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--enforce-explicit-markers", action="store_true")
    parser.add_argument("--summary-file", default=None)
    parser.add_argument("--skip-session-check", action="store_true")
    args = parser.parse_args()

    if not args.skip_session_check:
        code = _ensure_active_session()
        if code != 0:
            return code

    changed_files = collect_changed_files(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=args.from_git,
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )
    surface = compute_surface(changed_files, mapping_path=Path(args.mapping))
    policy_critical = is_non_trivial_diff(changed_files) and not bool(surface.get("docs_only"))
    try:
        markers = resolve_gate_markers(
            gate_name="pr gate",
            from_git=bool(args.from_git),
            base_ref=args.base_ref,
            head_ref=args.head_ref,
            from_stdin=bool(args.stdin),
            explicit_changed_files=list(args.changed_files),
            snapshot_mode_raw=args.snapshot_mode,
            profile_raw=args.profile,
            policy_critical=policy_critical,
            enforce_explicit_markers=bool(args.enforce_explicit_markers),
            # PR gate always resolves against diff semantics even when selectors are omitted.
            default_when_no_selector="changed-files",
        )
    except GateMarkerError as exc:
        print(f"pr gate: FAILED ({exc})")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 2
    snapshot_mode = markers.snapshot_mode
    profile = markers.profile
    for message in markers.deprecation_messages:
        print(message)

    loop_command = _build_loop_gate_command(
        mapping=args.mapping,
        from_git=args.from_git,
        git_ref=args.git_ref,
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        explicit_changed_files=list(changed_files) if (args.stdin or args.changed_files) else None,
        snapshot_mode=snapshot_mode,
        profile=profile,
    )
    loop_code = run_command(loop_command)
    if loop_code != 0:
        print(
            "pr gate: FAILED "
            f"(command={loop_command} snapshot_mode={snapshot_mode} profile={profile})\n"
            f"remediation: see {REMEDIATION_DOC}"
        )
        return loop_code

    commands = [
        scope_validate_command(
            command,
            base_sha=args.base_ref,
            head_sha=args.head_ref,
            changed_files=changed_files,
        )
        for command in surface["commands"]["pr"]
        if "scripts/run_loop_gate.py" not in command
    ]
    code, failed_command = run_commands(commands)
    write_summary(
        summary_file=args.summary_file,
        gate_name="pr gate",
        surface_result=surface,
        commands=[loop_command, *commands],
        snapshot_mode=snapshot_mode,
        profile=profile,
    )
    if code != 0:
        print(
            "pr gate: FAILED "
            f"(command={failed_command} snapshot_mode={snapshot_mode} profile={profile})\n"
            f"remediation: see {REMEDIATION_DOC}"
        )
        return code

    print(
        "pr gate: OK "
        f"(primary_surface={surface['primary_surface']} surfaces={','.join(surface['surfaces'])} "
        f"policy_critical={policy_critical} "
        f"snapshot_mode={snapshot_mode} profile={profile})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
