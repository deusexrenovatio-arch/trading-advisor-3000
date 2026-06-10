from __future__ import annotations

import argparse
from pathlib import Path

from compute_change_surface import compute_surface
from gate_common import (
    collect_changed_files,
    run_commands,
    scope_validate_command,
    write_summary,
)
from gate_marker_contract import (
    GateMarkerError,
    includes_policy_critical_command,
    resolve_gate_markers,
)

REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"
POLICY_CRITICAL_LOOP_VALIDATION_SCRIPTS = (
    "scripts/validate_solution_intent.py",
    "scripts/validate_critical_contour_closure.py",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run scoped hot-loop governance gate.")
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
    args = parser.parse_args()

    changed_files = collect_changed_files(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=args.from_git,
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )
    surface = compute_surface(changed_files, mapping_path=Path(args.mapping))
    loop_commands = list(surface["commands"]["loop"])
    policy_critical = includes_policy_critical_command(
        loop_commands,
        critical_script_paths=POLICY_CRITICAL_LOOP_VALIDATION_SCRIPTS,
    )
    try:
        markers = resolve_gate_markers(
            gate_name="loop gate",
            from_git=bool(args.from_git),
            base_ref=args.base_ref,
            head_ref=args.head_ref,
            from_stdin=bool(args.stdin),
            explicit_changed_files=list(args.changed_files),
            snapshot_mode_raw=args.snapshot_mode,
            profile_raw=args.profile,
            policy_critical=policy_critical,
            enforce_explicit_markers=bool(args.enforce_explicit_markers),
            default_when_no_selector="contract-only",
        )
    except GateMarkerError as exc:
        print(f"loop gate: FAILED ({exc})")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 2
    snapshot_mode = markers.snapshot_mode
    profile = markers.profile
    for message in markers.deprecation_messages:
        print(message)

    commands = [
        scope_validate_command(
            command,
            base_sha=args.base_ref,
            head_sha=args.head_ref,
            changed_files=changed_files,
        )
        for command in loop_commands
    ]
    code, failed_command = run_commands(commands)
    write_summary(
        summary_file=args.summary_file,
        gate_name="loop gate",
        surface_result=surface,
        commands=commands,
        snapshot_mode=snapshot_mode,
        profile=profile,
    )
    if code != 0:
        print(
            "loop gate: FAILED "
            f"(command={failed_command} snapshot_mode={snapshot_mode} profile={profile})\n"
            f"remediation: see {REMEDIATION_DOC}"
        )
        return code

    print(
        "loop gate: OK "
        f"(primary_surface={surface['primary_surface']} "
        f"surfaces={','.join(surface['surfaces'])} "
        f"policy_critical={policy_critical} "
        f"snapshot_mode={snapshot_mode} profile={profile})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
