#!/usr/bin/env python3
"""Start lifecycle and governed Codex route in one step."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from codex_governed_entry import (
    GovernedEntryError,
    STACKED_FOLLOWUP_ROUTE,
    main as governed_entry_main,
    normalize_entry_route_mode,
    normalize_snapshot_mode,
)
from task_session import (
    LEGACY_SESSION_MODE,
    default_session_lock_path,
    evaluate_session_lock,
    load_session_lock,
    normalize_session_mode,
)
from repo_mutation_lock import (
    DEFAULT_MUTATION_LOCK_TIMEOUT_ENV,
    DEFAULT_MUTATION_LOCK_TIMEOUT_SEC,
    RepoMutationLockError,
    repo_mutation_lock,
)


DEFAULT_BOOTSTRAP_STATE = Path(".runlogs/codex-governed-entry/bootstrap-state.json")


class GovernedBootstrapError(RuntimeError):
    """Raised when governed bootstrap cannot proceed safely."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def ensure_active_session(*, repo_root: Path, request: str, session_mode: str) -> dict[str, Any]:
    requested_session_mode = normalize_session_mode(session_mode)
    session_lock_path = default_session_lock_path(repo_root)
    payload = load_session_lock(session_lock_path)
    ok, message = evaluate_session_lock(payload, repo_root=repo_root)
    if ok:
        active_raw_mode = str(payload.get("session_mode", "")).strip() or LEGACY_SESSION_MODE
        active_session_mode = normalize_session_mode(active_raw_mode)
        if active_session_mode != requested_session_mode:
            raise GovernedBootstrapError(
                "active task session mode mismatch: "
                f"requested `{requested_session_mode}` but active session is `{active_session_mode}`; "
                "end the current session first or rerun bootstrap with the active session mode"
            )
        return {
            "action": "reused",
            "message": message,
            "payload": payload,
            "session_mode": active_session_mode,
        }

    command = [
        sys.executable,
        str((repo_root / "scripts" / "task_session.py").resolve()),
        "begin",
        "--request",
        request,
        "--mode",
        requested_session_mode,
    ]
    completed = subprocess.run(command, cwd=repo_root, text=True, check=False, capture_output=True)
    if completed.returncode != 0:
        raise GovernedBootstrapError(
            "failed to start task session:\n" + (completed.stdout + completed.stderr).strip()
        )
    return {
        "action": "started",
        "message": (completed.stdout or "").strip(),
        "payload": {},
        "session_mode": requested_session_mode,
    }


def write_bootstrap_state(
    path: Path,
    payload: dict[str, Any],
    *,
    repo_root: Path | None = None,
    mutation_lock_timeout_sec: float = DEFAULT_MUTATION_LOCK_TIMEOUT_SEC,
) -> None:
    if repo_root is None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return
    try:
        with repo_mutation_lock(
            repo_root=repo_root,
            owner="codex_governed_bootstrap:state-write",
            timeout_sec=mutation_lock_timeout_sec,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except RepoMutationLockError as exc:
        raise GovernedBootstrapError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start task session and governed Codex route in one step.")
    parser.add_argument("--request", required=True)
    parser.add_argument(
        "--route",
        choices=("auto", "package", "continue", STACKED_FOLLOWUP_ROUTE),
        default="auto",
    )
    parser.add_argument("--route-mode", default="legacy")
    parser.add_argument("--session-mode", default="legacy-full")
    parser.add_argument("--snapshot-mode", default="route-report")
    parser.add_argument("--package-path", default=None)
    parser.add_argument("--execution-contract", default=None)
    parser.add_argument("--parent-brief", default=None)
    parser.add_argument("--module-slug", default=None)
    parser.add_argument("--module-priority", choices=("phase-order", "slug-lexical"), default=None)
    parser.add_argument("--ambiguity-report-file", default=".runlogs/codex-governed-entry/module-ambiguity-report.json")
    parser.add_argument("--followup-contract-file", default=".runlogs/codex-governed-entry/stacked-followup-contract.json")
    parser.add_argument("--predecessor-ref", default=None)
    parser.add_argument("--source-branch", default=None)
    parser.add_argument("--new-base-ref", default="origin/main")
    parser.add_argument("--carry-surface", action="append", default=[])
    parser.add_argument("--temporary-downgrade-surface", action="append", default=[])
    parser.add_argument("--inbox", default="docs/codex/packages/inbox")
    parser.add_argument("--artifact-root", default="artifacts/codex")
    parser.add_argument("--output-last-message", default="artifacts/codex/from-package-last-message.txt")
    parser.add_argument("--route-state-file", default=".runlogs/codex-governed-entry/last-route.json")
    parser.add_argument("--bootstrap-state-file", default=str(DEFAULT_BOOTSTRAP_STATE))
    parser.add_argument("--mode", default="auto")
    parser.add_argument("--profile", default="")
    parser.add_argument("--backend", choices=("simulate", "codex-cli"), default="codex-cli")
    parser.add_argument("--worker-model", default="")
    parser.add_argument("--acceptor-model", default="")
    parser.add_argument("--remediation-model", default="")
    parser.add_argument("--max-remediation-cycles", type=int, default=2)
    parser.add_argument("--codex-bin", default=None)
    parser.add_argument("--skip-clean-check", action="store_true")
    parser.add_argument(
        "--mutation-lock-timeout-sec",
        type=float,
        default=DEFAULT_MUTATION_LOCK_TIMEOUT_SEC,
        help=(
            "timeout in seconds while waiting for governed repo mutation lock "
            f"(env override: {DEFAULT_MUTATION_LOCK_TIMEOUT_ENV})."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv or sys.argv[1:])
    repo_root = resolve_repo_root()
    bootstrap_state_file = resolve_path(repo_root, args.bootstrap_state_file)

    try:
        normalized_route_mode = normalize_entry_route_mode(args.route_mode, positional_alias=False)
        normalized_snapshot_mode = normalize_snapshot_mode(args.snapshot_mode)
    except GovernedEntryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        requested_session_mode = normalize_session_mode(args.session_mode)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        session_state = ensure_active_session(
            repo_root=repo_root,
            request=args.request,
            session_mode=requested_session_mode,
        )
    except GovernedBootstrapError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    effective_session_mode = str(session_state.get("session_mode", requested_session_mode)).strip()
    try:
        effective_session_mode = normalize_session_mode(effective_session_mode)
    except ValueError as exc:
        print(f"error: bootstrap reported invalid session mode `{effective_session_mode}` ({exc})", file=sys.stderr)
        return 2

    entry_args = [
        "--route",
        args.route,
        "--route-mode",
        normalized_route_mode,
        "--session-mode",
        effective_session_mode,
        "--snapshot-mode",
        normalized_snapshot_mode,
        "--ambiguity-report-file",
        args.ambiguity_report_file,
        "--followup-contract-file",
        args.followup_contract_file,
        "--inbox",
        args.inbox,
        "--artifact-root",
        args.artifact_root,
        "--output-last-message",
        args.output_last_message,
        "--route-state-file",
        args.route_state_file,
        "--mode",
        args.mode,
        "--backend",
        args.backend,
        "--max-remediation-cycles",
        str(max(args.max_remediation_cycles, 0)),
        "--mutation-lock-timeout-sec",
        str(max(float(args.mutation_lock_timeout_sec), 0.0)),
    ]
    if args.worker_model.strip():
        entry_args.extend(["--worker-model", args.worker_model])
    if args.acceptor_model.strip():
        entry_args.extend(["--acceptor-model", args.acceptor_model])
    if args.remediation_model.strip():
        entry_args.extend(["--remediation-model", args.remediation_model])
    if args.profile.strip():
        entry_args.extend(["--profile", args.profile])
    if args.package_path:
        entry_args.extend(["--package-path", args.package_path])
    if args.execution_contract:
        entry_args.extend(["--execution-contract", args.execution_contract])
    if args.parent_brief:
        entry_args.extend(["--parent-brief", args.parent_brief])
    if args.module_slug:
        entry_args.extend(["--module-slug", args.module_slug])
    if args.module_priority:
        entry_args.extend(["--module-priority", args.module_priority])
    if args.predecessor_ref:
        entry_args.extend(["--predecessor-ref", args.predecessor_ref])
    if args.source_branch:
        entry_args.extend(["--source-branch", args.source_branch])
    if str(args.new_base_ref or "").strip():
        entry_args.extend(["--new-base-ref", args.new_base_ref])
    for item in args.carry_surface:
        entry_args.extend(["--carry-surface", item])
    for item in args.temporary_downgrade_surface:
        entry_args.extend(["--temporary-downgrade-surface", item])
    if args.codex_bin:
        entry_args.extend(["--codex-bin", args.codex_bin])
    if args.skip_clean_check:
        entry_args.append("--skip-clean-check")
    if args.dry_run:
        entry_args.append("--dry-run")

    write_bootstrap_state(
        bootstrap_state_file,
        {
            "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
            "request": args.request,
            "session_action": session_state["action"],
            "route_mode": normalized_route_mode,
            "session_mode": effective_session_mode,
            "snapshot_mode": normalized_snapshot_mode,
            "profile": args.profile.strip() or "none",
            "route": args.route,
            "module_slug": args.module_slug or "",
            "module_priority": args.module_priority or "",
            "predecessor_ref": args.predecessor_ref or "",
            "source_branch": args.source_branch or "",
            "new_base_ref": args.new_base_ref or "",
            "carry_surfaces": list(args.carry_surface),
            "temporary_downgrade_surfaces": list(args.temporary_downgrade_surface),
            "route_args": entry_args,
        },
        repo_root=repo_root,
        mutation_lock_timeout_sec=max(float(args.mutation_lock_timeout_sec), 0.0),
    )
    print(f"session_action: {session_state['action']}")
    print(f"session_mode: {effective_session_mode}")
    return int(governed_entry_main(entry_args))


if __name__ == "__main__":
    raise SystemExit(main())
