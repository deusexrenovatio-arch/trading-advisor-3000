from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from handoff_resolver import read_task_note_lines


DEFAULT_SESSION_LOCK = Path(".runlogs/task-session/session-lock.json")
DEFAULT_SESSION_TTL_HOURS = 12
TASK_INDEX_ACTIVE = Path("docs/tasks/active/index.yaml")
TASK_INDEX_ARCHIVE = Path("docs/tasks/archive/index.yaml")
TERMINAL_OUTCOME_STATUSES = {"completed", "partial", "blocked"}
LEGACY_SESSION_MODE = "legacy-full"
TRACKED_SESSION_MODE = "tracked_session"
BRANCH_SHARED_SESSION_BINDING = "branch-shared"
WORKTREE_STRICT_SESSION_BINDING = "worktree-strict"
DEFAULT_SESSION_BINDING = BRANCH_SHARED_SESSION_BINDING
DEFAULT_SESSION_BINDING_ENV = "TA3000_SESSION_BINDING"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today_iso_utc() -> str:
    return _now().date().isoformat()


def _updated_stamp_utc() -> str:
    return _now().strftime("%Y-%m-%d %H:%M UTC")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True, check=False)


def _slugify(text: str, *, fallback: str = "task") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:48] if slug else fallback


def normalize_session_mode(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if value in {"", "full", "legacy", "legacy-full"}:
        return LEGACY_SESSION_MODE
    if value in {"tracked", "tracked_session", "tracked-session"}:
        return TRACKED_SESSION_MODE
    raise ValueError(
        "unknown session mode; expected one of legacy-full|tracked_session|tracked-session"
    )


def normalize_session_binding(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if value in {"", "branch", "branch-shared", "shared", "relaxed"}:
        return BRANCH_SHARED_SESSION_BINDING
    if value in {"worktree", "strict", "worktree-strict"}:
        return WORKTREE_STRICT_SESSION_BINDING
    raise ValueError("unknown session binding; expected one of branch-shared|worktree-strict")


def resolve_default_session_binding() -> str:
    raw = str(os.environ.get(DEFAULT_SESSION_BINDING_ENV, DEFAULT_SESSION_BINDING)).strip()
    try:
        return normalize_session_binding(raw)
    except ValueError:
        return DEFAULT_SESSION_BINDING


def get_repo_root() -> Path:
    completed = _run(["git", "rev-parse", "--show-toplevel"])
    if completed.returncode != 0 or not completed.stdout.strip():
        raise RuntimeError("not inside a git repository")
    return Path(completed.stdout.strip()).resolve()


def get_current_branch(repo_root: Path | None = None) -> str:
    completed = _run(["git", "branch", "--show-current"], cwd=repo_root)
    if completed.returncode != 0:
        return "<unknown>"
    value = completed.stdout.strip()
    return value or "<detached>"


def default_session_lock_path(repo_root: Path | None = None) -> Path:
    root = repo_root or get_repo_root()
    return (root / DEFAULT_SESSION_LOCK).resolve()


def load_session_lock(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_session_lock(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def clear_session_lock(path: Path) -> None:
    if path.exists():
        path.unlink()


def build_session_id() -> str:
    return f"TS-{_now().strftime('%Y%m%dT%H%M%SZ')}-{os.urandom(4).hex()}"


def create_session_lock(
    *,
    repo_root: Path,
    ttl_hours: int,
    session_mode: str,
    session_binding: str,
) -> dict[str, Any]:
    started_at = _now()
    return {
        "session_id": build_session_id(),
        "worktree": str(repo_root),
        "branch": get_current_branch(repo_root),
        "session_mode": session_mode,
        "session_binding": session_binding,
        "started_at": _iso(started_at),
        "expires_at": _iso(started_at + timedelta(hours=max(ttl_hours, 1))),
    }


def evaluate_session_lock(payload: dict[str, Any], *, repo_root: Path) -> tuple[bool, str]:
    if not payload:
        return False, "session lock is not set"
    worktree = str(payload.get("worktree", "")).strip()
    branch = str(payload.get("branch", "")).strip()
    session_mode_raw = str(payload.get("session_mode", "")).strip()
    session_binding_raw = str(payload.get("session_binding", "")).strip()
    started_at = _parse_iso(str(payload.get("started_at", "")).strip())
    expires_at = _parse_iso(str(payload.get("expires_at", "")).strip())
    session_id = str(payload.get("session_id", "")).strip()
    if not all((worktree, branch, started_at, expires_at, session_id)):
        return False, "session lock is invalid"
    if session_mode_raw:
        try:
            normalize_session_mode(session_mode_raw)
        except ValueError:
            return False, f"session lock has unknown session_mode `{session_mode_raw}`"
    if session_binding_raw:
        try:
            session_binding = normalize_session_binding(session_binding_raw)
        except ValueError:
            return False, f"session lock has unknown session_binding `{session_binding_raw}`"
    else:
        session_binding = resolve_default_session_binding()
    current_branch = get_current_branch(repo_root)
    current_worktree = str(repo_root)
    if os.name == "nt":
        same_worktree = current_worktree.casefold() == worktree.casefold()
    else:
        same_worktree = current_worktree == worktree
    branch_matches = current_branch == branch
    if not branch_matches:
        return False, (
            "session mismatch "
            f"(binding={session_binding} expected worktree={worktree} branch={branch}, "
            f"current worktree={current_worktree} branch={current_branch})"
        )
    if session_binding == WORKTREE_STRICT_SESSION_BINDING and not same_worktree:
        return False, (
            "session mismatch "
            f"(binding={session_binding} expected worktree={worktree} branch={branch}, "
            f"current worktree={current_worktree} branch={current_branch})"
        )
    if _now() > expires_at:
        return False, f"session expired at {payload['expires_at']}"
    return True, "ok"


def check_active_session(*, lock_path: Path | None = None) -> tuple[int, str, dict[str, Any]]:
    repo_root = get_repo_root()
    session_lock_path = lock_path or default_session_lock_path(repo_root)
    payload = load_session_lock(session_lock_path)
    ok, message = evaluate_session_lock(payload, repo_root=repo_root)
    if not ok:
        return 1, message, payload
    return 0, "ok", payload


def _repo_relative(path: Path, *, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except Exception:
        return path.as_posix()


def _load_index(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": _today_iso_utc(), "items": []}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        payload = {"version": 1}
    items = payload.get("items")
    if not isinstance(items, list):
        payload["items"] = []
    payload.setdefault("version", 1)
    payload.setdefault("updated_at", _today_iso_utc())
    return payload


def _write_index(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _task_note_template(*, request: str) -> str:
    timestamp = _updated_stamp_utc()
    return (
        "# Task Note\n"
        f"Updated: {timestamp}\n\n"
        "## Goal\n"
        f"- Deliver: {request.strip()}\n\n"
        "## Task Request Contract\n"
        "- Objective: define one measurable process/governance outcome.\n"
        "- In Scope: list explicit files/surfaces that can change now.\n"
        "- Out of Scope: list deferred items and non-goals.\n"
        "- Constraints: list risk/time/policy/runtime constraints.\n"
        "- Done Evidence: list exact commands/artifacts proving completion.\n"
        "- Priority Rule: quality and safety over speed when tradeoffs appear.\n\n"
        "## Current Delta\n"
        "- Session started and baseline scope captured.\n\n"
        "## First-Time-Right Report\n"
        "1. Confirmed coverage: objective and acceptance path are explicit.\n"
        "2. Missing or risky scenarios: unknown integrations and policy drifts.\n"
        "3. Resource/time risks and chosen controls: phased patches and deterministic checks.\n"
        "4. Highest-priority fixes or follow-ups: stabilize contract and validation first.\n\n"
        "## Repetition Control\n"
        "- Max Same-Path Attempts: 2\n"
        "- Stop Trigger: same failure repeats after two focused edits.\n"
        "- Reset Action: pause edits, capture failing check, and reframe the approach.\n"
        "- New Search Space: validator contract, routing logic, and docs alignment.\n"
        "- Next Probe: run the smallest failing command before next patch.\n\n"
        "## Task Outcome\n"
        "- Outcome Status: in_progress\n"
        "- Decision Quality: pending\n"
        "- Final Contexts: pending\n"
        "- Route Match: pending\n"
        "- Primary Rework Cause: none\n"
        "- Incident Signature: none\n"
        "- Improvement Action: pending\n"
        "- Improvement Artifact: pending\n\n"
        "## Blockers\n"
        "- No blocker.\n\n"
        "## Next Step\n"
        "- Implement focused patch and rerun loop gate.\n\n"
        "## Validation\n"
        "- `python scripts/validate_task_request_contract.py`\n"
        "- `python scripts/validate_session_handoff.py`\n"
        "- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`\n"
    )


def _write_session_handoff(*, path: Path, note_path: str, status: str, mode: str = LEGACY_SESSION_MODE) -> None:
    content = (
        "# Session Handoff\n"
        f"Updated: {_updated_stamp_utc()}\n\n"
        "## Active Task Note\n"
        f"- Path: {note_path}\n"
        f"- Mode: {mode}\n"
        f"- Status: {status}\n\n"
        "## Validation\n"
        "- `python scripts/validate_task_request_contract.py`\n"
        "- `python scripts/validate_session_handoff.py`\n"
        "- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _choose_task_note_path(*, repo_root: Path, request: str) -> Path:
    slug = _slugify(request)
    base = f"TASK-{_today_iso_utc()}-{slug}.md"
    note_path = repo_root / "docs" / "tasks" / "active" / base
    counter = 1
    while note_path.exists():
        note_path = repo_root / "docs" / "tasks" / "active" / f"TASK-{_today_iso_utc()}-{slug}-{counter}.md"
        counter += 1
    return note_path


def _task_id_from_path(path: Path) -> str:
    return path.stem.replace("_", "-").upper()


def _set_active_index_entry(*, repo_root: Path, note_rel: str, mode: str = LEGACY_SESSION_MODE) -> str:
    index_path = (repo_root / TASK_INDEX_ACTIVE).resolve()
    payload = _load_index(index_path)
    task_id = _task_id_from_path(Path(note_rel))
    rows = [row for row in payload.get("items", []) if isinstance(row, dict)]
    rows = [row for row in rows if str(row.get("path", "")).strip().lower() != note_rel.lower()]
    rows.append(
        {
            "id": task_id,
            "path": note_rel,
            "mode": mode,
            "status": "in_progress",
            "started_at": _today_iso_utc(),
        }
    )
    payload["items"] = rows
    payload["updated_at"] = _today_iso_utc()
    _write_index(index_path, payload)
    return task_id


def _close_task_indexes(
    *,
    repo_root: Path,
    source_relative: str,
    archive_relative: str,
    mode: str = LEGACY_SESSION_MODE,
) -> str:
    active_index_path = (repo_root / TASK_INDEX_ACTIVE).resolve()
    archive_index_path = (repo_root / TASK_INDEX_ARCHIVE).resolve()
    active_payload = _load_index(active_index_path)
    archive_payload = _load_index(archive_index_path)
    active_rows = [row for row in active_payload.get("items", []) if isinstance(row, dict)]
    archive_rows = [row for row in archive_payload.get("items", []) if isinstance(row, dict)]

    source_key = source_relative.lower()
    task_id = _task_id_from_path(Path(archive_relative))
    started_at = _today_iso_utc()
    remaining_active: list[dict[str, Any]] = []
    for row in active_rows:
        marker = str(row.get("path", "")).strip().replace("\\", "/").lower()
        if marker == source_key:
            task_id = str(row.get("id", "")).strip() or task_id
            started_at = str(row.get("started_at", "")).strip() or started_at
            row_mode = str(row.get("mode", "")).strip()
            if row_mode:
                try:
                    mode = normalize_session_mode(row_mode)
                except ValueError:
                    mode = row_mode
            continue
        remaining_active.append(dict(row))

    active_payload["items"] = remaining_active
    active_payload["updated_at"] = _today_iso_utc()
    _write_index(active_index_path, active_payload)

    archive_key = archive_relative.lower()
    without_existing = [
        row
        for row in archive_rows
        if str(row.get("path", "")).strip().replace("\\", "/").lower() != archive_key
    ]
    without_existing.append(
        {
            "id": task_id,
            "path": archive_relative,
            "mode": mode,
            "status": "completed",
            "started_at": started_at,
            "completed_at": _today_iso_utc(),
        }
    )
    archive_payload["items"] = without_existing
    archive_payload["updated_at"] = _today_iso_utc()
    _write_index(archive_index_path, archive_payload)
    return task_id


def _parse_task_outcome(lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    section_start = -1
    for idx, raw in enumerate(lines):
        if raw.strip() == "## Task Outcome":
            section_start = idx
            break
    if section_start < 0:
        return fields
    section_end = len(lines)
    for idx in range(section_start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            section_end = idx
            break
    for raw in lines[section_start + 1 : section_end]:
        stripped = raw.strip()
        if not stripped.startswith("- "):
            continue
        body = stripped[2:]
        if ":" not in body:
            continue
        key, value = body.split(":", 1)
        normalized_key = re.sub(r"[^a-z0-9]+", "_", key.strip().lower()).strip("_")
        fields[normalized_key] = value.strip()
    return fields


def _route_begin_context(*, request: str, handoff_path: Path) -> dict[str, Any]:
    try:
        from context_router import route_files
    except Exception:
        return {"primary_context": None, "recommendations": []}
    session_handoff_text = ""
    if handoff_path.exists():
        _target, lines, _pointer = read_task_note_lines(handoff_path)
        session_handoff_text = "\n".join(lines)
    return route_files([], request_text=request, session_handoff_text=session_handoff_text)


def begin_session(
    *,
    request: str,
    ttl_hours: int,
    session_handoff_path: Path,
    mode: str,
    binding: str = DEFAULT_SESSION_BINDING,
    lock_path: Path | None = None,
) -> int:
    if not request.strip():
        print("task session: begin requires non-empty --request")
        return 2

    try:
        session_mode = normalize_session_mode(mode)
    except ValueError as exc:
        print(f"task session: begin requires a valid --mode ({exc})")
        return 2
    try:
        session_binding = normalize_session_binding(binding)
    except ValueError as exc:
        print(f"task session: begin requires a valid --binding ({exc})")
        return 2

    repo_root = get_repo_root()
    session_lock_path = lock_path or default_session_lock_path(repo_root)
    existing = load_session_lock(session_lock_path)
    ok, message = evaluate_session_lock(existing, repo_root=repo_root)
    if ok:
        print(
            "task session: already active "
            f"(session_id={existing['session_id']} branch={existing['branch']})"
        )
        print("task session: end the current session before starting a new one")
        return 1

    note_path = _choose_task_note_path(repo_root=repo_root, request=request)
    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text(_task_note_template(request=request), encoding="utf-8")
    note_rel = _repo_relative(note_path, repo_root=repo_root)
    task_id = _set_active_index_entry(repo_root=repo_root, note_rel=note_rel, mode=session_mode)
    _write_session_handoff(
        path=session_handoff_path,
        note_path=note_rel,
        status="in_progress",
        mode=session_mode,
    )

    payload = create_session_lock(
        repo_root=repo_root,
        ttl_hours=ttl_hours,
        session_mode=session_mode,
        session_binding=session_binding,
    )
    write_session_lock(session_lock_path, payload)

    route = _route_begin_context(request=request.strip(), handoff_path=session_handoff_path)
    print("task session: started")
    print(f"  session_id: {payload['session_id']}")
    print(f"  task_id: {task_id}")
    print(f"  task_note: {note_rel}")
    print(f"  session_mode: {session_mode}")
    print(f"  session_binding: {session_binding}")
    print(f"  primary_context: {route.get('primary_context') or 'unknown'}")
    print("  next_gate: python scripts/run_loop_gate.py --from-git --git-ref HEAD")
    for note in list(route.get("recommendations", []))[:3]:
        print(f"  note: {note}")
    return 0


def session_status(*, lock_path: Path | None = None, quiet: bool = False) -> int:
    repo_root = get_repo_root()
    session_lock_path = lock_path or default_session_lock_path(repo_root)
    payload = load_session_lock(session_lock_path)
    ok, message = evaluate_session_lock(payload, repo_root=repo_root)
    if not ok:
        if not quiet:
            print(f"task session: inactive ({message})")
        return 1
    if quiet:
        return 0
    print("task session: active")
    print(f"  session_id: {payload['session_id']}")
    print(f"  worktree: {payload['worktree']}")
    print(f"  branch: {payload['branch']}")
    session_mode_raw = str(payload.get("session_mode", LEGACY_SESSION_MODE)).strip() or LEGACY_SESSION_MODE
    try:
        session_mode = normalize_session_mode(session_mode_raw)
    except ValueError:
        session_mode = session_mode_raw
    session_binding_raw = str(payload.get("session_binding", DEFAULT_SESSION_BINDING)).strip()
    try:
        session_binding = normalize_session_binding(session_binding_raw)
    except ValueError:
        session_binding = session_binding_raw
    print(f"  session_mode: {session_mode}")
    print(f"  session_binding: {session_binding}")
    print(f"  session_lock_path: {session_lock_path}")
    print(f"  started_at: {payload['started_at']}")
    print(f"  expires_at: {payload['expires_at']}")
    return 0


def end_session(
    *,
    session_handoff_path: Path,
    lock_path: Path | None = None,
) -> int:
    repo_root = get_repo_root()
    session_lock_path = lock_path or default_session_lock_path(repo_root)
    payload = load_session_lock(session_lock_path)
    ok, message = evaluate_session_lock(payload, repo_root=repo_root)
    if not ok:
        print(f"task session: cannot end inactive session ({message})")
        return 1
    session_mode_raw = str(payload.get("session_mode", LEGACY_SESSION_MODE)).strip() or LEGACY_SESSION_MODE
    try:
        session_mode = normalize_session_mode(session_mode_raw)
    except ValueError:
        session_mode = session_mode_raw
    if not session_handoff_path.exists():
        print(f"task session: missing {session_handoff_path.as_posix()}")
        return 1

    note_path, note_lines, is_pointer = read_task_note_lines(session_handoff_path)
    if not is_pointer:
        print("task session: session handoff is not in pointer mode; cannot close lifecycle safely")
        return 1
    note_rel = _repo_relative(note_path, repo_root=repo_root)
    if not note_rel.lower().startswith("docs/tasks/active/"):
        print("task session: active note is not under docs/tasks/active/")
        return 1

    outcome_fields = _parse_task_outcome(note_lines)
    outcome_status = outcome_fields.get("outcome_status", "").strip().lower()
    if outcome_status not in TERMINAL_OUTCOME_STATUSES:
        print(
            "task session: end requires terminal Task Outcome status "
            "(completed|partial|blocked)"
        )
        return 1

    archive_path = (repo_root / "docs" / "tasks" / "archive" / note_path.name).resolve()
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path.write_text(note_path.read_text(encoding="utf-8"), encoding="utf-8")
    note_path.unlink(missing_ok=True)
    archive_rel = _repo_relative(archive_path, repo_root=repo_root)
    task_id = _close_task_indexes(
        repo_root=repo_root,
        source_relative=note_rel,
        archive_relative=archive_rel,
    )
    _write_session_handoff(
        path=session_handoff_path,
        note_path=archive_rel,
        status="completed",
        mode=session_mode,
    )

    try:
        import sync_task_outcomes

        sync_task_outcomes.run(
            session_handoff_path=session_handoff_path,
            task_outcomes_path=repo_root / "memory" / "task_outcomes.yaml",
            task_id_override=task_id,
        )
    except Exception:
        # Keep lifecycle robust even when outcome sync is not yet wired.
        pass

    clear_session_lock(session_lock_path)
    print("task session: ended")
    print(f"  session_id: {payload['session_id']}")
    print(f"  task_id: {task_id}")
    print(f"  outcome_status: {outcome_status}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Canonical task session lifecycle entrypoint.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    begin = subparsers.add_parser("begin")
    begin.add_argument("--request", required=True)
    begin.add_argument("--ttl-hours", type=int, default=DEFAULT_SESSION_TTL_HOURS)
    begin.add_argument("--mode", default=LEGACY_SESSION_MODE)
    begin.add_argument("--binding", default=DEFAULT_SESSION_BINDING)
    begin.add_argument("--session-handoff-path", default="docs/session_handoff.md")
    begin.add_argument("--lock-path", default=None)

    status = subparsers.add_parser("status")
    status.add_argument("--lock-path", default=None)
    status.add_argument("--quiet", action="store_true")

    end = subparsers.add_parser("end")
    end.add_argument("--session-handoff-path", default="docs/session_handoff.md")
    end.add_argument("--lock-path", default=None)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "begin":
        return begin_session(
            request=args.request,
            ttl_hours=args.ttl_hours,
            mode=args.mode,
            binding=args.binding,
            session_handoff_path=Path(args.session_handoff_path),
            lock_path=Path(args.lock_path) if args.lock_path else None,
        )
    if args.command == "status":
        return session_status(
            lock_path=Path(args.lock_path) if args.lock_path else None,
            quiet=bool(args.quiet),
        )
    if args.command == "end":
        return end_session(
            session_handoff_path=Path(args.session_handoff_path),
            lock_path=Path(args.lock_path) if args.lock_path else None,
        )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
