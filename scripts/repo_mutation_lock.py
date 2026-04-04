from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


DEFAULT_REPO_LOCK_RELATIVE = Path(".runlogs/codex-governed-entry/repo-mutation.lock")
DEFAULT_EVENT_LOG_RELATIVE = Path(".runlogs/codex-governed-entry/repo-mutation-events.jsonl")
DEFAULT_MUTATION_LOCK_TIMEOUT_ENV = "TA3000_MUTATION_LOCK_TIMEOUT_SEC"
_FALLBACK_MUTATION_LOCK_TIMEOUT_SEC = 30.0


def _resolve_default_timeout_sec() -> float:
    raw = str(os.getenv(DEFAULT_MUTATION_LOCK_TIMEOUT_ENV, "")).strip()
    if not raw:
        return _FALLBACK_MUTATION_LOCK_TIMEOUT_SEC
    try:
        return max(float(raw), 0.0)
    except ValueError:
        return _FALLBACK_MUTATION_LOCK_TIMEOUT_SEC


DEFAULT_MUTATION_LOCK_TIMEOUT_SEC = _resolve_default_timeout_sec()


class RepoMutationLockError(RuntimeError):
    """Raised when governed repo mutation lock cannot be acquired safely."""


@dataclass(frozen=True)
class RepoMutationLease:
    repo_root: Path
    lock_path: Path
    event_log_path: Path
    owner: str
    token: str
    acquired_at: str
    waited_sec: float


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_path(repo_root: Path, raw: Path) -> Path:
    if raw.is_absolute():
        return raw
    return (repo_root / raw).resolve()


def _index_lock_path(repo_root: Path) -> Path:
    return (repo_root / ".git" / "index.lock").resolve()


def _read_lock_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _append_event(event_log_path: Path, payload: dict[str, Any]) -> None:
    event_log_path.parent.mkdir(parents=True, exist_ok=True)
    with event_log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _create_lock_file(lock_path: Path, payload: dict[str, Any]) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    except Exception:
        try:
            lock_path.unlink()
        except OSError:
            pass
        raise
    return True


def _release_lock(lease: RepoMutationLease) -> None:
    payload = _read_lock_payload(lease.lock_path)
    if payload.get("token") != lease.token:
        _append_event(
            lease.event_log_path,
            {
                "event": "release_skipped",
                "at": _utc_now(),
                "owner": lease.owner,
                "lock_path": lease.lock_path.as_posix(),
                "reason": "lock ownership mismatch",
            },
        )
        return
    try:
        lease.lock_path.unlink()
    except FileNotFoundError:
        return
    _append_event(
        lease.event_log_path,
        {
            "event": "released",
            "at": _utc_now(),
            "owner": lease.owner,
            "token": lease.token,
            "lock_path": lease.lock_path.as_posix(),
        },
    )


def acquire_repo_mutation_lock(
    *,
    repo_root: Path,
    owner: str,
    timeout_sec: float = DEFAULT_MUTATION_LOCK_TIMEOUT_SEC,
    poll_interval_sec: float = 0.25,
    lock_path: Path = DEFAULT_REPO_LOCK_RELATIVE,
    event_log_path: Path = DEFAULT_EVENT_LOG_RELATIVE,
) -> RepoMutationLease:
    normalized_owner = owner.strip() or "unknown-owner"
    resolved_repo_root = repo_root.resolve()
    resolved_lock_path = _resolve_path(resolved_repo_root, lock_path)
    resolved_event_log = _resolve_path(resolved_repo_root, event_log_path)
    index_lock = _index_lock_path(resolved_repo_root)
    started = time.monotonic()
    timeout_sec = max(float(timeout_sec), 0.0)
    poll_interval_sec = max(float(poll_interval_sec), 0.05)
    token = uuid.uuid4().hex

    _append_event(
        resolved_event_log,
        {
            "event": "acquire_started",
            "at": _utc_now(),
            "owner": normalized_owner,
            "lock_path": resolved_lock_path.as_posix(),
            "index_lock_path": index_lock.as_posix(),
            "timeout_sec": timeout_sec,
            "poll_interval_sec": poll_interval_sec,
        },
    )

    while True:
        waited = round(time.monotonic() - started, 3)
        if index_lock.exists():
            _append_event(
                resolved_event_log,
                {
                    "event": "index_lock_detected",
                    "at": _utc_now(),
                    "owner": normalized_owner,
                    "index_lock_path": index_lock.as_posix(),
                    "waited_sec": waited,
                },
            )
            raise RepoMutationLockError(
                "governed repo mutation lock blocked by active `.git/index.lock`; "
                "a git write operation may still be running. "
                "Retry after the process releases index.lock; do not delete it blindly."
            )

        payload = {
            "owner": normalized_owner,
            "token": token,
            "pid": os.getpid(),
            "acquired_at": _utc_now(),
            "repo_root": resolved_repo_root.as_posix(),
        }
        if _create_lock_file(resolved_lock_path, payload):
            lease = RepoMutationLease(
                repo_root=resolved_repo_root,
                lock_path=resolved_lock_path,
                event_log_path=resolved_event_log,
                owner=normalized_owner,
                token=token,
                acquired_at=str(payload["acquired_at"]),
                waited_sec=waited,
            )
            _append_event(
                resolved_event_log,
                {
                    "event": "acquired",
                    "at": _utc_now(),
                    "owner": normalized_owner,
                    "token": token,
                    "lock_path": resolved_lock_path.as_posix(),
                    "waited_sec": waited,
                },
            )
            return lease

        if waited >= timeout_sec:
            holder = _read_lock_payload(resolved_lock_path)
            holder_owner = str(holder.get("owner", "unknown-owner")).strip() or "unknown-owner"
            holder_at = str(holder.get("acquired_at", "unknown-time")).strip() or "unknown-time"
            _append_event(
                resolved_event_log,
                {
                    "event": "timeout",
                    "at": _utc_now(),
                    "owner": normalized_owner,
                    "lock_path": resolved_lock_path.as_posix(),
                    "waited_sec": waited,
                    "holder_owner": holder_owner,
                    "holder_acquired_at": holder_at,
                },
            )
            raise RepoMutationLockError(
                "governed repo mutation lock timeout: another governed write is still active "
                f"(holder={holder_owner}, acquired_at={holder_at}). "
                "Retry contract: wait for the active run to complete, then rerun."
            )

        time.sleep(poll_interval_sec)


@contextmanager
def repo_mutation_lock(
    *,
    repo_root: Path,
    owner: str,
    timeout_sec: float = DEFAULT_MUTATION_LOCK_TIMEOUT_SEC,
    poll_interval_sec: float = 0.25,
    lock_path: Path = DEFAULT_REPO_LOCK_RELATIVE,
    event_log_path: Path = DEFAULT_EVENT_LOG_RELATIVE,
) -> Iterator[RepoMutationLease]:
    lease = acquire_repo_mutation_lock(
        repo_root=repo_root,
        owner=owner,
        timeout_sec=timeout_sec,
        poll_interval_sec=poll_interval_sec,
        lock_path=lock_path,
        event_log_path=event_log_path,
    )
    try:
        yield lease
    finally:
        _release_lock(lease)
