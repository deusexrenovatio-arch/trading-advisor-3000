from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from repo_mutation_lock import (  # noqa: E402
    RepoMutationLockError,
    acquire_repo_mutation_lock,
    repo_mutation_lock,
)


def _ensure_git_dir(repo_root: Path) -> None:
    (repo_root / ".git").mkdir(parents=True, exist_ok=True)


def _read_event_names(event_log_path: Path) -> list[str]:
    events: list[str] = []
    for raw in event_log_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(raw)
        events.append(str(payload.get("event", "")).strip())
    return events


def test_repo_mutation_lock_records_acquire_and_release_events(tmp_path: Path) -> None:
    _ensure_git_dir(tmp_path)
    event_log = tmp_path / ".runlogs/codex-governed-entry/repo-mutation-events.jsonl"

    with repo_mutation_lock(repo_root=tmp_path, owner="test-owner", timeout_sec=0.2, poll_interval_sec=0.05) as lease:
        assert lease.lock_path.exists()

    assert lease.lock_path.exists() is False
    assert event_log.exists()
    events = _read_event_names(event_log)
    assert "acquire_started" in events
    assert "acquired" in events
    assert "released" in events


def test_repo_mutation_lock_times_out_with_retry_contract(tmp_path: Path) -> None:
    _ensure_git_dir(tmp_path)

    with repo_mutation_lock(repo_root=tmp_path, owner="holder", timeout_sec=0.2, poll_interval_sec=0.05):
        with pytest.raises(RepoMutationLockError, match="Retry contract"):
            acquire_repo_mutation_lock(
                repo_root=tmp_path,
                owner="contender",
                timeout_sec=0.1,
                poll_interval_sec=0.05,
            )


def test_repo_mutation_lock_fails_closed_on_git_index_lock(tmp_path: Path) -> None:
    _ensure_git_dir(tmp_path)
    index_lock = tmp_path / ".git/index.lock"
    index_lock.write_text("lock", encoding="utf-8")

    with pytest.raises(RepoMutationLockError, match="index.lock"):
        acquire_repo_mutation_lock(
            repo_root=tmp_path,
            owner="test-owner",
            timeout_sec=0.2,
            poll_interval_sec=0.05,
        )

    assert index_lock.exists(), "index.lock must remain untouched by governed lock helper"
