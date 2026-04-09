from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import task_session  # noqa: E402


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_payload(*, worktree: Path, branch: str, binding: str) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    return {
        "session_id": "TS-demo",
        "worktree": str(worktree),
        "branch": branch,
        "session_mode": "legacy-full",
        "session_binding": binding,
        "started_at": _iso(now - timedelta(minutes=5)),
        "expires_at": _iso(now + timedelta(hours=1)),
    }


def test_normalize_session_binding_accepts_branch_aliases() -> None:
    assert task_session.normalize_session_binding("branch") == "branch-shared"
    assert task_session.normalize_session_binding("shared") == "branch-shared"
    assert task_session.normalize_session_binding("branch-shared") == "branch-shared"


def test_normalize_session_binding_accepts_worktree_aliases() -> None:
    assert task_session.normalize_session_binding("worktree") == "worktree-strict"
    assert task_session.normalize_session_binding("strict") == "worktree-strict"
    assert task_session.normalize_session_binding("worktree-strict") == "worktree-strict"


def test_evaluate_session_lock_allows_worktree_drift_for_branch_shared(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(task_session, "get_current_branch", lambda repo_root=None: "feature/relaxed")
    payload = _build_payload(
        worktree=tmp_path / "other-worktree",
        branch="feature/relaxed",
        binding="branch-shared",
    )

    ok, message = task_session.evaluate_session_lock(payload, repo_root=tmp_path)

    assert ok is True
    assert message == "ok"


def test_evaluate_session_lock_rejects_worktree_drift_for_strict_binding(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(task_session, "get_current_branch", lambda repo_root=None: "feature/strict")
    payload = _build_payload(
        worktree=tmp_path / "other-worktree",
        branch="feature/strict",
        binding="worktree-strict",
    )

    ok, message = task_session.evaluate_session_lock(payload, repo_root=tmp_path)

    assert ok is False
    assert "session mismatch" in message
    assert "binding=worktree-strict" in message


def test_evaluate_session_lock_rejects_unknown_session_binding(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(task_session, "get_current_branch", lambda repo_root=None: "feature/strict")
    payload = _build_payload(
        worktree=tmp_path,
        branch="feature/strict",
        binding="unknown-binding",
    )

    ok, message = task_session.evaluate_session_lock(payload, repo_root=tmp_path)

    assert ok is False
    assert "unknown session_binding" in message
