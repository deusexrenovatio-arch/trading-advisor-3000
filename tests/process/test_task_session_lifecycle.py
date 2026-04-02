from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import task_session  # noqa: E402
from handoff_resolver import extract_active_task_note_path  # noqa: E402


def _index_items(path: Path) -> list[dict[str, object]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    items = payload.get("items", [])
    return [row for row in items if isinstance(row, dict)]


def _set_terminal_outcome(note_path: Path, *, outcome: str) -> None:
    text = note_path.read_text(encoding="utf-8")
    updated = text.replace("- Outcome Status: in_progress", f"- Outcome Status: {outcome}")
    note_path.write_text(updated, encoding="utf-8")


def _prepare_runtime(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(task_session, "get_repo_root", lambda: tmp_path)
    monkeypatch.setattr(task_session, "get_current_branch", lambda repo_root=None: "feature/h1-session-mode")
    monkeypatch.setattr(
        task_session,
        "_route_begin_context",
        lambda request, handoff_path: {"primary_context": "GOV-RUNTIME", "recommendations": []},
    )


def _run_lifecycle(
    *,
    monkeypatch,
    tmp_path: Path,
    capsys,
    requested_mode: str,
    expected_mode: str,
) -> None:
    _prepare_runtime(monkeypatch, tmp_path)
    handoff_path = tmp_path / "docs/session_handoff.md"
    lock_path = tmp_path / ".runlogs/task-session/session-lock.json"

    begin_code = task_session.begin_session(
        request=f"phase-02 lifecycle proof ({requested_mode})",
        ttl_hours=1,
        session_handoff_path=handoff_path,
        mode=requested_mode,
        lock_path=lock_path,
    )
    assert begin_code == 0
    begin_out = capsys.readouterr().out
    assert f"session_mode: {expected_mode}" in begin_out

    lock_payload = json.loads(lock_path.read_text(encoding="utf-8"))
    assert lock_payload["session_mode"] == expected_mode

    status_code = task_session.session_status(lock_path=lock_path, quiet=False)
    assert status_code == 0
    status_out = capsys.readouterr().out
    assert f"session_mode: {expected_mode}" in status_out

    handoff_lines = handoff_path.read_text(encoding="utf-8").splitlines()
    assert f"- Mode: {expected_mode}" in handoff_lines
    note_rel = extract_active_task_note_path(handoff_lines)
    assert note_rel is not None
    note_path = (tmp_path / note_rel).resolve()
    assert note_path.exists()

    active_index_path = (tmp_path / task_session.TASK_INDEX_ACTIVE).resolve()
    active_rows = _index_items(active_index_path)
    active_row = next(row for row in active_rows if row.get("path") == note_rel)
    assert active_row["mode"] == expected_mode
    assert active_row["status"] == "in_progress"

    _set_terminal_outcome(note_path, outcome="completed")

    end_code = task_session.end_session(session_handoff_path=handoff_path, lock_path=lock_path)
    assert end_code == 0
    capsys.readouterr()

    assert not lock_path.exists()

    handoff_after = handoff_path.read_text(encoding="utf-8").splitlines()
    assert f"- Mode: {expected_mode}" in handoff_after
    assert "- Status: completed" in handoff_after
    archive_rel = extract_active_task_note_path(handoff_after)
    assert archive_rel is not None
    assert archive_rel.startswith("docs/tasks/archive/")
    archive_path = (tmp_path / archive_rel).resolve()
    assert archive_path.exists()

    archive_index_path = (tmp_path / task_session.TASK_INDEX_ARCHIVE).resolve()
    archive_rows = _index_items(archive_index_path)
    archive_row = next(row for row in archive_rows if row.get("path") == archive_rel)
    assert archive_row["mode"] == expected_mode
    assert archive_row["status"] == "completed"

    remaining_active = _index_items(active_index_path)
    assert all(row.get("path") != note_rel for row in remaining_active)


def test_task_session_lifecycle_persists_legacy_mode_across_durable_state(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    _run_lifecycle(
        monkeypatch=monkeypatch,
        tmp_path=tmp_path,
        capsys=capsys,
        requested_mode="legacy-full",
        expected_mode="legacy-full",
    )


def test_task_session_lifecycle_persists_tracked_mode_across_durable_state(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    _run_lifecycle(
        monkeypatch=monkeypatch,
        tmp_path=tmp_path,
        capsys=capsys,
        requested_mode="tracked_session",
        expected_mode="tracked_session",
    )


def test_task_session_lifecycle_normalizes_full_alias_to_legacy_full(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    _run_lifecycle(
        monkeypatch=monkeypatch,
        tmp_path=tmp_path,
        capsys=capsys,
        requested_mode="full",
        expected_mode="legacy-full",
    )
