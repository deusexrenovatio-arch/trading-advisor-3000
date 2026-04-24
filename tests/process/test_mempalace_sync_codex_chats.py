from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from mempalace_sync_codex_chats import resolve_sessions_dir  # noqa: E402


def test_resolve_sessions_dir_prefers_codex_home(monkeypatch, tmp_path) -> None:
    codex_home = tmp_path / "CodexHome"
    codex_sessions = codex_home / "sessions"
    codex_sessions.mkdir(parents=True)

    legacy_home = tmp_path / "legacy-home"
    legacy_sessions = legacy_home / ".codex" / "sessions"
    legacy_sessions.mkdir(parents=True)

    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.delenv("MEMPAL_CONVO_DIR", raising=False)
    monkeypatch.setattr(Path, "home", lambda: legacy_home)

    assert resolve_sessions_dir() == codex_sessions


def test_resolve_sessions_dir_falls_back_to_legacy_home(monkeypatch, tmp_path) -> None:
    legacy_home = tmp_path / "legacy-home"
    legacy_sessions = legacy_home / ".codex" / "sessions"
    legacy_sessions.mkdir(parents=True)

    monkeypatch.delenv("CODEX_HOME", raising=False)
    monkeypatch.delenv("MEMPAL_CONVO_DIR", raising=False)
    monkeypatch.setattr(Path, "home", lambda: legacy_home)

    assert resolve_sessions_dir() == legacy_sessions


def test_resolve_sessions_dir_honors_explicit_override(monkeypatch, tmp_path) -> None:
    explicit = tmp_path / "manual" / "sessions"
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "CodexHome"))
    monkeypatch.setenv("MEMPAL_CONVO_DIR", str(tmp_path / "ignored"))

    assert resolve_sessions_dir(str(explicit)) == explicit
