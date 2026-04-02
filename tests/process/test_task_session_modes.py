from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from task_session import normalize_session_mode  # noqa: E402


def test_normalize_session_mode_accepts_legacy_aliases() -> None:
    assert normalize_session_mode("full") == "legacy-full"
    assert normalize_session_mode("legacy-full") == "legacy-full"
    assert normalize_session_mode("legacy") == "legacy-full"


def test_normalize_session_mode_accepts_tracked_aliases() -> None:
    assert normalize_session_mode("tracked") == "tracked_session"
    assert normalize_session_mode("tracked_session") == "tracked_session"
    assert normalize_session_mode("tracked-session") == "tracked_session"


def test_normalize_session_mode_rejects_unknown_value() -> None:
    try:
        normalize_session_mode("invalid")
    except ValueError as exc:
        assert "unknown session mode" in str(exc)
    else:  # pragma: no cover - fail branch
        raise AssertionError("expected invalid session mode to raise ValueError")
