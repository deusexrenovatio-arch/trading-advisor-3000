from __future__ import annotations

import json
import sys
from pathlib import Path
from subprocess import CompletedProcess


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import codex_governed_bootstrap as bootstrap  # noqa: E402


def test_bootstrap_reuses_active_session_and_forwards_to_governed_entry(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "ensure_active_session",
        lambda repo_root, request: {"action": "reused", "message": "ok", "payload": {}},
    )

    captured: dict[str, object] = {}

    def fake_entry(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "continue current governed phase",
            "--route",
            "continue",
            "--execution-contract",
            "docs/codex/contracts/demo.execution-contract.md",
            "--parent-brief",
            "docs/codex/modules/demo.parent.md",
            "--dry-run",
        ]
    )
    assert code == 0
    forwarded = captured["argv"]
    assert "--route" in forwarded
    assert "continue" in forwarded
    assert "--execution-contract" in forwarded
    assert "--parent-brief" in forwarded
    state = json.loads((tmp_path / ".runlogs/codex-governed-entry/bootstrap-state.json").read_text(encoding="utf-8"))
    assert state["session_action"] == "reused"


def test_bootstrap_starts_session_when_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)

    calls: list[tuple[Path, str]] = []

    def fake_ensure(repo_root: Path, request: str) -> dict[str, object]:
        calls.append((repo_root, request))
        return {"action": "started", "message": "started", "payload": {}}

    monkeypatch.setattr(bootstrap, "ensure_active_session", fake_ensure)
    monkeypatch.setattr(bootstrap, "governed_entry_main", lambda argv: 0)

    code = bootstrap.main(["--request", "take latest package", "--route", "auto", "--dry-run"])
    assert code == 0
    assert calls == [(tmp_path, "take latest package")]


def test_bootstrap_plan_only_package_route_omits_empty_profile(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "ensure_active_session",
        lambda repo_root, request: {"action": "reused", "message": "ok", "payload": {}},
    )

    captured: dict[str, object] = {}

    def fake_entry(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "take latest package in plan-only mode",
            "--route",
            "package",
            "--package-path",
            "docs/codex/packages/inbox/sample.zip",
            "--mode",
            "plan-only",
            "--dry-run",
        ]
    )

    assert code == 0
    forwarded = captured["argv"]
    assert "--route" in forwarded
    assert "package" in forwarded
    assert "--package-path" in forwarded
    assert "--mode" in forwarded
    assert "plan-only" in forwarded
    assert "--profile" not in forwarded


def test_bootstrap_forwards_explicit_profile_when_requested(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "ensure_active_session",
        lambda repo_root, request: {"action": "reused", "message": "ok", "payload": {}},
    )

    captured: dict[str, object] = {}

    def fake_entry(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "take latest package with explicit profile",
            "--route",
            "package",
            "--package-path",
            "docs/codex/packages/inbox/sample.zip",
            "--mode",
            "plan-only",
            "--profile",
            "deep",
            "--dry-run",
        ]
    )

    assert code == 0
    forwarded = captured["argv"]
    assert "--profile" in forwarded
    profile_index = forwarded.index("--profile")
    assert forwarded[profile_index + 1] == "deep"


def test_ensure_active_session_checks_repo_specific_lock(monkeypatch, tmp_path: Path) -> None:
    expected_lock = tmp_path / ".runlogs/task-session/session-lock.json"
    payload = {"session_id": "TS-demo"}

    def fake_default_lock(repo_root: Path) -> Path:
        assert repo_root == tmp_path
        return expected_lock

    monkeypatch.setattr(bootstrap, "default_session_lock_path", fake_default_lock)
    monkeypatch.setattr(bootstrap, "load_session_lock", lambda path: payload if path == expected_lock else {})
    monkeypatch.setattr(bootstrap, "evaluate_session_lock", lambda loaded, repo_root: (True, "ok"))

    result = bootstrap.ensure_active_session(repo_root=tmp_path, request="reuse current session")

    assert result["action"] == "reused"
    assert result["payload"] == payload


def test_ensure_active_session_starts_new_session_when_repo_specific_lock_is_inactive(
    monkeypatch, tmp_path: Path
) -> None:
    expected_lock = tmp_path / ".runlogs/task-session/session-lock.json"

    monkeypatch.setattr(bootstrap, "default_session_lock_path", lambda repo_root: expected_lock)
    monkeypatch.setattr(bootstrap, "load_session_lock", lambda path: {})
    monkeypatch.setattr(bootstrap, "evaluate_session_lock", lambda loaded, repo_root: (False, "inactive"))

    completed = CompletedProcess(args=["python"], returncode=0, stdout="started\n", stderr="")
    captured: dict[str, object] = {}

    def fake_run(command, cwd=None, text=None, check=None, capture_output=None):
        captured["cwd"] = cwd
        captured["command"] = command
        return completed

    monkeypatch.setattr(bootstrap.subprocess, "run", fake_run)

    result = bootstrap.ensure_active_session(repo_root=tmp_path, request="begin a new session")

    assert captured["cwd"] == tmp_path
    assert result["action"] == "started"
