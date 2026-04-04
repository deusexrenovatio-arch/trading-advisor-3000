from __future__ import annotations

import json
import sys
from pathlib import Path
from subprocess import CompletedProcess

import pytest


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
        lambda repo_root, request, session_mode: {"action": "reused", "message": "ok", "payload": {}},
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
    timeout_index = forwarded.index("--mutation-lock-timeout-sec")
    assert forwarded[timeout_index + 1] == str(max(float(bootstrap.DEFAULT_MUTATION_LOCK_TIMEOUT_SEC), 0.0))
    state = json.loads((tmp_path / ".runlogs/codex-governed-entry/bootstrap-state.json").read_text(encoding="utf-8"))
    assert state["session_action"] == "reused"


def test_bootstrap_starts_session_when_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)

    calls: list[tuple[Path, str, str]] = []

    def fake_ensure(repo_root: Path, request: str, session_mode: str) -> dict[str, object]:
        calls.append((repo_root, request, session_mode))
        return {"action": "started", "message": "started", "payload": {}}

    monkeypatch.setattr(bootstrap, "ensure_active_session", fake_ensure)
    monkeypatch.setattr(bootstrap, "governed_entry_main", lambda argv: 0)

    code = bootstrap.main(["--request", "take latest package", "--route", "auto", "--dry-run"])
    assert code == 0
    assert calls == [(tmp_path, "take latest package", "legacy-full")]


def test_bootstrap_fails_closed_on_reused_session_mode_mismatch(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    expected_lock = tmp_path / ".runlogs/task-session/session-lock.json"
    payload = {"session_id": "TS-demo"}

    monkeypatch.setattr(bootstrap, "default_session_lock_path", lambda repo_root: expected_lock)
    monkeypatch.setattr(bootstrap, "load_session_lock", lambda path: payload if path == expected_lock else {})
    monkeypatch.setattr(bootstrap, "evaluate_session_lock", lambda loaded, repo_root: (True, "ok"))

    called = {"entry": False}

    def fake_entry(argv: list[str]) -> int:
        called["entry"] = True
        return 0

    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "verify tracked session reuse",
            "--route",
            "package",
            "--package-path",
            "incoming.zip",
            "--session-mode",
            "tracked_session",
            "--dry-run",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert "active task session mode mismatch" in captured.err
    assert called["entry"] is False
    assert not (tmp_path / ".runlogs/codex-governed-entry/bootstrap-state.json").exists()


def test_bootstrap_normalizes_session_mode_alias_before_writing_state(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "ensure_active_session",
        lambda repo_root, request, session_mode: {
            "action": "reused",
            "message": "ok",
            "payload": {},
            "session_mode": session_mode,
        },
    )

    captured: dict[str, object] = {}

    def fake_entry(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "reuse active session with legacy alias",
            "--route",
            "package",
            "--package-path",
            "incoming.zip",
            "--session-mode",
            "legacy",
            "--dry-run",
        ]
    )

    assert code == 0
    forwarded = captured["argv"]
    session_mode_index = forwarded.index("--session-mode")
    assert forwarded[session_mode_index + 1] == "legacy-full"
    state = json.loads((tmp_path / ".runlogs/codex-governed-entry/bootstrap-state.json").read_text(encoding="utf-8"))
    assert state["session_mode"] == "legacy-full"


def test_bootstrap_normalizes_route_mode_before_writing_state(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "ensure_active_session",
        lambda repo_root, request, session_mode: {
            "action": "reused",
            "message": "ok",
            "payload": {},
            "session_mode": session_mode,
        },
    )

    captured: dict[str, object] = {}

    def fake_entry(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "normalize route and snapshot markers",
            "--route",
            "package",
            "--package-path",
            "incoming.zip",
            "--route-mode",
            "legacy",
            "--snapshot-mode",
            "phase-state",
            "--dry-run",
        ]
    )

    assert code == 0
    forwarded = captured["argv"]
    route_mode_index = forwarded.index("--route-mode")
    snapshot_mode_index = forwarded.index("--snapshot-mode")
    assert forwarded[route_mode_index + 1] == "legacy-wrapper"
    assert forwarded[snapshot_mode_index + 1] == "phase-state"
    state = json.loads((tmp_path / ".runlogs/codex-governed-entry/bootstrap-state.json").read_text(encoding="utf-8"))
    assert state["route_mode"] == "legacy-wrapper"
    assert state["snapshot_mode"] == "phase-state"


def test_bootstrap_fails_closed_on_invalid_route_mode_before_session_or_state(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    called = {"session": False, "entry": False}

    def fake_session(*args, **kwargs):
        called["session"] = True
        return {"action": "reused", "message": "ok", "payload": {}}

    def fake_entry(argv: list[str]) -> int:
        called["entry"] = True
        return 0

    monkeypatch.setattr(bootstrap, "ensure_active_session", fake_session)
    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "reject invalid route mode",
            "--route",
            "package",
            "--package-path",
            "incoming.zip",
            "--route-mode",
            "invalid-mode",
            "--dry-run",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert "unknown route mode" in captured.err
    assert called["session"] is False
    assert called["entry"] is False
    assert not (tmp_path / ".runlogs/codex-governed-entry/bootstrap-state.json").exists()


def test_bootstrap_plan_only_package_route_omits_empty_profile(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "ensure_active_session",
        lambda repo_root, request, session_mode: {"action": "reused", "message": "ok", "payload": {}},
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
        lambda repo_root, request, session_mode: {"action": "reused", "message": "ok", "payload": {}},
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


def test_bootstrap_forwards_stacked_followup_contract_arguments(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bootstrap, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "ensure_active_session",
        lambda repo_root, request, session_mode: {"action": "reused", "message": "ok", "payload": {}},
    )

    captured: dict[str, object] = {}

    def fake_entry(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(bootstrap, "governed_entry_main", fake_entry)

    code = bootstrap.main(
        [
            "--request",
            "continue stacked follow-up",
            "--route",
            "stacked-followup",
            "--execution-contract",
            "docs/codex/contracts/demo.execution-contract.md",
            "--parent-brief",
            "docs/codex/modules/demo.parent.md",
            "--module-slug",
            "demo",
            "--predecessor-ref",
            "merge-123",
            "--source-branch",
            "feature/split-contour",
            "--new-base-ref",
            "origin/main",
            "--carry-surface",
            "runtime_api",
            "--temporary-downgrade-surface",
            "legacy_truth_patch",
            "--dry-run",
        ]
    )

    assert code == 0
    forwarded = captured["argv"]
    assert "--route" in forwarded and "stacked-followup" in forwarded
    assert "--predecessor-ref" in forwarded and "merge-123" in forwarded
    assert "--source-branch" in forwarded and "feature/split-contour" in forwarded
    assert "--carry-surface" in forwarded and "runtime_api" in forwarded
    assert "--temporary-downgrade-surface" in forwarded and "legacy_truth_patch" in forwarded


def test_ensure_active_session_checks_repo_specific_lock(monkeypatch, tmp_path: Path) -> None:
    expected_lock = tmp_path / ".runlogs/task-session/session-lock.json"
    payload = {"session_id": "TS-demo"}

    def fake_default_lock(repo_root: Path) -> Path:
        assert repo_root == tmp_path
        return expected_lock

    monkeypatch.setattr(bootstrap, "default_session_lock_path", fake_default_lock)
    monkeypatch.setattr(bootstrap, "load_session_lock", lambda path: payload if path == expected_lock else {})
    monkeypatch.setattr(bootstrap, "evaluate_session_lock", lambda loaded, repo_root: (True, "ok"))

    result = bootstrap.ensure_active_session(
        repo_root=tmp_path,
        request="reuse current session",
        session_mode="legacy-full",
    )

    assert result["action"] == "reused"
    assert result["payload"] == payload
    assert result["session_mode"] == "legacy-full"


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

    result = bootstrap.ensure_active_session(
        repo_root=tmp_path,
        request="begin a new session",
        session_mode="tracked_session",
    )

    assert captured["cwd"] == tmp_path
    assert result["action"] == "started"
    assert "--mode" in captured["command"]
    assert "tracked_session" in captured["command"]


def test_ensure_active_session_raises_on_reused_mode_mismatch(monkeypatch, tmp_path: Path) -> None:
    expected_lock = tmp_path / ".runlogs/task-session/session-lock.json"
    payload = {"session_id": "TS-demo", "session_mode": "legacy-full"}

    monkeypatch.setattr(bootstrap, "default_session_lock_path", lambda repo_root: expected_lock)
    monkeypatch.setattr(bootstrap, "load_session_lock", lambda path: payload if path == expected_lock else {})
    monkeypatch.setattr(bootstrap, "evaluate_session_lock", lambda loaded, repo_root: (True, "ok"))

    with pytest.raises(bootstrap.GovernedBootstrapError, match="mode mismatch"):
        bootstrap.ensure_active_session(
            repo_root=tmp_path,
            request="reuse current session",
            session_mode="tracked_session",
        )
