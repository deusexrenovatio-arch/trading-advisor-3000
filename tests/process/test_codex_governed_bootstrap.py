from __future__ import annotations

import json
import sys
from pathlib import Path


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
