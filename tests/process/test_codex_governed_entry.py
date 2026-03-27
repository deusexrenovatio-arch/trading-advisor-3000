from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_governed_entry import (  # noqa: E402
    DEFAULT_ROUTE_STATE,
    decide_route,
    discover_active_module,
    main,
    normalize_route_argv,
    resolve_repo_root,
    write_route_state,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _phase_module(repo: Path, slug: str = "demo") -> tuple[Path, Path, Path]:
    parent = repo / "docs/codex/modules" / f"{slug}.parent.md"
    phase = repo / "docs/codex/modules" / f"{slug}.phase-01.md"
    contract = repo / "docs/codex/contracts" / f"{slug}.execution-contract.md"
    _write(
        parent,
        f"""# Module Parent Brief

## Next Phase To Execute
- docs/codex/modules/{slug}.phase-01.md
""",
    )
    _write(
        phase,
        """# Module Phase Brief

## Phase
- Name: Phase 01
- Status: planned

## Objective
- Build the thing.
""",
    )
    _write(
        contract,
        """# Execution Contract

## Next Allowed Unit Of Work
- Execute phase 01 only: build the thing.
""",
    )
    return parent, phase, contract


def test_discover_active_module_returns_single_candidate(tmp_path: Path) -> None:
    parent, phase, contract = _phase_module(tmp_path, "demo")
    route = discover_active_module(tmp_path)
    assert route is not None
    assert route.parent_brief == parent.resolve()
    assert route.current_phase == phase.resolve()
    assert route.execution_contract == contract.resolve()


def test_decide_route_prefers_active_module_over_inbox_package(tmp_path: Path) -> None:
    _phase_module(tmp_path, "demo")
    inbox = tmp_path / "docs/codex/packages/inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    package = inbox / "spec.zip"
    package.write_bytes(b"zip-placeholder")

    decision = decide_route(
        repo_root=tmp_path,
        explicit_package=None,
        explicit_contract=None,
        explicit_parent=None,
        inbox=inbox,
    )
    assert decision.route == "continue"
    assert decision.module is not None
    assert "continuation wins over package intake" in decision.reason


def test_decide_route_uses_explicit_package_when_requested(tmp_path: Path) -> None:
    _phase_module(tmp_path, "demo")
    package = tmp_path / "incoming.zip"
    package.write_bytes(b"zip-placeholder")

    decision = decide_route(
        repo_root=tmp_path,
        explicit_package=package,
        explicit_contract=None,
        explicit_parent=None,
        inbox=tmp_path / "docs/codex/packages/inbox",
    )
    assert decision.route == "package"
    assert decision.package_path == package.resolve()


def test_write_route_state_records_decision(tmp_path: Path) -> None:
    parent, phase, contract = _phase_module(tmp_path, "demo")
    decision = decide_route(
        repo_root=tmp_path,
        explicit_package=None,
        explicit_contract=contract,
        explicit_parent=parent,
        inbox=tmp_path / "docs/codex/packages/inbox",
    )
    target = tmp_path / DEFAULT_ROUTE_STATE
    write_route_state(target, decision)
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["route"] == "continue"
    assert payload["module"]["current_phase"] == phase.resolve().as_posix()


def test_normalize_route_argv_accepts_positional_route_alias() -> None:
    argv = normalize_route_argv(["package", "--package-path", "docs/codex/packages/inbox/sample.zip"])
    assert argv[:2] == ["--route", "package"]
    assert "--package-path" in argv


def test_main_accepts_positional_package_route_in_plan_only_dry_run(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "package",
            "--package-path",
            "incoming.zip",
            "--mode",
            "plan-only",
            "--route-state-file",
            ".runlogs/test-route.json",
            "--dry-run",
        ],
    )

    code = main()

    assert code == 0
    payload = json.loads((tmp_path / ".runlogs/test-route.json").read_text(encoding="utf-8"))
    assert payload["route"] == "package"
    assert payload["package_path"] == (tmp_path / "incoming.zip").resolve().as_posix()


def test_package_route_prints_continue_hint_and_persists_durable_handoff_when_active_module_is_materialized(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)

    package = tmp_path / "incoming.zip"
    package.write_bytes(b"zip-placeholder")

    def fake_package_main(argv: list[str]) -> int:
        _phase_module(tmp_path, "demo")
        return 0

    monkeypatch.setattr("codex_governed_entry.package_main", fake_package_main)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "package",
            "--package-path",
            "incoming.zip",
            "--route-state-file",
            ".runlogs/test-route.json",
        ],
    )

    code = main()

    captured = capsys.readouterr()
    assert code == 0
    assert "package_route_outcome: active_module_detected" in captured.out
    assert "next_governed_route: continue" in captured.out
    assert "docs/codex/contracts/demo.execution-contract.md" in captured.out
    assert "docs/codex/modules/demo.parent.md" in captured.out
    payload = json.loads((tmp_path / ".runlogs/test-route.json").read_text(encoding="utf-8"))
    assert payload["route"] == "continue"
    assert payload["module"]["execution_contract"].endswith("docs/codex/contracts/demo.execution-contract.md")
    assert payload["module"]["parent_brief"].endswith("docs/codex/modules/demo.parent.md")


def test_resolve_repo_root_points_at_script_parent() -> None:
    assert resolve_repo_root() == ROOT
