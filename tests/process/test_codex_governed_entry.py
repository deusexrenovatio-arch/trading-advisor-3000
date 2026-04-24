from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_governed_entry import (  # noqa: E402
    DEFAULT_MUTATION_LOCK_TIMEOUT_SEC,
    DEFAULT_ROUTE_STATE,
    GovernedEntryError,
    choose_latest_package,
    decide_route,
    discover_active_module,
    main,
    normalize_route_argv,
    package_main,
    resolve_repo_root,
    write_route_state,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _phase_module(repo: Path, slug: str = "demo", phase_id: str = "01") -> tuple[Path, Path, Path]:
    parent = repo / "docs/codex/modules" / f"{slug}.parent.md"
    phase = repo / "docs/codex/modules" / f"{slug}.phase-{phase_id}.md"
    contract = repo / "docs/codex/contracts" / f"{slug}.execution-contract.md"
    _write(
        parent,
        f"""# Module Parent Brief

## Next Phase To Execute
- docs/codex/modules/{slug}.phase-{phase_id}.md
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
        requested_route="auto",
        explicit_package=None,
        explicit_contract=None,
        explicit_parent=None,
        inbox=inbox,
        module_slug=None,
        module_priority=None,
        ambiguity_report_path=tmp_path / ".runlogs/ambiguity.json",
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
        requested_route="auto",
        explicit_package=package,
        explicit_contract=None,
        explicit_parent=None,
        inbox=tmp_path / "docs/codex/packages/inbox",
        module_slug=None,
        module_priority=None,
        ambiguity_report_path=tmp_path / ".runlogs/ambiguity.json",
    )
    assert decision.route == "package"
    assert decision.package_path == package.resolve()


def test_decide_route_emits_machine_readable_ambiguity_report_for_multiple_modules(tmp_path: Path) -> None:
    _phase_module(tmp_path, "alpha", phase_id="01")
    _phase_module(tmp_path, "beta", phase_id="02")
    report_path = tmp_path / ".runlogs/codex-governed-entry/ambiguity.json"

    with pytest.raises(GovernedEntryError, match="ambiguity report written to"):
        decide_route(
            repo_root=tmp_path,
            requested_route="auto",
            explicit_package=None,
            explicit_contract=None,
            explicit_parent=None,
            inbox=tmp_path / "docs/codex/packages/inbox",
            module_slug=None,
            module_priority=None,
            ambiguity_report_path=report_path,
        )

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["route_signal"] == "entry:multi-module-ambiguity"
    assert payload["requested_route"] == "auto"
    assert {item["slug"] for item in payload["candidates"]} == {"alpha", "beta"}


def test_decide_route_resolves_multi_module_with_explicit_slug(tmp_path: Path) -> None:
    _phase_module(tmp_path, "alpha", phase_id="01")
    _phase_module(tmp_path, "beta", phase_id="02")

    decision = decide_route(
        repo_root=tmp_path,
        requested_route="continue",
        explicit_package=None,
        explicit_contract=None,
        explicit_parent=None,
        inbox=tmp_path / "docs/codex/packages/inbox",
        module_slug="beta",
        module_priority=None,
        ambiguity_report_path=tmp_path / ".runlogs/codex-governed-entry/ambiguity.json",
    )

    assert decision.route == "continue"
    assert decision.module is not None
    assert decision.module.slug == "beta"
    assert "explicit module slug" in (decision.module_resolution or "")


def test_decide_route_resolves_multi_module_with_phase_order_priority(tmp_path: Path) -> None:
    _phase_module(tmp_path, "alpha", phase_id="02")
    _phase_module(tmp_path, "beta", phase_id="01")

    decision = decide_route(
        repo_root=tmp_path,
        requested_route="continue",
        explicit_package=None,
        explicit_contract=None,
        explicit_parent=None,
        inbox=tmp_path / "docs/codex/packages/inbox",
        module_slug=None,
        module_priority="phase-order",
        ambiguity_report_path=tmp_path / ".runlogs/codex-governed-entry/ambiguity.json",
    )

    assert decision.module is not None
    assert decision.module.slug == "beta"
    assert "phase-order" in (decision.module_resolution or "")


def test_write_route_state_records_decision(tmp_path: Path) -> None:
    parent, phase, contract = _phase_module(tmp_path, "demo")
    decision = decide_route(
        repo_root=tmp_path,
        requested_route="auto",
        explicit_package=None,
        explicit_contract=contract,
        explicit_parent=parent,
        inbox=tmp_path / "docs/codex/packages/inbox",
        module_slug=None,
        module_priority=None,
        ambiguity_report_path=tmp_path / ".runlogs/ambiguity.json",
    )
    target = tmp_path / DEFAULT_ROUTE_STATE
    write_route_state(target, decision)
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["route"] == "continue"
    assert payload["phase_brief"] == phase.resolve().as_posix()
    assert payload["module"]["current_phase"] == phase.resolve().as_posix()


def test_normalize_route_argv_accepts_positional_route_alias() -> None:
    argv = normalize_route_argv(["package", "--package-path", "docs/codex/packages/inbox/sample.zip"])
    assert argv[:2] == ["--route", "package"]
    assert "--package-path" in argv


def test_normalize_route_argv_accepts_stacked_followup_alias() -> None:
    argv = normalize_route_argv(["stacked-followup", "--execution-contract", "docs/codex/contracts/demo.execution-contract.md"])
    assert argv[:2] == ["--route", "stacked-followup"]


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


def test_package_route_stops_at_intake_checkpoint_by_default(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)

    package = tmp_path / "incoming.zip"
    package.write_bytes(b"zip-placeholder")

    def fake_package_main(argv: list[str]) -> int:
        assert "--continue-after-intake" not in argv
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
    assert "package_route_outcome: intake_checkpoint_required" in captured.out
    assert "next_governed_route: package" in captured.out
    assert "--continue-after-intake" in captured.out


def test_package_route_allows_materialization_only_with_explicit_continue_after_intake(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)

    package = tmp_path / "incoming.zip"
    package.write_bytes(b"zip-placeholder")

    def fake_package_main(argv: list[str]) -> int:
        assert "--continue-after-intake" in argv
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
            "--continue-after-intake",
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


def test_main_writes_dual_mode_metadata_into_route_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "--route",
            "package",
            "--route-mode",
            "explicit",
            "--session-mode",
            "tracked_session",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "ops",
            "--package-path",
            "incoming.zip",
            "--route-state-file",
            ".runlogs/test-route.json",
            "--dry-run",
        ],
    )

    code = main()

    assert code == 0
    payload = json.loads((tmp_path / ".runlogs/test-route.json").read_text(encoding="utf-8"))
    assert payload["route_mode"] == "explicit-dual-mode"
    assert payload["session_mode"] == "tracked_session"
    assert payload["snapshot_mode"] == "changed-files"
    assert payload["profile"] == "ops"
    assert payload["entry_route"] == "package"


def test_main_rejects_positional_alias_in_explicit_route_mode(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "package",
            "--route-mode",
            "explicit",
            "--package-path",
            "incoming.zip",
            "--dry-run",
        ],
    )

    code = main()

    assert code == 2


def test_continue_route_forwards_profile_to_orchestrator(monkeypatch, tmp_path: Path) -> None:
    parent, _phase, contract = _phase_module(tmp_path, "demo")
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)

    calls: list[list[str]] = []

    def fake_orchestrator(argv: list[str]) -> int:
        calls.append(list(argv))
        return 0

    monkeypatch.setattr("codex_governed_entry.orchestrator_main", fake_orchestrator)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "--route",
            "continue",
            "--execution-contract",
            contract.as_posix(),
            "--parent-brief",
            parent.as_posix(),
            "--backend",
            "simulate",
            "--profile",
            "ops",
            "--max-remediation-cycles",
            "0",
        ],
    )

    code = main()

    assert code == 0
    assert len(calls) == 2
    assert any("--profile" in call and "ops" in call for call in calls)
    expected_timeout = str(max(float(DEFAULT_MUTATION_LOCK_TIMEOUT_SEC), 0.0))
    for call in calls:
        timeout_index = call.index("--mutation-lock-timeout-sec")
        assert call[timeout_index + 1] == expected_timeout


def test_continue_route_omits_model_flags_when_not_explicitly_requested(monkeypatch, tmp_path: Path) -> None:
    parent, _phase, contract = _phase_module(tmp_path, "demo")
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)

    calls: list[list[str]] = []

    def fake_orchestrator(argv: list[str]) -> int:
        calls.append(list(argv))
        return 0

    monkeypatch.setattr("codex_governed_entry.orchestrator_main", fake_orchestrator)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "--route",
            "continue",
            "--execution-contract",
            contract.as_posix(),
            "--parent-brief",
            parent.as_posix(),
            "--backend",
            "simulate",
            "--max-remediation-cycles",
            "0",
        ],
    )

    code = main()

    assert code == 0
    assert len(calls) == 2
    for call in calls:
        assert "--worker-model" not in call
        assert "--acceptor-model" not in call
        assert "--remediation-model" not in call


def test_lazy_package_import_wrappers_delegate_on_demand(monkeypatch, tmp_path: Path) -> None:
    stub = types.ModuleType("codex_from_package")

    def fake_choose_latest_package(inbox: Path) -> Path | None:
        return inbox / "latest.zip"

    def fake_main(argv: list[str]) -> int:
        return 7

    stub.choose_latest_package = fake_choose_latest_package  # type: ignore[attr-defined]
    stub.main = fake_main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "codex_from_package", stub)

    assert choose_latest_package(tmp_path) == (tmp_path / "latest.zip")
    assert package_main(["--route", "package"]) == 7


def test_main_stacked_followup_writes_continuation_contract(monkeypatch, tmp_path: Path) -> None:
    parent, _phase, contract = _phase_module(tmp_path, "demo")
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        "codex_governed_entry._git_stdout",
        lambda repo_root, *args: {
            ("rev-parse", "merge-123^{commit}"): "sha-merge-123",
            ("rev-parse", "feature/split-contour^{commit}"): "sha-feature-split",
            ("rev-parse", "origin/main^{commit}"): "sha-origin-main",
        }[args],
    )
    monkeypatch.setattr("codex_governed_entry._git_is_ancestor", lambda repo_root, ancestor, descendant: True)
    monkeypatch.setattr("codex_governed_entry.orchestrator_main", lambda argv: 0)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "--route",
            "stacked-followup",
            "--execution-contract",
            contract.as_posix(),
            "--parent-brief",
            parent.as_posix(),
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
            "--followup-contract-file",
            ".runlogs/codex-governed-entry/followup.json",
            "--route-state-file",
            ".runlogs/codex-governed-entry/route.json",
            "--dry-run",
        ],
    )

    code = main()

    assert code == 0
    followup = json.loads(
        (tmp_path / ".runlogs/codex-governed-entry/followup.json").read_text(encoding="utf-8")
    )
    assert followup["route"] == "stacked-followup"
    assert followup["predecessor_merge_context"]["merged_into_new_base"] is True
    route_state = json.loads(
        (tmp_path / ".runlogs/codex-governed-entry/route.json").read_text(encoding="utf-8")
    )
    assert route_state["entry_route"] == "stacked-followup"
    assert route_state["continuation_contract_path"].endswith("followup.json")


def test_main_stacked_followup_fails_closed_when_predecessor_not_merged(
    monkeypatch, tmp_path: Path
) -> None:
    parent, _phase, contract = _phase_module(tmp_path, "demo")
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr("codex_governed_entry._git_stdout", lambda repo_root, *args: "sha-any")
    monkeypatch.setattr("codex_governed_entry._git_is_ancestor", lambda repo_root, ancestor, descendant: False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "--route",
            "stacked-followup",
            "--execution-contract",
            contract.as_posix(),
            "--parent-brief",
            parent.as_posix(),
            "--predecessor-ref",
            "merge-123",
            "--source-branch",
            "feature/split-contour",
            "--new-base-ref",
            "origin/main",
            "--carry-surface",
            "runtime_api",
            "--followup-contract-file",
            ".runlogs/codex-governed-entry/followup.json",
            "--route-state-file",
            ".runlogs/codex-governed-entry/route.json",
            "--dry-run",
        ],
    )

    code = main()

    assert code == 2
    assert not (tmp_path / ".runlogs/codex-governed-entry/route.json").exists()


def test_main_stacked_followup_forwards_continuation_contract_to_orchestrator(
    monkeypatch, tmp_path: Path
) -> None:
    parent, _phase, contract = _phase_module(tmp_path, "demo")
    monkeypatch.setattr("codex_governed_entry.resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        "codex_governed_entry._git_stdout",
        lambda repo_root, *args: {
            ("rev-parse", "merge-123^{commit}"): "sha-merge-123",
            ("rev-parse", "feature/split-contour^{commit}"): "sha-feature-split",
            ("rev-parse", "origin/main^{commit}"): "sha-origin-main",
        }[args],
    )
    monkeypatch.setattr("codex_governed_entry._git_is_ancestor", lambda repo_root, ancestor, descendant: True)

    calls: list[list[str]] = []

    def fake_orchestrator(argv: list[str]) -> int:
        calls.append(list(argv))
        return 0

    monkeypatch.setattr("codex_governed_entry.orchestrator_main", fake_orchestrator)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex_governed_entry.py",
            "--route",
            "stacked-followup",
            "--execution-contract",
            contract.as_posix(),
            "--parent-brief",
            parent.as_posix(),
            "--predecessor-ref",
            "merge-123",
            "--source-branch",
            "feature/split-contour",
            "--new-base-ref",
            "origin/main",
            "--carry-surface",
            "runtime_api",
            "--followup-contract-file",
            ".runlogs/codex-governed-entry/followup.json",
            "--backend",
            "simulate",
            "--skip-clean-check",
            "--max-remediation-cycles",
            "0",
        ],
    )

    code = main()

    assert code == 0
    assert len(calls) == 2
    for call in calls:
        assert "--entry-route" in call
        assert "stacked-followup" in call
        assert "--continuation-contract" in call
        assert any(item.endswith("/.runlogs/codex-governed-entry/followup.json") for item in call)


def test_resolve_repo_root_points_at_script_parent() -> None:
    assert resolve_repo_root() == ROOT
