from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_phase_orchestrator import orchestrate_current_phase  # noqa: E402
from codex_phase_orchestrator import OrchestratorError  # noqa: E402
from codex_phase_policy import RoleLaunchConfig  # noqa: E402


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _launch(model: str) -> RoleLaunchConfig:
    return RoleLaunchConfig(profile="", model=model, config_overrides=())


def _write_acceptor_skills(repo: Path) -> None:
    skills = {
        "phase-acceptance-governor": "---\nname: phase-acceptance-governor\ndescription: test\nclassification: KEEP_CORE\nwave: WAVE_1\nstatus: ACTIVE\nowner_surface: CTX-OPS\nrouting_triggers:\n  - acceptance\n---\n# phase acceptance governor\n",
        "architecture-review": "---\nname: architecture-review\ndescription: test\nclassification: KEEP_CORE\nwave: WAVE_1\nstatus: ACTIVE\nowner_surface: CTX-ARCHITECTURE\nrouting_triggers:\n  - architecture\n---\n# architecture review\n",
        "testing-suite": "---\nname: testing-suite\ndescription: test\nclassification: KEEP_CORE\nwave: WAVE_1\nstatus: ACTIVE\nowner_surface: CTX-OPS\nrouting_triggers:\n  - tests\n---\n# testing suite\n",
        "docs-sync": "---\nname: docs-sync\ndescription: test\nclassification: KEEP_CORE\nwave: WAVE_1\nstatus: ACTIVE\nowner_surface: CTX-OPS\nrouting_triggers:\n  - docs\n---\n# docs sync\n",
    }
    for skill_id, text in skills.items():
        _write(repo / ".cursor/skills" / skill_id / "SKILL.md", text)


def _module_fixture(repo: Path) -> tuple[Path, Path]:
    _write_acceptor_skills(repo)
    contract = repo / "docs/codex/contracts/demo.execution-contract.md"
    parent = repo / "docs/codex/modules/demo.parent.md"
    phase1 = repo / "docs/codex/modules/demo.phase-01.md"
    phase2 = repo / "docs/codex/modules/demo.phase-02.md"
    worker = repo / "docs/codex/prompts/phases/worker.md"
    acceptor = repo / "docs/codex/prompts/phases/acceptor.md"
    remediation = repo / "docs/codex/prompts/phases/remediation.md"

    _write(
        contract,
        """# Execution Contract

## Next Allowed Unit Of Work
- Execute phase 01 only: foundation work.
""",
    )
    _write(
        parent,
        """# Module Parent Brief

## Next Phase To Execute
- docs/codex/modules/demo.phase-01.md
""",
    )
    _write(
        phase1,
        """# Module Phase Brief

## Phase
- Name: Phase 01
- Status: planned

## Objective
- Build the foundation.

## Release Gate Impact
- Surface Transition: demo_surface planned -> implemented
- Minimum Proof Class: integration
- Accepted State Label: real_contour_closed

## Release Surface Ownership
- Owned Surfaces: demo_surface
- Delivered Proof Class: integration
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove
- Release readiness: this phase does not prove ALLOW_RELEASE_READINESS.
""",
    )
    _write(
        phase2,
        """# Module Phase Brief

## Phase
- Name: Phase 02
- Status: planned

## Objective
- Add enforcement.

## Release Gate Impact
- Surface Transition: demo_surface hardening
- Minimum Proof Class: integration
- Accepted State Label: real_contour_closed

## Release Surface Ownership
- Owned Surfaces: demo_surface
- Delivered Proof Class: integration
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove
- Release readiness: this phase does not prove ALLOW_RELEASE_READINESS.
""",
    )
    _write(worker, "worker prompt\n")
    _write(acceptor, "acceptor prompt\n")
    _write(remediation, "remediation prompt\n")
    return contract, parent


def test_orchestrator_pass_advances_next_phase(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass",
        max_remediation_cycles=2,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 0
    assert "Status: completed" in (tmp_path / "docs/codex/modules/demo.phase-01.md").read_text(encoding="utf-8")
    assert "- docs/codex/modules/demo.phase-02.md" in parent.read_text(encoding="utf-8")
    assert "Execute Phase 02 only: Add enforcement." in contract.read_text(encoding="utf-8")
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "accepted"
    assert payload["attempts_total"] == 1
    assert payload["route_mode"] == "governed-phase-orchestration"
    assert payload["attempts"][0]["acceptor_used_skills"][0] == "phase-acceptance-governor"
    assert payload["role_skill_bindings"]["acceptor"][0]["skill_id"] == "phase-acceptance-governor"


def test_orchestrator_block_then_pass_uses_remediation_loop(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="block-then-pass",
        max_remediation_cycles=2,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 0
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "accepted"
    assert payload["attempts_total"] == 2
    assert payload["attempts"][0]["verdict"] == "BLOCKED"
    assert payload["attempts"][1]["verdict"] == "PASS"
    assert any("unlock next phase" in item for item in payload["route_trace"])


def test_orchestrator_blocked_keeps_same_phase_locked(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="blocked",
        max_remediation_cycles=1,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 3
    assert "Status: blocked" in (tmp_path / "docs/codex/modules/demo.phase-01.md").read_text(encoding="utf-8")
    assert "- docs/codex/modules/demo.phase-01.md" in parent.read_text(encoding="utf-8")
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "blocked"
    assert payload["attempts_total"] == 2
    assert payload["route_trace"][-1] == "phase remains locked"


def test_orchestrator_auto_blocks_worker_shortcuts_even_if_acceptor_passes(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass-with-shortcut",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "blocked"
    assert payload["attempts"][0]["policy_blockers_total"] >= 1
    acceptance_md = (
        tmp_path
        / "artifacts/codex/orchestration"
        / payload["run_id"]
        / "attempt-01"
        / "acceptance.md"
    ).read_text(encoding="utf-8")
    assert "Silent or unapproved fallback path" in acceptance_md


def test_orchestrator_auto_blocks_acceptance_evidence_gaps(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass-with-evidence-gap",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["attempts"][0]["policy_blockers_total"] >= 1
    route_report = (
        tmp_path
        / "artifacts/codex/orchestration"
        / payload["run_id"]
        / "route-report.md"
    ).read_text(encoding="utf-8")
    assert "Route Mode: governed-phase-orchestration" in route_report
    assert "acceptor: model=gpt-5.4" in route_report
    assert "phase-acceptance-governor" in route_report


def test_skip_clean_check_uses_worker_files_for_changed_files_snapshot(monkeypatch, tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Synthetic worker output.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["docs/example.md", "scripts/example.py"],
                "checks_run": [],
                "remaining_risks": [],
                "assumptions": [],
                "skips": [],
                "fallbacks": [],
                "deferred_work": [],
                "evidence_contract": {
                    "surfaces": ["demo_surface"],
                    "proof_class": "integration",
                    "artifact_paths": ["artifacts/demo-proof.json"],
                    "checks": ["python scripts/run_loop_gate.py --from-git --git-ref HEAD"],
                    "real_bindings": [],
                },
            }
        return {
            "verdict": "PASS",
            "summary": "Synthetic acceptance pass.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "testing-suite",
                "docs-sync",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
        }

    monkeypatch.setattr("codex_phase_orchestrator.run_role", fake_run_role)

    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )

    assert code == 0
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    changed_files_payload = json.loads(
        (
            tmp_path
            / "artifacts/codex/orchestration"
            / payload["run_id"]
            / "attempt-01"
            / "changed-files.json"
        ).read_text(encoding="utf-8")
    )
    assert changed_files_payload["changed_files"] == ["docs/example.md", "scripts/example.py"]


def test_orchestrator_binds_stacked_followup_contract_into_worker_prompt_and_state(
    tmp_path: Path,
) -> None:
    contract, parent = _module_fixture(tmp_path)
    continuation_contract = tmp_path / ".runlogs/codex-governed-entry/followup.json"
    _write(
        continuation_contract,
        json.dumps(
            {
                "route": "stacked-followup",
                "predecessor_merge_context": {
                    "ref": "merge-123",
                    "resolved_sha": "sha-merge-123",
                    "merged_into_new_base": True,
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
    )

    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
        entry_route="stacked-followup",
        continuation_contract_path=continuation_contract,
    )

    assert code == 0
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["entry_route"] == "stacked-followup"
    assert payload["continuation_contract_path"] == continuation_contract.as_posix()
    worker_prompt = (
        tmp_path / "artifacts/codex/orchestration" / payload["run_id"] / "attempt-01" / "worker-prompt.md"
    ).read_text(encoding="utf-8")
    assert "Entry Route: stacked-followup" in worker_prompt
    assert f"Continuation Contract: {continuation_contract.as_posix()}" in worker_prompt


def test_orchestrator_fails_closed_when_required_acceptor_skill_is_missing(tmp_path: Path) -> None:
    contract = tmp_path / "docs/codex/contracts/demo.execution-contract.md"
    parent = tmp_path / "docs/codex/modules/demo.parent.md"
    phase = tmp_path / "docs/codex/modules/demo.phase-01.md"
    worker = tmp_path / "docs/codex/prompts/phases/worker.md"
    acceptor = tmp_path / "docs/codex/prompts/phases/acceptor.md"
    remediation = tmp_path / "docs/codex/prompts/phases/remediation.md"

    _write_acceptor_skills(tmp_path)
    (tmp_path / ".cursor/skills/docs-sync/SKILL.md").unlink()
    _write(contract, "# Execution Contract\n\n## Next Allowed Unit Of Work\n- Execute phase 01 only: build the thing.\n")
    _write(parent, "# Module Parent Brief\n\n## Next Phase To Execute\n- docs/codex/modules/demo.phase-01.md\n")
    _write(phase, "# Module Phase Brief\n\n## Phase\n- Name: Phase 01\n- Status: planned\n\n## Objective\n- Build the thing.\n")
    _write(worker, "worker prompt\n")
    _write(acceptor, "acceptor prompt\n")
    _write(remediation, "remediation prompt\n")

    try:
        orchestrate_current_phase(
            repo_root=tmp_path,
            execution_contract_path=contract,
            parent_path=parent,
            worker_prompt_path=worker,
            acceptor_prompt_path=acceptor,
            remediation_prompt_path=remediation,
            artifact_root=tmp_path / "artifacts/codex/orchestration",
            backend="simulate",
            worker_launch=_launch("gpt-5.3-codex"),
            acceptor_launch=_launch("gpt-5.4"),
            remediation_launch=_launch("gpt-5.3-codex"),
            codex_bin=None,
            simulate_scenario="pass",
            max_remediation_cycles=0,
            ignore_globs=(),
            skip_clean_check=True,
        )
    except OrchestratorError as exc:
        assert "missing required skill binding" in str(exc)
    else:
        raise AssertionError("expected missing skill binding to fail closed")


def test_orchestrator_auto_blocks_missing_worker_evidence_contract(tmp_path: Path, monkeypatch) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Worker without evidence contract.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["docs/example.md"],
                "checks_run": ["python scripts/run_loop_gate.py --from-git --git-ref HEAD"],
                "remaining_risks": [],
                "assumptions": [],
                "skips": [],
                "fallbacks": [],
                "deferred_work": [],
            }
        return {
            "verdict": "PASS",
            "summary": "Acceptor would pass.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "testing-suite",
                "docs-sync",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
        }

    monkeypatch.setattr("codex_phase_orchestrator.run_role", fake_run_role)

    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )

    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["attempts"][0]["policy_blockers_total"] >= 1


def test_orchestrator_auto_blocks_weaker_worker_evidence_proof_class(tmp_path: Path, monkeypatch) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Worker with weak evidence contract.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["docs/example.md"],
                "checks_run": ["python scripts/run_loop_gate.py --from-git --git-ref HEAD"],
                "remaining_risks": [],
                "assumptions": [],
                "skips": [],
                "fallbacks": [],
                "deferred_work": [],
                "evidence_contract": {
                    "surfaces": ["demo_surface"],
                    "proof_class": "doc",
                    "artifact_paths": ["artifacts/demo-proof.json"],
                    "checks": ["python scripts/run_loop_gate.py --from-git --git-ref HEAD"],
                    "real_bindings": [],
                },
            }
        return {
            "verdict": "PASS",
            "summary": "Acceptor would pass.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "testing-suite",
                "docs-sync",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
        }

    monkeypatch.setattr("codex_phase_orchestrator.run_role", fake_run_role)

    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )

    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["attempts"][0]["policy_blockers_total"] >= 1
