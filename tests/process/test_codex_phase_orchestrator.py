from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_phase_orchestrator import build_acceptor_prompt  # noqa: E402
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
        "verification-before-completion": "---\nname: verification-before-completion\ndescription: test\nclassification: KEEP_CORE\nwave: WAVE_1\nstatus: ACTIVE\nowner_surface: CTX-OPS\nrouting_triggers:\n  - verification before completion\n---\n# verification before completion\n",
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


def _release_decision_module_fixture(repo: Path) -> tuple[Path, Path]:
    _write_acceptor_skills(repo)
    contract = repo / "docs/codex/contracts/demo.execution-contract.md"
    parent = repo / "docs/codex/modules/demo.parent.md"
    phase1 = repo / "docs/codex/modules/demo.phase-01.md"
    worker = repo / "docs/codex/prompts/phases/worker.md"
    acceptor = repo / "docs/codex/prompts/phases/acceptor.md"
    remediation = repo / "docs/codex/prompts/phases/remediation.md"

    _write(
        contract,
        """# Execution Contract

## Release Target Contract
- Target Decision: ALLOW_RELEASE_READINESS
- Target Environment: governed pipeline
- Forbidden Proof Substitutes: docs-only
- Release-Ready Proof Class: live-real

## Next Allowed Unit Of Work
- Execute phase 01 only: enforce release decision.
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
- Enforce release decision closeout.

## Release Gate Impact
- Surface Transition: demo_surface enforcement
- Minimum Proof Class: live-real
- Accepted State Label: release_decision

## Release Surface Ownership
- Owned Surfaces: demo_surface
- Delivered Proof Class: live-real
- Required Real Bindings: explicit markers, mutation lock evidence
- Target Downgrade Is Forbidden: yes
""",
    )
    _write(worker, "worker prompt\n")
    _write(acceptor, "acceptor prompt\n")
    _write(remediation, "remediation prompt\n")
    _write(
        repo / ".runlogs/codex-governed-entry/last-route.json",
        json.dumps({"snapshot_mode": "changed-files", "profile": "none"}, ensure_ascii=False, indent=2) + "\n",
    )
    _write(
        repo / "artifacts/codex/h4/route-state.attempt-01.json",
        json.dumps({"snapshot_mode": "changed-files", "profile": "none"}, ensure_ascii=False, indent=2) + "\n",
    )
    _write(
        repo / "artifacts/codex/h4/loop-gate-summary.md",
        "# Loop Gate Summary\n\n- Snapshot mode: `changed-files`\n- Profile: `none`\n",
    )
    _write(
        repo / "artifacts/codex/h4/pr-gate-summary.md",
        "# PR Gate Summary\n\n- Snapshot mode: `changed-files`\n- Profile: `none`\n",
    )
    _write(
        repo / ".runlogs/codex-governed-entry/repo-mutation-events.jsonl",
        "\n".join(
            [
                json.dumps({"event": "acquired", "owner": "test", "timestamp": "2026-04-01T10:00:00Z"}),
                json.dumps({"event": "released", "owner": "test", "timestamp": "2026-04-01T10:00:01Z"}),
            ]
        )
        + "\n",
    )
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
    run_root = tmp_path / "artifacts/codex/orchestration" / payload["run_id"]
    assert payload["chat_summary_path"] == (run_root / "chat-summary.md").as_posix()
    phase_summary = (run_root / "chat-summary.md").read_text(encoding="utf-8")
    attempt_summary = (run_root / "attempt-01" / "chat-summary.md").read_text(encoding="utf-8")
    assert "Final Status: accepted" in phase_summary
    assert "Attempt 1 (worker): PASS" in phase_summary
    assert "Attempt Kind: worker" in attempt_summary
    assert "Verdict: PASS" in attempt_summary


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


def test_orchestrator_escalates_second_remediation_attempt_to_gpt54(tmp_path: Path) -> None:
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
        max_remediation_cycles=2,
        ignore_globs=(),
        skip_clean_check=True,
    )

    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["attempts_total"] == 3
    assert payload["attempts"][0]["execution_model"] == "gpt-5.3-codex"
    assert payload["attempts"][1]["execution_model"] == "gpt-5.3-codex"
    assert payload["attempts"][2]["execution_model"] == "gpt-5.4"
    route_report = (
        tmp_path / "artifacts/codex/orchestration" / payload["run_id"] / "route-report.md"
    ).read_text(encoding="utf-8")
    assert "attempt 3: remediation -> BLOCKED (model=gpt-5.4;" in route_report


def test_orchestrator_emits_attempt_bound_release_decision_for_release_decision_phase(
    tmp_path: Path,
    monkeypatch,
) -> None:
    contract, parent = _release_decision_module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Worker completed H4 scoped checks without worker-side release emission.",
                "route_signal": "worker:phase-only",
                "files_touched": [
                    "artifacts/codex/h4/route-state.attempt-01.json",
                    "artifacts/codex/h4/loop-gate-summary.md",
                    "artifacts/codex/h4/pr-gate-summary.md",
                    ".runlogs/codex-governed-entry/repo-mutation-events.jsonl",
                ],
                "checks_run": [
                    "python scripts/codex_governed_entry.py --route continue --execution-contract docs/codex/contracts/governed-pipeline-hardening.execution-contract.md --parent-brief docs/codex/modules/governed-pipeline-hardening.parent.md --snapshot-mode changed-files --profile none --route-state-file artifacts/codex/h4/route-state.attempt-01.json --dry-run",
                    "python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none --summary-file artifacts/codex/h4/loop-gate-summary.md",
                    "python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none --summary-file artifacts/codex/h4/pr-gate-summary.md",
                ],
                "remaining_risks": [],
                "assumptions": [],
                "skips": [],
                "fallbacks": [],
                "deferred_work": [],
                "evidence_contract": {
                    "surfaces": ["demo_surface"],
                    "proof_class": "live-real",
                    "artifact_paths": [
                        "artifacts/codex/h4/route-state.attempt-01.json",
                        "artifacts/codex/h4/loop-gate-summary.md",
                        "artifacts/codex/h4/pr-gate-summary.md",
                        ".runlogs/codex-governed-entry/repo-mutation-events.jsonl",
                    ],
                    "checks": [
                        "python scripts/codex_governed_entry.py --route continue --execution-contract docs/codex/contracts/governed-pipeline-hardening.execution-contract.md --parent-brief docs/codex/modules/governed-pipeline-hardening.parent.md --snapshot-mode changed-files --profile none --route-state-file artifacts/codex/h4/route-state.attempt-01.json --dry-run"
                    ],
                    "real_bindings": ["real repository working tree (Windows)"],
                },
            }
        return {
            "verdict": "BLOCKED",
            "summary": "Acceptance remains blocked pending evidence closure.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "code-reviewer",
                "testing-suite",
                "docs-sync",
                "verification-before-completion",
            ],
            "blockers": [
                {
                    "id": "B1",
                    "title": "Upstream acceptance blocker",
                    "why": "Simulated blocker to force DENY release decision.",
                    "remediation": "Keep remediation loop active.",
                }
            ],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "recurrence_risks": [],
            "operational_exceptions": [],
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
    attempt_dir = tmp_path / "artifacts/codex/orchestration" / payload["run_id"] / "attempt-01"
    acceptance_payload = json.loads((attempt_dir / "acceptance.json").read_text(encoding="utf-8"))
    release_decision_path = Path(acceptance_payload["release_decision_path"])
    assert release_decision_path.exists()
    decision_payload = json.loads(release_decision_path.read_text(encoding="utf-8"))
    assert decision_payload["truth_sources"]["acceptance_json"] == (attempt_dir / "acceptance.json").resolve().as_posix()
    assert decision_payload["verdict"] == "DENY_RELEASE_READINESS"


def test_orchestrator_release_decision_closeout_fails_without_phase_scoped_route_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    contract, parent = _release_decision_module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Worker completed H4 scoped checks but did not publish phase-scoped route-state evidence.",
                "route_signal": "worker:phase-only",
                "files_touched": [
                    "artifacts/codex/h4/loop-gate-summary.md",
                    "artifacts/codex/h4/pr-gate-summary.md",
                    ".runlogs/codex-governed-entry/repo-mutation-events.jsonl",
                ],
                "checks_run": [
                    "python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none --summary-file artifacts/codex/h4/loop-gate-summary.md",
                    "python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none --summary-file artifacts/codex/h4/pr-gate-summary.md",
                ],
                "remaining_risks": [],
                "assumptions": [],
                "skips": [],
                "fallbacks": [],
                "deferred_work": [],
                "evidence_contract": {
                    "surfaces": ["demo_surface"],
                    "proof_class": "live-real",
                    "artifact_paths": [
                        "artifacts/codex/h4/loop-gate-summary.md",
                        "artifacts/codex/h4/pr-gate-summary.md",
                        ".runlogs/codex-governed-entry/repo-mutation-events.jsonl",
                    ],
                    "checks": [
                        "python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none --summary-file artifacts/codex/h4/loop-gate-summary.md",
                        "python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none --summary-file artifacts/codex/h4/pr-gate-summary.md",
                    ],
                    "real_bindings": ["real repository working tree (Windows)"],
                },
            }
        return {
            "verdict": "PASS",
            "summary": "Acceptance would pass, pending orchestrator release-decision closeout.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "code-reviewer",
                "testing-suite",
                "docs-sync",
                "verification-before-completion",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "recurrence_risks": [],
            "operational_exceptions": [],
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
    attempt_dir = tmp_path / "artifacts/codex/orchestration" / payload["run_id"] / "attempt-01"
    acceptance_payload = json.loads((attempt_dir / "acceptance.json").read_text(encoding="utf-8"))
    assert acceptance_payload["verdict"] == "BLOCKED"
    assert not (attempt_dir / "release-decision.json").exists()
    assert "release_decision_path" not in acceptance_payload
    assert any(
        "phase-scoped route state path" in str(item).lower()
        for item in [*acceptance_payload.get("evidence_gaps", []), acceptance_payload.get("summary", "")]
    )


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


def test_build_acceptor_prompt_includes_compact_acceptance_capsule(tmp_path: Path) -> None:
    template = tmp_path / "acceptor.md"
    _write(template, "acceptor prompt\n")
    prompt = build_acceptor_prompt(
        template_path=template,
        execution_contract_path=tmp_path / "docs/codex/contracts/demo.execution-contract.md",
        parent_path=tmp_path / "docs/codex/modules/demo.parent.md",
        phase_path=tmp_path / "docs/codex/modules/demo.phase-01.md",
        worker_report_path=tmp_path / "artifacts/codex/orchestration/run/attempt-01/worker-report.json",
        changed_files_path=tmp_path / "artifacts/codex/orchestration/run/attempt-01/changed-files.json",
        attempt=1,
    )

    assert "Hard Acceptance Capsule" in prompt
    assert "follow-up PR risk" in prompt
    assert "code-reviewer" in prompt
    assert "Execution Contract:" in prompt


def test_orchestrator_auto_blocks_acceptance_recurrence_risk(tmp_path: Path, monkeypatch) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Worker completed the requested phase changes.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["src/example.py"],
                "checks_run": ["python scripts/run_loop_gate.py --from-git --git-ref HEAD"],
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
            "summary": "Acceptor found a likely follow-up PR risk that still needs prevention.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "code-reviewer",
                "testing-suite",
                "docs-sync",
                "verification-before-completion",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "recurrence_risks": ["Legacy compatibility fallback would likely return in the next follow-up PR."],
            "operational_exceptions": [],
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
    acceptance_payload = json.loads(
        (
            tmp_path
            / "artifacts/codex/orchestration"
            / payload["run_id"]
            / "attempt-01"
            / "acceptance.json"
        ).read_text(encoding="utf-8")
    )
    assert acceptance_payload["recurrence_risks"]


def test_orchestrator_auto_blocks_worker_documentation_edits(tmp_path: Path, monkeypatch) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Worker changed docs directly.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["docs/codex/modules/demo.phase-01.md"],
                "checks_run": ["python scripts/run_loop_gate.py --from-git --git-ref HEAD"],
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
            "summary": "Acceptor would pass.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "code-reviewer",
                "testing-suite",
                "docs-sync",
                "verification-before-completion",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "recurrence_risks": [],
            "operational_exceptions": [],
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


def test_skip_clean_check_uses_worker_files_for_changed_files_snapshot(monkeypatch, tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Synthetic worker output.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["src/example.py", "scripts/example.py"],
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
                "code-reviewer",
                "testing-suite",
                "docs-sync",
                "verification-before-completion",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "recurrence_risks": [],
            "operational_exceptions": [],
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
    assert changed_files_payload["changed_files"] == ["src/example.py", "scripts/example.py"]


def test_orchestrator_binds_stacked_followup_contract_into_worker_prompt_and_state(
    tmp_path: Path,
) -> None:
    contract, parent = _module_fixture(tmp_path)
    phase = tmp_path / "docs/codex/modules/demo.phase-01.md"
    continuation_contract = tmp_path / ".runlogs/codex-governed-entry/followup.json"
    _write(
        continuation_contract,
        json.dumps(
            {
                "route": "stacked-followup",
                "module": {
                    "slug": "demo",
                    "execution_contract": contract.as_posix(),
                    "parent_brief": parent.as_posix(),
                    "current_phase": phase.as_posix(),
                },
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


def test_orchestrator_fails_closed_when_stacked_followup_contract_module_binding_mismatches(
    tmp_path: Path,
) -> None:
    contract, parent = _module_fixture(tmp_path)
    continuation_contract = tmp_path / ".runlogs/codex-governed-entry/followup.json"
    _write(
        continuation_contract,
        json.dumps(
            {
                "route": "stacked-followup",
                "module": {
                    "slug": "other",
                    "execution_contract": (tmp_path / "docs/codex/contracts/other.execution-contract.md").as_posix(),
                    "parent_brief": (tmp_path / "docs/codex/modules/other.parent.md").as_posix(),
                    "current_phase": (tmp_path / "docs/codex/modules/other.phase-01.md").as_posix(),
                },
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

    try:
        orchestrate_current_phase(
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
    except OrchestratorError as exc:
        assert "execution contract mismatch" in str(exc)
    else:
        raise AssertionError("expected mismatched module binding to fail closed")


def test_orchestrator_does_not_fail_when_acceptor_skill_file_is_missing(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    (tmp_path / ".cursor/skills/docs-sync/SKILL.md").unlink()

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
    assert payload["final_status"] == "accepted"


def test_orchestrator_auto_blocks_missing_worker_evidence_contract(tmp_path: Path, monkeypatch) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Worker without evidence contract.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["src/example.py"],
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
                "code-reviewer",
                "testing-suite",
                "docs-sync",
                "verification-before-completion",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "recurrence_risks": [],
            "operational_exceptions": [],
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
                "files_touched": ["src/example.py"],
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
                "code-reviewer",
                "testing-suite",
                "docs-sync",
                "verification-before-completion",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "recurrence_risks": [],
            "operational_exceptions": [],
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


def test_orchestrator_soft_learning_mode_does_not_block_when_hook_fails(tmp_path: Path, monkeypatch) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_learning_hook(**kwargs: object) -> dict[str, object]:
        return {
            "route_signal": "orchestrator:openspace-learning",
            "attempt": kwargs.get("attempt"),
            "attempt_kind": kwargs.get("kind"),
            "mode": kwargs.get("mode"),
            "status": "failed",
            "decision_status": "",
            "recommendation": "",
            "error": "simulated hook failure",
            "changed_files_count": len(kwargs.get("changed_files") or []),
            "command": ["python", "scripts/skill_update_decision.py"],
        }

    monkeypatch.setattr("codex_phase_orchestrator.run_openspace_learning_hook", fake_learning_hook)

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
        openspace_learning_mode="soft",
    )

    assert code == 0
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "accepted"
    assert payload["attempts"][0]["openspace_learning_status"] == "failed"
    attempt_dir = tmp_path / "artifacts/codex/orchestration" / payload["run_id"] / "attempt-01"
    report = json.loads((attempt_dir / "openspace-learning-report.json").read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    changed_files_payload = json.loads((attempt_dir / "changed-files.json").read_text(encoding="utf-8"))
    assert any("openspace-learning-report.json" in item for item in changed_files_payload["changed_files"])


def test_orchestrator_strict_learning_mode_blocks_when_hook_fails(tmp_path: Path, monkeypatch) -> None:
    contract, parent = _module_fixture(tmp_path)

    def fake_learning_hook(**kwargs: object) -> dict[str, object]:
        return {
            "route_signal": "orchestrator:openspace-learning",
            "attempt": kwargs.get("attempt"),
            "attempt_kind": kwargs.get("kind"),
            "mode": kwargs.get("mode"),
            "status": "failed",
            "decision_status": "",
            "recommendation": "",
            "error": "simulated hook failure",
            "changed_files_count": len(kwargs.get("changed_files") or []),
            "command": ["python", "scripts/skill_update_decision.py"],
        }

    monkeypatch.setattr("codex_phase_orchestrator.run_openspace_learning_hook", fake_learning_hook)

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
        openspace_learning_mode="strict",
    )

    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "blocked"
    assert payload["attempts"][0]["policy_blockers_total"] >= 1
    assert payload["attempts"][0]["openspace_learning_status"] == "failed"
    acceptance_payload = json.loads(
        (
            tmp_path
            / "artifacts/codex/orchestration"
            / payload["run_id"]
            / "attempt-01"
            / "acceptance.json"
        ).read_text(encoding="utf-8")
    )
    assert any("learning hook" in item["why"].lower() for item in acceptance_payload["policy_blockers"])
