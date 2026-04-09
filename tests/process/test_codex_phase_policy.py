from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_phase_policy import (  # noqa: E402
    AcceptanceResult,
    PhaseEvidenceRequirement,
    WorkerReport,
    apply_acceptance_policy,
)


def _acceptance_payload() -> AcceptanceResult:
    return AcceptanceResult(
        verdict="PASS",
        summary="Acceptor would pass without policy upgrades.",
        route_signal="acceptance:governed-phase-route",
        used_skills=[
            "phase-acceptance-governor",
            "architecture-review",
            "code-reviewer",
            "testing-suite",
            "docs-sync",
            "verification-before-completion",
        ],
        blockers=[],
        rerun_checks=[],
        evidence_gaps=[],
        prohibited_findings=[],
        policy_blockers=[],
        recurrence_risks=[],
        operational_exceptions=[],
    )


def test_policy_blocks_placeholder_commands_in_worker_evidence() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker report with placeholder checks.",
        route_signal="worker:phase-only",
        files_touched=["docs/example.md"],
        checks_run=[],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["enforcement_serialization_contour"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python scripts/run_loop_gate.py --changed-files <phase-scoped-files>"],
            "real_bindings": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["enforcement_serialization_contour"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=_acceptance_payload(), phase_requirement=phase_requirement)
    assert result.verdict == "BLOCKED"
    assert any("placeholder tokens are not allowed" in blocker.why for blocker in result.policy_blockers)


def test_policy_blocks_worker_release_decision_emission() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker report that emits release decision directly.",
        route_signal="worker:phase-only",
        files_touched=["artifacts/codex/h4/release-decision.json"],
        checks_run=[
            (
                "python scripts/build_governed_release_decision.py "
                "--execution-contract docs/codex/contracts/governed-pipeline-hardening.execution-contract.md "
                "--phase-brief docs/codex/modules/governed-pipeline-hardening.phase-05.md "
                "--acceptance-json artifacts/codex/orchestration/demo/attempt-01/acceptance.json "
                "--route-state .runlogs/codex-governed-entry/h4-route-state.json "
                "--loop-summary artifacts/ci/h4-loop-summary.md "
                "--pr-summary artifacts/ci/h4-pr-summary.md "
                "--mutation-events .runlogs/codex-governed-entry/repo-mutation-events.jsonl "
                "--output artifacts/codex/h4/release-decision.json"
            )
        ],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["enforcement_serialization_contour"],
            "proof_class": "live-real",
            "artifact_paths": ["artifacts/codex/h4/release-decision.json"],
            "checks": [],
            "real_bindings": ["real repository working tree (Windows)"],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["enforcement_serialization_contour"],
        delivered_proof_class="live-real",
        requires_real_bindings=True,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=_acceptance_payload(), phase_requirement=phase_requirement)
    assert result.verdict == "BLOCKED"
    assert any("release-decision emission before acceptance-owned closeout" in blocker.why for blocker in result.policy_blockers)


def test_policy_does_not_treat_release_decision_builder_code_edit_as_emission() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker updates release decision logic without emitting release package.",
        route_signal="worker:phase-only",
        files_touched=["scripts/build_governed_release_decision.py"],
        checks_run=["python -m pytest tests/process/test_build_governed_release_decision.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["enforcement_serialization_contour"],
            "proof_class": "integration",
            "artifact_paths": ["scripts/build_governed_release_decision.py"],
            "checks": ["python -m pytest tests/process/test_build_governed_release_decision.py -q"],
            "real_bindings": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["enforcement_serialization_contour"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=_acceptance_payload(), phase_requirement=phase_requirement)
    assert result.verdict == "PASS"
    assert not any(
        "release-decision emission before acceptance-owned closeout" in blocker.why
        for blocker in result.policy_blockers
    )


def test_policy_blocks_live_real_dry_run_continue_route() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker report claims live-real with dry-run continuation only.",
        route_signal="worker:phase-only",
        files_touched=[".runlogs/codex-governed-entry/h4-route-state.json"],
        checks_run=[
            (
                "python scripts/codex_governed_entry.py --route continue "
                "--execution-contract docs/codex/contracts/governed-pipeline-hardening.execution-contract.md "
                "--parent-brief docs/codex/modules/governed-pipeline-hardening.parent.md "
                "--snapshot-mode changed-files --profile none --dry-run"
            )
        ],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["enforcement_serialization_contour"],
            "proof_class": "live-real",
            "artifact_paths": [".runlogs/codex-governed-entry/h4-route-state.json"],
            "checks": [],
            "real_bindings": ["real repository working tree (Windows)"],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["enforcement_serialization_contour"],
        delivered_proof_class="live-real",
        requires_real_bindings=True,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=_acceptance_payload(), phase_requirement=phase_requirement)
    assert result.verdict == "BLOCKED"
    assert any("only dry-run governed continue commands" in blocker.why for blocker in result.policy_blockers)


def test_policy_blocks_worker_documentation_edits() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker edited docs directly.",
        route_signal="worker:phase-only",
        files_touched=["docs/codex/modules/demo.phase-01.md"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=_acceptance_payload(), phase_requirement=phase_requirement)
    assert result.verdict == "BLOCKED"
    assert any("modify documentation files directly" in blocker.why for blocker in result.policy_blockers)


def test_policy_blocks_missing_required_acceptor_review_lens() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker completed the phase cleanly.",
        route_signal="worker:phase-only",
        files_touched=["src/example.py"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
    )
    acceptance = _acceptance_payload()
    acceptance.used_skills = [
        "phase-acceptance-governor",
        "architecture-review",
        "testing-suite",
        "docs-sync",
        "verification-before-completion",
    ]
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=acceptance, phase_requirement=phase_requirement)
    assert result.verdict == "BLOCKED"
    assert any("missing required review lenses" in blocker.why for blocker in result.policy_blockers)


def test_policy_blocks_acceptance_recurrence_risk_and_operational_exception() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker completed the phase cleanly.",
        route_signal="worker:phase-only",
        files_touched=["src/example.py"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
    )
    acceptance = _acceptance_payload()
    acceptance.recurrence_risks = ["The current patch would likely produce a follow-up PR around legacy path fallback."]
    acceptance.operational_exceptions = ["Tests only pass when the host temp directory is writable via manual cleanup."]
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=acceptance, phase_requirement=phase_requirement)
    assert result.verdict == "BLOCKED"
    assert any("recurrence path" in blocker.remediation.lower() for blocker in result.policy_blockers)
    assert any("operational exception" in blocker.title.lower() for blocker in result.policy_blockers)


def test_policy_blocks_remediation_doc_edits_without_documentation_context() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Remediation changed docs without context contract.",
        route_signal="remediation:phase-only",
        files_touched=["docs/codex/modules/demo.phase-01.md"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "doc",
            "artifact_paths": ["docs/codex/modules/demo.phase-01.md"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="doc",
        requires_real_bindings=False,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=_acceptance_payload(), phase_requirement=phase_requirement)
    assert result.verdict == "BLOCKED"
    assert any("documentation_context is incomplete" in blocker.why for blocker in result.policy_blockers)


def test_policy_allows_remediation_doc_edits_with_full_documentation_context() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Remediation changed docs with full context coverage.",
        route_signal="remediation:phase-only",
        files_touched=["docs/codex/modules/demo.phase-01.md"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "doc",
            "artifact_paths": ["docs/codex/modules/demo.phase-01.md"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
        documentation_context={
            "source_documents": ["docs/codex/packages/extracted/spec.md"],
            "materialized_documents": [
                "docs/codex/contracts/demo.execution-contract.md",
                "docs/codex/modules/demo.parent.md",
                "docs/codex/modules/demo.phase-01.md",
            ],
            "preserved_goals": ["Preserve source goals without reinterpretation."],
            "preserved_acceptance_criteria": ["DoD stays measurable and unchanged."],
            "unresolved_conflicts": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="doc",
        requires_real_bindings=False,
    )

    result = apply_acceptance_policy(worker=worker, acceptance=_acceptance_payload(), phase_requirement=phase_requirement)
    assert result.verdict == "PASS"
    assert not any("documentation_context" in blocker.why for blocker in result.policy_blockers)


def test_policy_strict_learning_mode_blocks_missing_report() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker completed the phase cleanly.",
        route_signal="worker:phase-only",
        files_touched=["src/example.py"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )
    result = apply_acceptance_policy(
        worker=worker,
        acceptance=_acceptance_payload(),
        phase_requirement=phase_requirement,
        learning_mode="strict",
        openspace_learning_report=None,
    )
    assert result.verdict == "BLOCKED"
    assert any("learning report" in blocker.why.lower() for blocker in result.policy_blockers)


def test_policy_soft_learning_mode_does_not_block_failed_report() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker completed the phase cleanly.",
        route_signal="worker:phase-only",
        files_touched=["src/example.py"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )
    result = apply_acceptance_policy(
        worker=worker,
        acceptance=_acceptance_payload(),
        phase_requirement=phase_requirement,
        learning_mode="soft",
        openspace_learning_report={
            "status": "failed",
            "decision_status": "update_required",
            "recommendation": "Update governance docs.",
            "error": "command exited with code 1",
        },
    )
    assert result.verdict == "PASS"
    assert not any("learning" in blocker.title.lower() for blocker in result.policy_blockers)


def test_policy_strict_learning_mode_blocks_update_required_decision() -> None:
    worker = WorkerReport(
        status="DONE",
        summary="Worker completed the phase cleanly.",
        route_signal="worker:phase-only",
        files_touched=["src/example.py"],
        checks_run=["python -m pytest tests/process/test_codex_phase_policy.py -q"],
        remaining_risks=[],
        assumptions=[],
        skips=[],
        fallbacks=[],
        deferred_work=[],
        evidence_contract={
            "surfaces": ["demo_surface"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python -m pytest tests/process/test_codex_phase_policy.py -q"],
            "real_bindings": [],
        },
    )
    phase_requirement = PhaseEvidenceRequirement(
        owned_surfaces=["demo_surface"],
        delivered_proof_class="integration",
        requires_real_bindings=False,
    )
    result = apply_acceptance_policy(
        worker=worker,
        acceptance=_acceptance_payload(),
        phase_requirement=phase_requirement,
        learning_mode="strict",
        openspace_learning_report={
            "status": "ok",
            "decision_status": "update_required",
            "recommendation": "Update docs/workflows/skill-governance-sync.md",
        },
    )
    assert result.verdict == "BLOCKED"
    assert any("decision_status=update_required" in blocker.why for blocker in result.policy_blockers)
