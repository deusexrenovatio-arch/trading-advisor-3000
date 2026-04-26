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
    normalize_acceptance_payload,
    normalize_worker_payload,
    render_acceptance_markdown,
    render_route_report,
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


def test_acceptance_payload_normalizes_result_quality_and_renders_markdown() -> None:
    acceptance = normalize_acceptance_payload(
        {
            "verdict": "PASS",
            "summary": "Acceptor scored a strong phase result.",
            "route_signal": "acceptance:governed-phase-route",
            "context_footprint": {
                "primary_context": "CTX-ORCHESTRATION",
                "navigation_order": ["CTX-ORCHESTRATION"],
            },
            "context_expansion_log": [
                {
                    "reason": "Check whether worker evidence referenced generated artifacts.",
                    "source": "artifacts",
                    "insufficiency": "The worker summary named evidence but not the artifact details.",
                    "stop_condition": "Artifact path confirms or refutes the evidence claim.",
                }
            ],
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
            "result_quality": {
                "requirements_alignment": {
                    "score": 91,
                    "summary": "The phase output matches the scoped requirement closely.",
                },
                "documentation_quality": {
                    "score": 88,
                    "summary": "Documentation quality is strong and operator-safe.",
                },
                "implementation_quality": {
                    "score": 86,
                    "summary": "Implementation quality is solid for the current boundary.",
                },
                "testing_quality": {
                    "score": 82,
                    "summary": "Testing quality is good with minor headroom left.",
                },
                "strengths": ["Strong requirements fit."],
                "gaps": ["Negative-path evidence could still be deeper."],
            },
        }
    )

    assert acceptance.result_quality is not None
    assert acceptance.result_quality.overall_score == 87
    assert acceptance.context_footprint is not None
    assert acceptance.context_footprint["primary_context"] == "CTX-ORCHESTRATION"
    assert acceptance.context_expansion_log is not None
    assert acceptance.context_expansion_log[0]["source"] == "artifacts"
    markdown = render_acceptance_markdown(acceptance)
    assert "## Result Quality" in markdown
    assert "Overall Score: 87 (strong)" in markdown
    assert "## Context Expansion" in markdown
    assert "source=artifacts" in markdown


def test_worker_payload_preserves_context_footprint_and_expansion_log() -> None:
    worker = normalize_worker_payload(
        {
            "status": "DONE",
            "summary": "Worker used routed context and one justified expansion.",
            "route_signal": "worker:phase-only",
            "files_touched": ["scripts/context_router.py"],
            "checks_run": ["python -m pytest tests/process/test_context_router.py -q"],
            "remaining_risks": [],
            "assumptions": [],
            "skips": [],
            "fallbacks": [],
            "deferred_work": [],
            "context_footprint": {
                "primary_context": "CTX-ORCHESTRATION",
                "navigation_order": ["CTX-ORCHESTRATION", "CTX-OPS"],
                "secondary_contexts": ["CTX-OPS"],
                "unmapped_files": [],
                "cold_context_files": [],
                "critical_contours": [],
            },
            "context_expansion_log": [
                {
                    "reason": "Confirm whether prompt JSON is preserved by runtime policy code.",
                    "source": "serena",
                    "insufficiency": "Prompt text alone cannot prove payload fields survive normalization.",
                    "stop_condition": "Relevant dataclass and normalizer are identified.",
                }
            ],
        }
    )

    assert worker.context_footprint is not None
    assert worker.context_footprint["primary_context"] == "CTX-ORCHESTRATION"
    assert worker.context_expansion_log is not None
    assert worker.context_expansion_log[0]["source"] == "serena"


def test_context_trace_metadata_is_soft_when_malformed() -> None:
    worker = normalize_worker_payload(
        {
            "status": "DONE",
            "summary": "Worker emitted an unstructured context trace.",
            "route_signal": "worker:phase-only",
            "files_touched": [],
            "checks_run": [],
            "remaining_risks": [],
            "assumptions": [],
            "skips": [],
            "fallbacks": [],
            "deferred_work": [],
            "context_footprint": "CTX-OPS",
            "context_expansion_log": "looked at memory",
        }
    )

    assert worker.context_footprint == {
        "raw": "CTX-OPS",
        "format_note": "`context_footprint` was not an object",
    }
    assert worker.context_expansion_log == [{"reason": "looked at memory", "source": "unstructured"}]


def test_route_report_surfaces_context_expansion_reasons() -> None:
    markdown = render_route_report(
        {
            "route_mode": "governed-phase-orchestration",
            "phase_name": "Phase 01",
            "backend": "simulate",
            "final_status": "accepted",
            "route_guardrails": [],
            "route_trace": [],
            "attempts": [
                {
                    "attempt": 1,
                    "kind": "worker",
                    "verdict": "PASS",
                    "worker_route_signal": "worker:phase-only",
                    "acceptor_route_signal": "acceptance:governed-phase-route",
                    "policy_blockers_total": 0,
                    "worker_context_expansion_log": [
                        {
                            "reason": "Need previous decision context.",
                            "source": "memory",
                            "insufficiency": "Primary card does not explain prior operator preference.",
                            "stop_condition": "Preference is found or absent.",
                        }
                    ],
                    "acceptor_context_expansion_log": [
                        {
                            "reason": "Verify executed evidence.",
                            "source": "artifacts",
                            "insufficiency": "Worker report lists a check but not its output.",
                            "stop_condition": "Artifact confirms the check result.",
                        }
                    ],
                }
            ],
            "next_phase": "done",
        }
    )

    assert "## Context Expansion" in markdown
    assert "attempt 1 worker: source=memory" in markdown
    assert "attempt 1 acceptor: source=artifacts" in markdown
