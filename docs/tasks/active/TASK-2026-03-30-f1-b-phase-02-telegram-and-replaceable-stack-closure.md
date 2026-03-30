# Task Note
Updated: 2026-03-30 11:43 UTC

## Goal
- Deliver: F1-B phase-02 closure patch for Telegram and replaceable-stack terminal consistency without opening next phase scope.

## Task Request Contract
- Objective: make the in-scope replaceable technologies terminal and non-ambiguous across route state, ADR set, active architecture docs, registry enforcement, and tests.
- In Scope: `docs/session_handoff.md`; one active F1-B task note; `docs/architecture/app/product-plane-spec-v2/01_Architecture_Overview.md`; `docs/architecture/app/product-plane-spec-v2/04_ADRs.md`; `docs/architecture/app/product-plane-spec-v2/08_Codex_AI_Shell_Integration.md`; `registry/stack_conformance.yaml`; `scripts/validate_stack_conformance.py`; `tests/process/test_validate_stack_conformance.py`.
- Out of Scope: phase-03 contracts-freeze expansion, broker-process closure, release-readiness verdict change, and any trading/business logic.
- Constraints: keep patch phase-scoped; no silent assumptions/fallbacks/skips/deferrals; preserve pointer-shim handoff contract; keep docs and validator fail-closed.
- Done Evidence: run phase checks for task contract/handoff, stack conformance, targeted runtime/research/observability tests, and canonical loop/pr gates.
- Priority Rule: quality and governance integrity over speed.

## Current Delta
- Previous acceptance run for this phase was blocked by route pointer drift and ADR/overview contradictions; this patch closes those exact gaps.

## First-Time-Right Report
1. Confirmed coverage: phase objective is mapped to route integrity + docs alignment + validator/test enforcement.
2. Missing or risky scenarios: removed technologies can silently reappear in active overview/ADR wording without failing existing checks.
3. Resource/time risks and chosen controls: keep changes limited to F1-B files and add one focused fail-closed guard with regression tests.
4. Highest-priority fixes or follow-ups: route pointer switch first, then ADR/overview correction, then validator/test reinforcement.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: if the same phase check fails twice after targeted fixes.
- Reset Action: stop edits, capture exact failing evidence, and re-scope to blocker-only remediation.
- New Search Space: stack registry guard logic and active architecture/ADR truth-source alignment.
- Next Probe: run phase-targeted validators and tests before broad closeout gates.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: worker:phase-only
- Primary Rework Cause: prior acceptance blockers B1 B2 P-EVIDENCE_GAP-1
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Apply the phase-scoped patch and execute the phase-targeted check set.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_stack_conformance.py`
- `python -m pytest tests/process/test_validate_stack_conformance.py -q`
- `python -m pytest tests/app/integration/test_phase2a_data_plane.py -q`
- `python -m pytest tests/app/unit/test_phase2c_runtime_components.py -q`
- `python -m pytest tests/app/integration/test_phase2c_runtime_postgres_store.py -q`
- `python -m pytest tests/app/integration/test_phase2b_research_plane.py -q`
- `python -m pytest tests/app/integration/test_phase5_review_observability.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
