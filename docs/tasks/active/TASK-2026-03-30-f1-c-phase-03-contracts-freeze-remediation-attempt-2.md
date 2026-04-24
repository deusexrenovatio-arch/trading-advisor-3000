# Task Note
Updated: 2026-03-30 13:07 UTC

## Goal
- Deliver: F1-C phase-03 remediation attempt 2 that closes blockers `B1`, `B2`, and `P-EVIDENCE_GAP-1` without opening next-phase scope.

## Task Request Contract
- Objective: restore governed phase-route integrity and close the remaining release-blocking contract evidence gaps for contracts freeze.
- In Scope: `docs/session_handoff.md`; this F1-C task note; `docs/tasks/active/index.yaml`; `docs/architecture/product-plane/CONTRACT_SURFACES.md`; `docs/architecture/product-plane/STATUS.md`; `src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml`; phase-03 contract schemas/fixtures/tests for sidecar metrics and runtime API exclusion decision evidence.
- Out of Scope: F1-D/F1-E/F1-F implementation, real broker connector closure, release-readiness verdict change, and any trading/business logic.
- Constraints: stay inside phase-03; no silent assumptions/fallbacks/skips/deferrals; keep pointer-shim handoff contract; rerun canonical loop gate and record evidence.
- Done Evidence: `python scripts/validate_task_request_contract.py`; `python scripts/validate_session_handoff.py`; `python scripts/run_loop_gate.py --from-git --git-ref HEAD`; `python -m pytest tests/product-plane/contracts/test_release_blocking_contracts.py -q`; `python -m pytest tests/product-plane/contracts -q`; `python scripts/validate_stack_conformance.py`; `python scripts/validate_architecture_policy.py`; `python scripts/validate_docs_links.py --roots AGENTS.md docs`.
- Priority Rule: quality and governance integrity over speed.

## Current Delta
- Acceptance attempt 01 blocked route integrity because the active handoff pointer still referenced the F1-B phase-02 task note.
- The sidecar `GET /metrics` public envelope is present in truth-source docs but missing from versioned schema/fixture/test inventory.
- Runtime API exclusions for `/runtime/signal-events` and `/runtime/strategy-registry` were not documented as an explicit truth-source decision.

## First-Time-Right Report
1. Confirmed coverage: this remediation closes only the three declared blockers and keeps all edits inside F1-C contract-freeze surfaces.
2. Missing or risky scenarios: adding metrics coverage without documenting exclusion decisions could still leave policy blockers even when tests pass.
3. Resource/time risks and chosen controls: apply one focused patch set (`route -> contracts/tests -> truth-source docs`) and run only the rerun checks requested by acceptance.
4. Highest-priority fixes or follow-ups: route pointer correctness and canonical loop-gate evidence first, then missing sidecar/runtime API evidence closure.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same acceptance blocker persists after this focused remediation.
- Reset Action: stop edits, capture failing evidence, and request explicit contract clarification before more code/doc changes.
- New Search Space: governed route pointer state, release-blocking contract inventory completeness, and truth-source exclusion decisions.
- Next Probe: run the canonical rerun-check set from acceptance after patching.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: remediation:phase-only
- Primary Rework Cause: close blockers from acceptance attempt-01 for F1-C phase-03
- Incident Signature: none
- Improvement Action: make blocker-to-artifact traceability explicit in task note and truth-source inventory.
- Improvement Artifact: `src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml`

## Blockers
- No blocker.

## Next Step
- Apply blocker-only remediation patch and execute the acceptance rerun checks in canonical order.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/contracts/test_release_blocking_contracts.py -q`
- `python -m pytest tests/product-plane/contracts -q`
- `python scripts/validate_stack_conformance.py`
- `python scripts/validate_architecture_policy.py`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
