# Task Note
Updated: 2026-03-26 14:45 UTC

## Goal
- Deliver: reopen governed module phase 02 (G1 - Machine-Verifiable Stack-Conformance Gate) and harden the stack-conformance guard so checklist and acceptance-document overclaims fail closed before later phases are reconsidered.

## Task Request Contract
- Objective: extend phase-02 machine guard coverage to the real acceptance surfaces that can reintroduce false closure language.
- In Scope: `registry/stack_conformance.yaml`, `scripts/validate_stack_conformance.py`, targeted tests for closure-guard surface coverage, and any phase-02 docs/check wiring needed to keep that validator on the governed route.
- Out of Scope: Delta, Spark, Dagster, runtime, Telegram, sidecar, or release-proof implementation work; re-accepting later phases before G1 is corrected.
- Constraints: keep the phase governance-only; fail closed on checklist/acceptance-doc overclaims; do not weaken existing runtime-proof checks; keep the repo on canonical gate names and governed route only.
- Done Evidence: validator fails on a forbidden closure phrase inserted into a checklist or acceptance doc while non-implemented surfaces remain; targeted tests cover that regression; phase-scoped loop/pr gate evidence is green.
- Priority Rule: quality and fail-closed governance over speed.

## Current Delta
- Phase order is intentionally rolled back to G1 because review findings showed the current guard still ignores the very checklist and acceptance surfaces that phase-01 said must fail after G1 lands.
- Later product/runtime phases are reopened as pending so their acceptance can be re-earned only after the strengthened guard is in place.
- Active handoff is switched back to this phase-02 note before any rerun of governed continuation.

## First-Time-Right Report
1. Confirmed coverage: the strongest current gap is not runtime proof but unchecked acceptance surfaces in `docs/checklists/app/*` and related app acceptance docs.
2. Missing or risky scenarios: a shallow fix that only appends a few explicit file paths would still leave future acceptance docs outside the machine guard.
3. Resource/time risks and chosen controls: extend the validator contract itself to support broader document coverage and prove it with targeted fail-path tests before rerunning later phases.
4. Highest-priority fixes or follow-ups: revalidate G1 first, then replay D1/D2/D3 on the reopened governed path instead of trusting earlier acceptance.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: if the same overclaim regression still escapes the validator after two focused edits, stop phase replay and tighten the guard contract again before any more phase progression.
- Reset Action: capture the exact escaping document/test case, widen the registry/validator contract, and rerun only the phase-scoped checks.
- New Search Space: closure-guard document selection, glob support, checklist/acceptance-doc coverage, and validator fail-path testing.
- Next Probe: make a checklist/acceptance-doc overclaim fail in a targeted test, then rerun the phase-scoped validator and gates.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: CTX-CONTRACTS, GOV-RUNTIME, GOV-DOCS
- Route Match: matched
- Primary Rework Cause: governance_gap
- Incident Signature: g1-acceptance-surface-undercoverage
- Improvement Action: strengthen_closure_guard
- Improvement Artifact: phase-02 validator/registry/test coverage over checklist and acceptance-doc surfaces.

## Blockers
- No blocker.

## Next Step
- Implement the phase-02 guard expansion and rerun governed continuation for this reopened phase only.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_stack_conformance.py`
- `python -m pytest tests/process/test_validate_stack_conformance.py -q`
- `python scripts/run_loop_gate.py --changed-files registry/stack_conformance.yaml scripts/validate_stack_conformance.py tests/process/test_validate_stack_conformance.py docs/session_handoff.md docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-02-for-stack-conf.md`
- `python scripts/run_pr_gate.py --skip-session-check --changed-files registry/stack_conformance.yaml scripts/validate_stack_conformance.py tests/process/test_validate_stack_conformance.py docs/session_handoff.md docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-02-for-stack-conf.md`
