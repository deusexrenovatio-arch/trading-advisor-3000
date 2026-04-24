# Task Note
Updated: 2026-04-13 12:52 UTC

## Goal
- Deliver: Continue governed module for MOEX historical-route consolidation Phase 02 implementation and parity proof

## Task Request Contract
- Objective: implement the canonical raw and canonical route surfaces for Phase 02 and prove parity-safe scoped recompute, idempotency, and machine-safe `PASS-NOOP` behavior on governed proof windows.
- In Scope: phase-02 product-plane route code and schemas under `src/trading_advisor_3000/product_plane/data_plane/moex/**`, phase-02 fixtures/tests under `tests/product-plane/**`, phase-scoped contract/report surfaces needed for parity evidence, the active task note, and governed continuation artifacts for `docs/codex/modules/moex-historical-route-consolidation.phase-02.md`.
- Out of Scope: Dagster cutover as the only operating route, final legacy/proof cleanup, a separate reconciliation module, live intraday or broker-execution contours, and any Phase 03/04 claim.
- Constraints: raw writes only raw history, canonical writes only canonical bars and provenance, recompute remains scoped to `changed_windows`, publish stays fail-closed on QC/compatibility failure, and no silent fallback or deferred critical work may remain in the governed evidence package.
- Done Evidence: governed continuation for Phase 02 completes, parity/idempotency/no-op evidence is materialized, phase-relevant pytest/validator commands pass, and acceptance unlocks the route without policy blockers.
- Priority Rule: deliver a reviewable phase-bounded parity contour before touching Dagster-only ownership or cleanup scope.

## Current Delta
- Phase 01 is accepted and the module now points to Phase 02 as the next allowed unit of work.
- The route decision is fixed to `Dagster -> Python raw ingest -> Spark canonical refresh`, but this phase closes parity-safe route behavior rather than Dagster-only ownership.
- Existing repository state already contains earlier MOEX Phase 01/02 implementation context, so this continuation must separate reusable baseline code from the specific governed Phase 02 acceptance requirements.
- Worker continuation landed phase-02 code changes for changed-window scoped canonical recompute, parity artifacts, and `PASS-NOOP` no-mutation behavior.
- A staging-real canonical rerun on the accepted source raw table completed at `artifacts/codex/moex-phase02-governed/20260413T130500Z` using a derived-but-real `raw_ingest_run_report.v2` built from the accepted `20260409T162421Z` phase-01 shard progress evidence.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact
- Closure Evidence: real governed raw/canonical proof windows, deterministic parity artifacts, machine-safe `PASS-NOOP`, idempotent reruns, and fail-closed QC/compatibility behavior
- Shortcut Waiver: none

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: legacy phase-02 proof code may look implemented while still missing governed parity-proof closure on the exact Phase 02 acceptance gate.
3. Resource/time risks and chosen controls: keep work phase-bounded, reuse existing implementation only where it matches the current route contract, and verify through targeted parity/idempotency/no-op tests.
4. Highest-priority fixes or follow-ups: establish the exact phase gap, close it with product-plane changes, and rerun governed acceptance on the same module.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: Phase 02 parity contour, canonical raw/canonical route surfaces, governed continuation evidence
- Route Match: governed continuation
- Primary Rework Cause: Phase 02 is unlocked but not yet accepted under the current one-route consolidation module
- Incident Signature: moex-phase02-parity-contour-closure
- Improvement Action: close parity/idempotency/no-op proof on the canonical raw and canonical surfaces without widening into Dagster cutover
- Improvement Artifact: docs/codex/modules/moex-historical-route-consolidation.phase-02.md

## Blockers
- Governed Phase 02 acceptance is still pending because the first worker attempt stopped after code/integration proof and did not close staging-real evidence plus docs remediation inside the route state.

## Next Step
- Re-run governed continuation for Phase 02 with staging-real proof artifacts and updated docs closure in scope.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
