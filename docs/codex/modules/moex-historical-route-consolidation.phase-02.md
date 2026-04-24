# Module Phase Brief

Updated: 2026-04-11 13:10 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-historical-route-consolidation.parent.md
- Execution Contract: docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md

## Phase

- Name: Phase 02 - Route Implementation And Parity Proof
- Status: completed

## Objective

- Implement the canonical raw and canonical route surfaces and prove deterministic scoped recompute, idempotency, and `PASS-NOOP` behavior on governed parity windows.

## In Scope

- canonical `moex_raw_ingest` route surface
- canonical `moex_canonical_refresh` route surface
- raw idempotency and overlap behavior
- canonical QC/compatibility and no-op behavior
- parity manifests and proof artifacts across the active universe

## Out Of Scope

- Dagster cutover as the only operating route
- legacy/proof cleanup
- separate reconciliation module
- live intraday or broker-execution contours

## Change Surfaces

- CTX-DATA
- CTX-CONTRACTS
- CTX-ORCHESTRATION
- GOV-DOCS

## Constraints

- Raw writes only raw history.
- Canonical writes only canonical bars and provenance.
- Recompute stays scoped to `changed_windows`.
- Publish remains fail-closed on QC/compatibility failure.

## Acceptance Gate

- Raw reruns on the same governed window remain idempotent.
- Overlap refresh corrects near-watermark updates without duplicate rows.
- Canonical recomputes only the governed `changed_windows` scope.
- Empty `changed_windows` leads to `PASS-NOOP` without canonical-table mutation.
- Parity evidence covers the active universe, required source/runtime timeframes, and fixed proof windows with zero duplicate, missing-bar, timestamp-drift, or aggregation mismatch.

## Disprover

- Duplicate rows on rerun.
- Canonical mutation during `PASS-NOOP`.
- Publish allowed after QC/compatibility failure.
- Parity scope missing any required dimension or artifact.

## Done Evidence

- Raw parity report
- Canonical parity report
- Governed `changed_window_set` manifest
- QC and compatibility reports

## Release Gate Impact

- Surface Transition: moex_historical_parity_contour `planned -> implemented`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: moex_historical_parity_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: real governed raw/canonical proof windows on the active universe
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove final release readiness for the consolidated route.
- It does not prove Dagster-only operating ownership.
- It does not prove final cleanup or stabilization.

## Rollback Note

- Revert Phase 02 claims if parity depends on fixture-only evidence or if `PASS-NOOP` behavior is not machine-safe.
