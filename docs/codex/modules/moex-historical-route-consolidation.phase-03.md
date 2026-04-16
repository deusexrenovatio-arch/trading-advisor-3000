# Module Phase Brief

Updated: 2026-04-11 13:10 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-historical-route-consolidation.parent.md
- Execution Contract: docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md

## Phase

- Name: Phase 03 - Dagster Cutover And Recovery Proof
- Status: completed

## Objective

- Materialize the canonical Dagster route and prove that nightly, repair, and backfill all run through the same owner path with single-writer protection and recovery behavior.

## In Scope

- canonical Dagster graph `moex_raw_ingest -> moex_canonical_refresh`
- schedule, retry, and lock hooks for the canonical route
- repair/backfill reuse of the same route
- recovery drill and two successful governed nightly cycles
- route adoption as the one operating path

## Out Of Scope

- Destructive cleanup of legacy/proof paths
- Reintroducing a second fallback route
- Separate reconciliation/trust-gate module
- Live intraday decisioning

## Change Surfaces

- CTX-ORCHESTRATION
- CTX-DATA
- GOV-RUNTIME
- GOV-DOCS

## Constraints

- Canonical cannot start after raw `BLOCKED` or `FAILED`.
- One active writer only for the authoritative route.
- Recovery must stay inside the same route.
- Two consecutive governed nightly cycles are required before cutover acceptance.

## Acceptance Gate

- Canonical Dagster graph exists as the production-like route graph, not only as proof scaffolding.
- Canonical never starts after raw failure.
- Single-writer discipline is proven by runtime behavior and negative tests.
- Repair and backfill run through the same Dagster route as nightly.
- Recovery drill passes without manual cleanup of authoritative Delta tables.
- Two governed nightly cycles in a row meet the `06:00 Europe/Moscow` readiness target.

## Disprover

- Dagster graph remains proof-only or legacy-only.
- Separate operating fallback route is still required.
- Recovery depends on manual cleanup of authoritative tables.
- Morning readiness misses the fixed target across the governed acceptance cycles.

## Done Evidence

- Canonical Dagster graph definition artifact
- Schedule/retry/lock contract artifact
- Recovery drill artifact
- Two governed nightly run reports meeting morning readiness

## Release Gate Impact

- Surface Transition: moex_historical_dagster_cutover_contour `planned -> cutover_ready`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: moex_historical_dagster_cutover_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: real governed nightly cycles and real route recovery evidence
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove final release readiness for the consolidated route.
- It does not prove legacy/proof cleanup completion.
- It does not prove long-term stabilization beyond the governed acceptance window.

## Rollback Note

- Revert Phase 03 claims if the route still depends on a non-canonical scheduler, proof graph, or manual fallback path.
