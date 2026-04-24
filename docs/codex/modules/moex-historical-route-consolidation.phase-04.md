# Module Phase Brief

Updated: 2026-04-11 13:10 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-historical-route-consolidation.parent.md
- Execution Contract: docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md

## Phase

- Name: Phase 04 - Legacy And Proof Cleanup
- Status: planned

## Objective

- Remove competing legacy and proof routes after stabilization and leave exactly one operator-facing MOEX historical route in repo truth, docs, and executable surfaces.

## In Scope

- retirement or deletion of legacy manual scripts
- retirement or deletion of proof-only route entrypoints
- docs, runbooks, tests, and gate mappings cleanup
- zero dangling references to `phase2a` in active route surfaces
- stabilization-window evidence

## Out Of Scope

- Reopening Phase 01-03 contract or cutover decisions
- New consumer scopes or trust-gate work
- Live intraday or broker-execution contours

## Change Surfaces

- GOV-DOCS
- ARCH-DOCS
- CTX-ORCHESTRATION
- CTX-DATA

## Constraints

- Cleanup is allowed only after the stabilization window.
- No retired path may remain advertised as a working route.
- The repo must end with one operator-facing route only.

## Acceptance Gate

- Stabilization window is satisfied: 10 successful nightly cycles in a row or 14 calendar days.
- Legacy/proof scripts and docs no longer advertise working-route status.
- `phase2a` dangling references are reduced to zero in active tests, gates, imports, docs, and runbooks.
- The repo truth documents name exactly one operator-facing MOEX historical route.

## Disprover

- Any active doc, test, gate, or import still treats a retired path as active.
- Cleanup happens before stabilization.
- More than one operator-facing route remains after the phase.

## Done Evidence

- Cleanup diff across docs, tests, scripts, and route decision surfaces
- Zero-dangling-reference proof for `phase2a`
- Stabilization-window evidence pack

## Release Gate Impact

- Surface Transition: moex_historical_cleanup_contour `planned -> one_route_only`
- Minimum Proof Class: live-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: moex_historical_cleanup_contour
- Delivered Proof Class: live-real
- Required Real Bindings: stabilization-window evidence and one-route-only repo truth
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove final release readiness for the consolidated route.
- It does not prove a separate trust-gate or reconciliation module.
- It does not prove live intraday readiness.

## Rollback Note

- Revert Phase 04 claims if any retired route remains active in docs or execution surfaces, or if stabilization evidence is incomplete.
