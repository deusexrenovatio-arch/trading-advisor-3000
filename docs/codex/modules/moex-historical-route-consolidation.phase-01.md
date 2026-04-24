# Module Phase Brief

Updated: 2026-04-11 13:10 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-historical-route-consolidation.parent.md
- Execution Contract: docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md

## Phase

- Name: Phase 01 - Contracts And Handoff Freeze
- Status: completed

## Objective

- Freeze the executable route contracts before any implementation cutover: the raw-to-canonical handoff, parity manifest, authoritative route ledger, CAS lease API, and the explicit supersession package.

## In Scope

- `raw_ingest_run_report.v2` schema and status semantics
- `parity_manifest.v1` schema and deterministic generation rules
- `technical_route_run_ledger` schema
- CAS lease API contract in the Delta runtime surface
- supersession matrix across execution contract, parent brief, route decision, status, runbooks, and legacy scripts
- morning-update and consumer-impact contract

## Out Of Scope

- Raw/canonical route code changes
- Dagster graph implementation
- Cleanup of legacy/proof paths
- Live intraday or broker-execution contours

## Change Surfaces

- CTX-CONTRACTS
- CTX-ORCHESTRATION
- GOV-DOCS
- ARCH-DOCS

## Constraints

- Status semantics must remain fail-closed.
- `PASS-NOOP` must be unambiguous and machine-validated.
- The lease API must be defined against one canonical backend only.
- This phase may not claim any route cutover or runtime readiness.

## Acceptance Gate

- `raw_ingest_run_report.v2` exists as a versioned machine-validatable contract with required fields and status rules.
- `technical_route_run_ledger` exists as a versioned machine-validatable contract with single-writer state semantics.
- `parity_manifest.v1` exists as a versioned machine-validatable contract with deterministic window and hash rules.
- CAS operations are frozen as an executable API contract with negative concurrency tests.
- The supersession package names exactly one active MOEX planning truth after this phase.

## Disprover

- Missing `changed_windows` schema or ambiguous `PASS-NOOP` semantics.
- Missing ledger/lease fields or missing conflict-as-`BLOCKED` rule.
- Missing deterministic parity manifest rules.
- Missing explicit supersession matrix from old module to new module.

## Done Evidence

- Schema docs or manifests for all three contract families.
- Contract fixtures and negative test inventory for the CAS lease API.
- Updated planning truth docs aligned to one active module pointer.

## Release Gate Impact

- Surface Transition: moex_historical_handoff_contract_contour `planned -> executable_contracts`
- Minimum Proof Class: schema
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: moex_historical_handoff_contract_contour
- Delivered Proof Class: schema
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove final release readiness for the consolidated route.
- It does not prove parity on real data windows.
- It does not prove Dagster cutover or morning readiness.

## Rollback Note

- Revert Phase 01 claims if any contract remains prose-only or if more than one active planning truth remains in repo docs.
