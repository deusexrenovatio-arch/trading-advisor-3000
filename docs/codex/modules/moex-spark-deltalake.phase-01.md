# Module Phase Brief

Updated: 2026-04-02 08:19 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-spark-deltalake.parent.md
- Execution Contract: docs/codex/contracts/moex-spark-deltalake.execution-contract.md

## Phase

- Name: Этап 1 - Foundation
- Status: completed

## Objective

- Establish universe/mapping foundation, MOEX coverage discovery, and deterministic raw ingest/backfill baseline for the initial futures scope.

## In Scope

- Initial MOEX futures universe policy and versioned config template.
- Instrument mapping registry (`internal_id`, `finam_symbol`, `moex_engine/market/board/secid`) with uniqueness controls.
- `moex_candleborders_discovery` and bootstrap ingest contour with idempotent rerun behavior.
- Coverage artifacts and technical runbook baseline required for the next phase.

## Out Of Scope

- Canonical/resampling publish closure.
- Cross-source reconciliation and threshold alerts.
- Final operations hardening and release decision.
- Broker execution contour changes.

## Change Surfaces

- CTX-DATA
- CTX-CONTRACTS
- GOV-DOCS

## Constraints

- Universe scope stays limited to MOEX futures priority groups from the package.
- Mapping updates are versioned and soft-deactivated; no hard-delete of historical mappings.
- Ingest must be deterministic and idempotent before phase closure.

## Acceptance Gate

- Instrument mapping model exists (`internal_id`, `finam_symbol`, `moex_engine/market/board/secid`) with one active mapping per source key.
- MOEX discovery of available history ranges is implemented and persisted for all initial assets/intervals.
- Raw ingest path writes deterministic Delta output and verifies idempotent rerun behavior.
- Unit and integration tests for parsing, mapping, and incremental watermarking pass.

## Disprover

- Break mapping uniqueness for one active instrument and confirm the phase gate fails.
- Re-run the same ingest window twice; if duplicate bars appear, the phase fails.
- Remove coverage evidence for any active asset/interval and confirm acceptance cannot pass.

## Done Evidence

- Coverage table/report artifact for the full initial universe.
- Raw ingest/backfill run report with deterministic rerun proof.
- Passing parser/mapping/watermark test outputs.
- Updated technical runbook section for foundation operations.

## Release Gate Impact

- Surface Transition: moex_history_ingest_contour `planned -> implemented`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: moex_history_ingest_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: real MOEX ISS source windows for active universe, persisted Delta raw outputs, and replayable ingest/coverage artifacts
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove final release decision for the full package contour.
- Canonical/resampling closure: this phase does not prove contract-safe canonical outputs or QC publish readiness.

## Rollback Note

- Revert foundation closure claims if ingest evidence depends on synthetic fixtures only or mapping/version controls are not replayable.
