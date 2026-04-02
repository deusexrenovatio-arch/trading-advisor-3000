# Module Phase Brief

Updated: 2026-04-02 08:19 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-spark-deltalake.parent.md
- Execution Contract: docs/codex/contracts/moex-spark-deltalake.execution-contract.md

## Phase

- Name: Этап 2 - Canonical
- Status: completed

## Objective

- Build contract-safe canonical bars and deterministic resampling outputs with fail-closed QC enforcement for runtime-compatible timeframes.

## In Scope

- Canonical builder aligned with `canonical_bar.v1` without in-place contract drift.
- Deterministic resampling rules (`open=first`, `high=max`, `low=min`, `close=last`, `volume=sum`) for `5m`, `15m`, `1h`.
- Mandatory QC gates: uniqueness, monotonic timeline, OHLC validity, provenance completeness.
- Runtime decoupling check so Spark remains out of runtime hot-path.

## Out Of Scope

- Finam vs MOEX reconciliation contour closure.
- Operations hardening and final release decision.
- Expanding runtime timeframe enum without separate versioned contract change.
- Broker execution contour work.

## Change Surfaces

- CTX-DATA
- CTX-CONTRACTS
- CTX-ORCHESTRATION

## Constraints

- `canonical_bar.v1` fields and runtime timeframe policy (`5m`, `15m`, `1h`) remain unchanged in place.
- QC publish behavior is fail-closed; critical gate failures block publish.
- Provenance remains queryable via technical tables, not by mutating canonical payload contract.

## Acceptance Gate

- Canonical bars remain compliant with `canonical_bar.v1` fields/types and timeframe policy.
- Resampling output is deterministic and passes aggregation correctness checks on fixtures/integration windows.
- QC gates block invalid OHLCV, duplicates, and non-monotonic timelines.
- Runtime hot-path remains decoupled from Spark execution.

## Disprover

- Add unapproved field drift to `canonical_bar.v1`; contract compatibility checks must fail the phase.
- Inject a deterministic aggregation mismatch in resampling; golden/identity tests must fail.
- Force critical QC violation and verify publish is blocked instead of silently downgraded.

## Done Evidence

- Canonical and resampling snapshot artifacts with stable keys/timeframes.
- QC report showing PASS for mandatory gates on integration windows.
- Contract compatibility report against current schema manifest.
- Runtime decoupling proof for hot-path reads.

## Release Gate Impact

- Surface Transition: canonical_resampling_qc_contour `planned -> implemented`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: canonical_resampling_qc_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: real canonical and resampled outputs from governed runs, fail-closed QC artifacts, and contract compatibility evidence
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove final production contour allow decision.
- Reconciliation closure: this phase does not prove cross-source drift governance between Finam and MOEX.

## Rollback Note

- Revert canonical/resampling closure claims if proof relies on fixture-only results or if critical QC gates can be bypassed.
