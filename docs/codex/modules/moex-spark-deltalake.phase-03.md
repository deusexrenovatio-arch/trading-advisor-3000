# Module Phase Brief

Updated: 2026-04-02 15:20 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-spark-deltalake.parent.md
- Execution Contract: docs/codex/contracts/moex-spark-deltalake.execution-contract.md

## Phase

- Name: Этап 3 - Reconciliation
- Status: completed

## Objective

- Close cross-source reconciliation contour by adding Finam live archive ingest, automated overlap comparison, and threshold-driven alerting with operational reports.

## In Scope

- Asynchronous Finam snapshot archive ingest for analytics/reconciliation contour.
- Automated overlap comparison (close drift, volume drift, missing bars, lag class).
- Versioned threshold policy and escalation behavior.
- Reconciliation report artifacts and alert simulation outputs.

## Out Of Scope

- Final release decision and production hardening closure.
- Replacing Finam live runtime contour with MOEX.
- Canonical contract redesign beyond approved boundaries.
- Broker execution contour closure.

## Change Surfaces

- CTX-DATA
- CTX-RESEARCH
- CTX-ORCHESTRATION
- GOV-DOCS

## Constraints

- Reconciliation must run on real overlap windows, not synthetic-only comparison.
- Threshold changes are versioned and change-controlled.
- No silent downgrade of hard threshold violations to warning-only behavior.

## Acceptance Gate

- Cross-source comparison between Finam archive and MOEX overlap windows is automated.
- Drift metrics for close/volume/missing rate/latency class are persisted and queryable.
- Alert thresholds and escalation paths are defined, executable, and tested.
- Reconciliation report artifacts are produced for governed runs.

## Disprover

- Disable one compared dimension (for example lag class) and confirm phase acceptance fails.
- Trigger hard-threshold drift without incident/degraded handling; acceptance must fail.
- Remove alert simulation evidence and confirm the phase cannot be accepted.

## Done Evidence

- Reconciliation report artifact with gate-level outcomes and offending samples.
- Alert simulation output and escalation trace.
- Finam archive ingest report with source latency/timestamp metadata.
- Updated runbook section references for reconciliation incidents.

## Release Gate Impact

- Surface Transition: finam_moex_reconciliation_contour `planned -> implemented`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: finam_moex_reconciliation_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: real Finam archive contour, real MOEX overlap windows, persisted drift metrics, and executable alert/escalation artifacts
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove final allow decision for production contour.
- Operations hardening: this phase does not prove scheduler/recovery readiness under sustained incidents.

## Rollback Note

- Revert reconciliation closure claims if persisted overlap metrics or alert escalations are missing or not reproducible.
