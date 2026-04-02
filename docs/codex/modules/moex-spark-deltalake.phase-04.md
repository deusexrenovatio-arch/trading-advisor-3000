# Module Phase Brief

Updated: 2026-04-02 08:20 UTC

## Parent

- Parent Brief: docs/codex/modules/moex-spark-deltalake.parent.md
- Execution Contract: docs/codex/contracts/moex-spark-deltalake.execution-contract.md

## Phase

- Name: Этап 4 - Production hardening
- Status: completed

## Objective

- Complete operational hardening (retry/recovery/performance/observability) and issue final production contour decision only after all acceptance gates are closed with live-real evidence.

## In Scope

- Scheduler configuration for bootstrap/increment/reconciliation/QC jobs with observable run states.
- Recovery/replay drill proving restart without manual data cleanup.
- Monitoring and alerting for latency, failures, QC breaches, reconciliation drift, and freshness.
- Final release checklist closure: gates A-D PASS, no unresolved P1/P2 integrity defects.

## Out Of Scope

- Rewriting source roles (Finam live contour remains live decision source).
- Broker execution closure or MOEX trading adapter implementation.
- New contract families outside this package scope.

## Change Surfaces

- CTX-ORCHESTRATION
- GOV-RUNTIME
- GOV-DOCS
- CTX-DATA

## Constraints

- Final decision must be based on live-real operational proof, not dry-run narratives.
- Publish pointers must remain rollback-safe to last healthy snapshot.
- Recovery proof must include fail/replay/recover sequence with traceable artifacts.

## Acceptance Gate

- Gates A-C consume governed acceptance closure artifacts for phases 01-03 (`acceptance.json` with `verdict=PASS` and route `acceptance:governed-phase-route`), not worker-only status fields.
- Scheduled runs are configured and observable for all required jobs.
- Recovery after transient failure is proven by replay run without manual cleanup.
- Monitoring metrics and dashboards are emitted and documented for operator use.
- On-call runbook supports deterministic incident triage and restart.
- All acceptance gates A-D are PASS with no unresolved P1/P2 data-integrity defects.

## Disprover

- Simulate transient source/API failure; if retry/recovery cannot restore a healthy cycle, the phase fails.
- Execute recovery drill that requires manual cleanup or produces non-deterministic state; acceptance must fail.
- Remove a critical observability signal (freshness or QC fail counter) and confirm final decision is blocked.

## Done Evidence

- Scheduler config snapshot and tick/run status evidence.
- Recovery drill artifacts with replay timeline and restored healthy publish pointer.
- Monitoring dashboard/query references covering run health, drift, QC, and freshness.
- Completed operations runbook checklist and final release decision bundle.

## Release Gate Impact

- Surface Transition: operations_recovery_contour `planned -> release_decision`
- Minimum Proof Class: live-real
- Accepted State Label: release_decision

## Release Surface Ownership

- Owned Surfaces: operations_recovery_contour
- Delivered Proof Class: live-real
- Required Real Bindings: real production scheduler and job runs, real monitored pipeline metrics/alerts, and replayable recovery evidence
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Broker execution closure: this phase does not prove external broker submit/replace/cancel/fills contour readiness.
- Runtime source swap: this phase does not prove replacing Finam live runtime contour with MOEX for in-the-moment decisions.

## Rollback Note

- Revert release decision claims immediately if any accepted proof depends on mock-only, smoke-only, or non-replayable artifacts.
