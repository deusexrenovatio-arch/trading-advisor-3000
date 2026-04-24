# Task Note
Updated: 2026-04-14 05:40 UTC

## Goal
- Deliver: Continue governed module for MOEX historical-route consolidation Phase 03 Dagster cutover and recovery proof

## Task Request Contract
- Objective: materialize the canonical Dagster route for MOEX historical refresh and prove that nightly, repair, and backfill all execute through the same owner path with single-writer protection and recovery behavior.
- In Scope: canonical Dagster graph `moex_raw_ingest -> moex_canonical_refresh`, schedule/retry/lock hooks, repair/backfill reuse of the same route, recovery drill evidence, two governed nightly cycles, phase-03 docs closure, the active task note, and governed continuation artifacts for `docs/codex/modules/moex-historical-route-consolidation.phase-03.md`.
- Out of Scope: destructive cleanup of legacy/proof paths, reintroducing a fallback route, separate reconciliation/trust-gate scope, live intraday decisioning, and any Phase 04 cleanup claim.
- Constraints: canonical cannot start after raw `BLOCKED` or `FAILED`, one active writer only for the authoritative route, recovery must stay inside the same route, and two consecutive governed nightly cycles must meet the fixed `06:00 Europe/Moscow` readiness target.
- Done Evidence: canonical Dagster graph artifact exists as the production-like route graph, schedule/retry/lock contract artifact exists, recovery drill passes without manual cleanup of authoritative Delta tables, two governed nightly run reports meet morning readiness, and governed acceptance unlocks Phase 04 without policy blockers.
- Priority Rule: close Dagster-only route ownership and recovery proof before any cleanup work.

## Current Delta
- Phase 02 is accepted and the module now points to Phase 03 as the next allowed unit of work.
- Route truth is already fixed to `Dagster -> Python raw ingest -> Spark canonical refresh`, so this phase must turn Dagster from planned owner into the real production-like owner path.
- Earlier `phase2a` Dagster and Spark code remains reusable implementation context, but proof-only contours are not sufficient for this phase unless they are refactored onto the authoritative MOEX raw/canonical route.
- First Phase 03 attempt is `BLOCKED`: acceptance rejected local materialization proof, non-Dagster-backed nightly evidence, and stale phase docs.
- Current remediation scope upgrades the route to instance-backed Dagster job runs with actual schedule/retry bindings and closes the missing GOV-DOCS truth updates.
- The remaining gap is no longer local route shape; it is the missing operational bridge from real staging Dagster runs to the `staging-binding-report.json` format already accepted by the canonical Phase-03 runner.
- This task now includes an explicit staging evidence collector and runbook so the next governed rerun can attach real Dagster bindings instead of another local-only proof replay.
- The collector path now has a live local Dagster GraphQL roundtrip smoke through the canonical runner; this reduces bridge risk but does not count as staging-real evidence.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact
- Closure Evidence: integration test evidence, canonical dataset ownership through the Dagster cutover route, downstream research handoff preservation, and runtime-ready surface proof through governed nightly and recovery execution
- Shortcut Waiver: none

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: proof-only Dagster scaffolding may look close while still missing ownership, recovery, or morning-readiness closure on the authoritative route.
3. Resource/time risks and chosen controls: keep work phase-bounded, reuse only route-compatible Dagster/Spark patterns, and require executable governed nightly evidence rather than narrative cutover claims.
4. Highest-priority fixes or follow-ups: identify the exact route-ownership gap, close it in code/runtime, and rerun governed acceptance on the same module.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: blocked
- Decision Quality: constrained
- Final Contexts: Phase 03 Dagster cutover contour, canonical route ownership, governed nightly and recovery evidence
- Route Match: governed continuation
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: tighten real-staging evidence handoff and keep local smoke paths fail-closed
- Improvement Artifact: local route report captured outside tracked docs; the durable blocker remains the real staging binding report described below.

## Blockers
- Governed Phase 03 acceptance is now blocked on missing real staging evidence for the canonical Dagster owner path.
- Local docs truth and local route behavior are no longer the main blocker.
- The live blocker is external staging evidence: two governed nightly cycles, repair/backfill reuse, and recovery artifact must be packaged into a real staging binding report.

## Next Step
- Use the new staging evidence collector to package real Dagster run ids plus phase-scoped artifacts into `staging-binding-report.json`, rerun the canonical Phase-03 contour with that binding report, and then rerun governed continuation on the same module.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
