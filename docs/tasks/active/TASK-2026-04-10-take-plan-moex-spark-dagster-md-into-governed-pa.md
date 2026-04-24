# Task Note
Updated: 2026-04-10 13:31 UTC

## Goal
- Deliver: Take PLAN_MOEX_spark_dagster.md into governed package intake and materialize planning artifacts for the new MOEX historical route consolidation module

## Task Request Contract
- Objective: materialize the new MOEX historical-route consolidation plan into first-class governed docs inside the repo and align active truth surfaces to that module.
- In Scope: `docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md`, `docs/codex/modules/moex-historical-route-consolidation.parent.md`, `docs/codex/modules/moex-historical-route-consolidation.phase-01.md`, `docs/codex/modules/moex-historical-route-consolidation.phase-02.md`, `docs/codex/modules/moex-historical-route-consolidation.phase-03.md`, `docs/codex/modules/moex-historical-route-consolidation.phase-04.md`, `docs/architecture/product-plane/STATUS.md`, `docs/architecture/product-plane/moex-historical-route-decision.md`, historical supersession notes in old MOEX governed docs, and this active task note.
- Out of Scope: Phase 01 code implementation, contract schema materialization in runtime code, Dagster graph implementation, and any live intraday or broker-execution scope.
- Constraints: preserve one active planning truth, keep target-state fixed to one route with no separate trust-gate or fallback route, use `06:00 Europe/Moscow` as the canonical morning publish target, and do not claim code/runtime closure that is not yet landed.
- Done Evidence: new execution contract and parent brief exist; four new phase briefs exist; active product-plane truth docs point at the new module; focused validations pass.
- Priority Rule: replace ambiguity with explicit governed truth before opening code-phase work.

## Current Delta
- Package intake showed the source plan is strong enough to define the new route shape, but further intake retries stopped being useful because the remaining blockers were repo-side truth conflicts rather than source-package quality gaps.
- Work now moved from external package intake to repo-side governed planning materialization.

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: unknown integrations and policy drifts.
3. Resource/time risks and chosen controls: phased patches and deterministic checks.
4. Highest-priority fixes or follow-ups: stabilize contract and validation first.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: repo-side planning materialization in progress
- Final Contexts: new MOEX governed execution contract, parent brief, phase briefs, and superseded product-plane truth docs
- Route Match: governed planning
- Primary Rework Cause: package intake reached diminishing returns because blockers shifted from source-package quality to repo-side truth alignment
- Incident Signature: moex-route-intake-truth-conflict
- Improvement Action: materialize the new route as first-class governed docs and explicitly supersede the old MOEX module as planning truth
- Improvement Artifact: docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md

## Blockers
- No blocker for this planning patch. Code-phase blockers remain deferred to Phase 01 and later.

## Next Step
- Validate the new planning docs, then move into Phase 01 contract materialization.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_phase_planning_contract.py`
