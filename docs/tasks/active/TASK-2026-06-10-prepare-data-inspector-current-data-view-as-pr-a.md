# Task Note
Updated: 2026-06-10 11:18 UTC

## Goal
- Deliver: Prepare Data Inspector current-data view as PR and integrate into main

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Session started and baseline scope captured.

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

## Solution Intent
- Solution Class: target
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic publication, sample artifact, smoke only, manifest only, scaffold-only
- Closure Evidence: runtime output from FastAPI TestClient over the durable store D:/TA3000-data returned rows for raw, canonical, indicators, and derived indicators; focused pytest, ruff, and changed-scope boring checks passed for the publication contour.
- Shortcut Waiver: none
- Chosen path: product-plane FastAPI plus static HTML view over existing Delta tables.
- Why it is not a shortcut: the view resolves real storage roots and table schemas instead of fixture, sample, or generated manifest data.
- Future shape preserved: the layer registry keeps raw, canonical, indicators, and derived indicators separate so later inspector surfaces can add more data products without merging semantics.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: pending
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Implement focused patch and rerun loop gate.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
