# Task Note
Updated: 2026-04-28 11:19 UTC

## Goal
- Deliver: Profile vectorbt family search on a real instrument and fix mtf pullback native signal alignment
- Change Surface: product-plane

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact
- Closure Evidence: integration test and focused unit evidence preserve canonical dataset to downstream research handoff, with the vectorbt runtime-ready surface validated against materialized `D:/TA3000-data` Delta inputs and loop gate before PR update.
- Shortcut Waiver: none
- Target: prove the vectorbt family-search route on a real instrument/contract and repair the native-clock mtf pullback signal alignment bug found during profiling.
- Staged: keep broader loader optimization as a follow-up after capturing profiling evidence.
- Fallback: fail closed on missing native clock inputs, duplicate execution columns, or unresolved data-shape errors rather than silently falling back to precomputed or fixture-only proof.

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
