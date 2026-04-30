# Task Note
Updated: 2026-04-30 13:39 UTC

## Goal
- Deliver: Split Optuna vectorbt preintegration changes into reviewable PR stack

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Session started and baseline scope captured.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: integration test coverage; canonical dataset rooted forced data-prep proof run `20260430T122745128431Z`; downstream research backtest campaign run `20260430T124414366266Z`; focused research tests; changed-files loop gate; and reviewable stacked draft PRs.
- Shortcut Waiver: none

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
