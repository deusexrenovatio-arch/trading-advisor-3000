# Task Note
Updated: 2026-05-07 11:07 UTC

## Goal
- Deliver: Publish continuous front calendar session active front PR

## Task Request Contract
- Objective: publish and integrate the calendar/session active-front change through the PR-only route.
- In Scope: continuous-front policy defaults, MOEX canonical refresh window hardening, related schemas/configs, regression tests, and the process evidence required by the PR gate.
- Out of Scope: rebuilding production Delta tables, changing indicator formulas, changing derived-indicator semantics beyond regression coverage, and adding spread/basis contours.
- Constraints: main remains PR-only; active futures selection must stay deterministic by date/session, not by unavailable open interest; local proof cannot substitute the GitHub gate.
- Done Evidence: local focused tests pass, ruff passes, pre-push loop gate passes, GitHub PR checks pass, and the PR is mergeable into `main`.
- Priority Rule: keep the contour reviewable and policy-compliant even if that requires an explicit process commit.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: none
- Closure Evidence: integration test coverage confirms the canonical dataset handoff into downstream research, with runtime-ready surface checks for MOEX baseline/canonical refresh and continuous-front Dagster routing.
- Shortcut Waiver: none
- Design Checkpoint: chosen path=publish the real canonical-to-research contour changes already committed on the branch; why_not_shortcut=the PR keeps Spark/Dagster/data-plane code and executable tests together instead of replacing runtime behavior with documentation; future_shape=canonical bars remain the silver layer and `pit_active_front` is produced as gold L0 bars for downstream indicators.

## Current Delta
- Branch contains four product/data commits plus this process note so the remote PR gate can evaluate the required Solution Intent.
- PR #101 was opened against `main`; the first PR run exposed that Solution Intent must be present in the branch, not only in local session state.

## First-Time-Right Report
1. Confirmed coverage: canonical refresh, continuous-front Spark route, Dagster exposure, and derived chronology are covered by focused tests.
2. Missing or risky scenarios: operational materialized data refresh remains a post-merge run, not PR evidence.
3. Resource/time risks and chosen controls: CI policy checks are treated as merge blockers; local state alone is not accepted.
4. Highest-priority fixes or follow-ups: after merge, run the canonical and continuous-front refresh against the authoritative data root.

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
- Push the process note, wait for GitHub PR checks, then merge only if the required checks are green.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
