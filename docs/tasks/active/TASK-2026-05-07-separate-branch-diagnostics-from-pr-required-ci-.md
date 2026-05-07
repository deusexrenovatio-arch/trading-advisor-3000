# Task Note
Updated: 2026-05-07 12:55 UTC

## Goal
- Deliver: Separate branch diagnostics from PR-required CI lanes

## Task Request Contract
- Objective: make PR-required hosted CI contexts represent PR-range validation only, while branch push diagnostics use a separate non-required context.
- In Scope: `.github/workflows/ci.yml`, hot gate docs, governance remediation guidance, and process tests that lock the lane split.
- Out of Scope: branch protection rule changes, new scanners, coverage gates, product-plane logic, or hosted runner billing/config changes.
- Constraints: keep hosted CI opt-in, keep `loop-lane` and `pr-lane` as required `main` contexts, and avoid lowering gate strictness.
- Done Evidence: YAML parse, workflow contract tests, docs links, PR-only policy validation, boring quick checks, loop gate, and PR gate.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Implemented `branch-lane` for push/manual diagnostics.
- Reserved `loop-lane` and `pr-lane` for pull request events.
- Updated docs and process test coverage for the lane ownership contract.

## First-Time-Right Report
1. Confirmed coverage: workflow, required-context docs, remediation guidance, and process tests are aligned.
2. Missing or risky scenarios: live GitHub branch protection still depends on server configuration, so validation checks only confirm required context names.
3. Resource/time risks and chosen controls: avoid duplicate merge-required contexts by keeping push/manual checks under `branch-lane`.
4. Highest-priority fixes or follow-ups: observe the hosted PR run before merge and adjust only if GitHub reports a real YAML or required-check mismatch.

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
- Push PR and verify hosted `branch-lane`/PR-required lane separation.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
