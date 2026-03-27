# Task Note
Updated: 2026-03-27 15:29 UTC

## Goal
- Deliver: Push policy-compliant runtime and sidecar branch for stack-conformance remediation.

## Task Request Contract
- Objective: push a policy-compliant runtime and sidecar branch for stack-conformance remediation without triggering the data-integration contour.
- In Scope: stack-conformance governance surfaces, durable runtime bootstrap, FastAPI closure, live transport baseline, .NET sidecar proof, runtime-side truth-source alignment, and branch-local process state.
- Out of Scope: Delta runtime, Spark closure, Dagster closure, and final release-readiness unlock.
- Constraints: one critical contour only (`runtime-publication-closure`); no silent assumptions/skips/fallbacks/deferrals; branch truth-source must not overclaim data-foundation surfaces that are absent here.
- Done Evidence: task/session validators, stack-conformance validator, solution-intent validator, and branch push to origin on a single-contour diff.
- Priority Rule: policy-compliant runtime contour integrity over speed.

## Current Delta
- Session is bound to the runtime/sidecar branch.
- Branch-local truth-source must be narrowed to runtime/API/sidecar surfaces present here.
- Data-foundation promotions from the integrated remediation series must not leak into this branch.

## First-Time-Right Report
1. Confirmed coverage: this branch should carry governance plus runtime-publication closure only.
2. Missing or risky scenarios: integrated truth-source files overclaim data surfaces that are intentionally absent here.
3. Resource/time risks and chosen controls: branch-specific truth-source downgrade plus deterministic validators before push.
4. Highest-priority fixes or follow-ups: make stack-conformance and contour validators pass on a single-runtime-contour diff.

## Solution Intent
- Solution Class: target
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic publication, sample artifact, smoke only, manifest only, scaffold-only
- Closure Evidence: runtime output, durable store, publication contour, and end-to-end publication evidence limited to the runtime and sidecar baseline on this branch
- Shortcut Waiver: none

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
