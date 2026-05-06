# Task Note
Updated: 2026-03-27 15:21 UTC

## Goal
- Deliver: Push policy-compliant foundation/data branch for stack-conformance remediation.

## Task Request Contract
- Objective: push a policy-compliant foundation/data branch for stack-conformance remediation without triggering the runtime-publication contour.
- In Scope: stack-conformance governance surfaces, Delta/Spark/Dagster/data-plane code and tests, data-branch truth-source alignment, and branch-local process state.
- Out of Scope: runtime bootstrap, FastAPI closure, Telegram adapter closure, .NET sidecar closure, and final release-readiness reporting.
- Constraints: one critical contour only (`data-integration-closure`); no silent assumptions/skips/fallbacks/deferrals; branch truth-source must not overclaim runtime/sidecar surfaces that are absent here.
- Done Evidence: task/session validators, stack-conformance validator, solution-intent validator, and branch push to origin on a single-contour diff.
- Priority Rule: policy-compliant branch integrity over speed.

## Current Delta
- Session is bound to the foundation/data branch.
- Branch-local truth-source must be downgraded to match only data-foundation surfaces present on this branch.
- Runtime/API/sidecar claims from the integrated remediation series must not leak into this branch.

## First-Time-Right Report
1. Confirmed coverage: this branch should carry governance plus data-integration closure only.
2. Missing or risky scenarios: integrated truth-source files overclaim runtime or sidecar surfaces that are intentionally absent here.
3. Resource/time risks and chosen controls: branch-specific truth-source downgrade plus deterministic validators before push.
4. Highest-priority fixes or follow-ups: make stack-conformance and solution-intent pass on a single-contour diff.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact
- Closure Evidence: integration test evidence for real Delta runtime, canonical dataset outputs, downstream research proof, and runtime-ready surface alignment limited to the data foundation contour
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
