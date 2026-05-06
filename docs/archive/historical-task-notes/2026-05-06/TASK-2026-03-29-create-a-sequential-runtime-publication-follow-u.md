# Task Note
Updated: 2026-03-29 18:48 UTC

## Goal
- Deliver: Create a sequential runtime/publication follow-up branch from updated origin/main by carrying the runtime bootstrap and .NET sidecar contour without regressing merged foundation/data truth-source surfaces.

## Task Request Contract
- Objective: produce one pushable runtime/publication branch from the already-updated `origin/main` that preserves merged data truth and carries the durable runtime + .NET sidecar contour as the only new critical closure.
- In Scope: runtime bootstrap/API/sidecar code, runtime-side docs and runbooks, stack-conformance truth sources that must be recombined after the first merge, and process metadata required for policy-compliant validation.
- Out of Scope: reworking foundation/data proofs, reopening merged data contour claims, emergency direct-main push, and any branch-local downgrade of already merged data surfaces.
- Constraints: stay within `runtime-publication-closure`, treat `origin/main` after PR #14 as canonical base, keep PR flow only, and prefer explicit reconciliation over blind branch merge.
- Done Evidence: branch-local validators pass against `origin/main`, the resulting branch contains runtime/publication changes without data regressions, and it is ready for its own PR merge.
- Priority Rule: preserve truth-source correctness and replayable merge history over minimizing number of edits.

## Current Delta
- Foundation/data contour is already merged into `main`, so the old runtime split branch can no longer be merged as-is because it would reintroduce branch-local downgrades against the new base.
- The chosen path is to rebuild the runtime follow-up from updated `origin/main` and carry only the runtime/publication contour with explicit truth-source recombination.

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

## Solution Intent
- Solution Class: target
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic publication, sample artifact, smoke only, manifest only, scaffold-only
- Closure Evidence: runtime output, durable store, publication contour, and end-to-end publication evidence carried on top of the already merged foundation/data base without reverting truth-source surfaces
- Shortcut Waiver: none

Design checkpoint:
- chosen path: replay runtime-specific commits onto updated `origin/main` and manually reconcile shared registry/docs surfaces to the combined truth
- why it is not a shortcut: the branch is rebuilt against the real merged base instead of relying on a stale split branch whose diff still encodes old compensating downgrades
- what future shape is preserved: the runtime/publication PR remains single-contour and mergeable after the first contour has landed

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
