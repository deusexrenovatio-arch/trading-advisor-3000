# Task Note
Updated: 2026-04-26 14:20 UTC

## Goal
- Deliver: Re-split derived-indicator and feature-layer removal work into policy-clean PRs and integrate through PR flow

## Task Request Contract
- Objective: re-split the current research data-plane work into policy-clean PRs, retire the active feature materialization layer, and integrate the accepted stack through PR-only main flow.
- In Scope: research contracts, derived indicator routing, vectorbt/backtest data prep, active feature-layer removal, focused tests, docs, PR metadata, and the governance task pointer required by loop/pr gates.
- Out of Scope: unrelated open PRs, historical archive rewrites, direct-main pushes, and recreating feature semantics under a replacement namespace.
- Constraints: declare change surface as mixed, keep product logic out of shell control-plane files, preserve indicator/derived-indicator boundaries, and pass loop/pr lanes before merge.
- Done Evidence: merged replacement PR stack, closed superseded draft PRs, passing GitHub lanes, targeted local pytest suites, and stack conformance validation.
- Priority Rule: repo-policy correctness and reviewability win over minimizing PR count.

## Current Delta
- Superseded draft PRs are closed, the replacement stack is split by reviewable research capability, and the final feature-layer removal PR needs explicit multi-contour Solution Intent for the pull-request loop gate.

## Solution Intent
- Solution Class: target
- Critical Contour: multi-contour
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact, fixture path, tests/product-plane/fixtures/, synthetic publication, smoke only, manifest only
- Closure Evidence: integration test coverage proves the canonical dataset and downstream research handoff, runtime output remains published through durable store candidate contracts, the publication contour is covered by runtime lifecycle/store tests, and GitHub loop/pr lanes must pass before merge.
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
