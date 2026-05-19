# Task Note
Updated: 2026-05-17 15:24 UTC

## Goal
- Deliver: Implement official MOEX session-bounded canonicalization plan from PLAN_canon.md

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact, fixture path
- Closure Evidence: integration test coverage validates official-session-bounded canonical dataset generation, session admission reports, PASS-NOOP behavior, downstream research handoff compatibility through canonical outputs, and runtime-ready surface contracts.
- Shortcut Waiver: none
- Design Checkpoint: chosen path=official session intervals as required upstream boundary; why_not_shortcut=canonicalization no longer infers sessions from raw candles and rejects missing official intervals; future_shape=the same canonical tables feed downstream research and runtime-ready surfaces without widening shell/product boundaries.

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
