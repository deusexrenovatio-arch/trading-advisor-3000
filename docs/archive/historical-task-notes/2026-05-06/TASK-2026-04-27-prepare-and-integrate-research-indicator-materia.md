# Task Note
Updated: 2026-04-27 11:10 UTC

## Goal
- Deliver: Prepare and integrate research indicator materialization PR

## Task Request Contract
- Objective: publish and integrate the completed research indicator and derived-indicator materialization branch through the repository PR flow.
- In Scope: PR preparation, local validation, branch push, PR creation, CI/merge observation, and final main integration.
- Out of Scope: additional materialization logic changes, new strategy logic, canonical MOEX route changes, and unrelated context-routing task edits.
- Constraints: preserve PR-only main policy, keep verification-copy cleanup outside committed artifacts, and do not mix unrelated local work into the data-plane PR.
- Done Evidence: current Delta outputs remain present, verification copies are cleaned, relevant tests pass, loop gate passes against `origin/main`, PR is merged into `main`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: scaffold-only, sample artifact, synthetic upstream
- Closure Evidence: integration test evidence proves Delta schema extension, partition replacement, downstream research indicator profile extension, derived profile extension, and derived reuse after unrelated base-indicator addition; real current Delta outputs under `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current`, `_delta_log` and row-count proof for `research_indicator_frames.delta` and `research_derived_indicator_frames.delta`, `research_data_prep_job`/sensor import proof, loop gate, PR merge result
- Shortcut Waiver: none

## Current Delta
- Session started and baseline scope captured.
- Data-plane implementation commit is already prepared on `codex/materialize-indicators-derived-job`.
- Verification copies under `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification` were cleaned after evidence capture.

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
