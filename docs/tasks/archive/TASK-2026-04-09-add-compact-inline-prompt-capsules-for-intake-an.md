# Task Note
Updated: 2026-04-09 11:27 UTC

## Goal
- Deliver: Add compact inline prompt capsules for intake and acceptor without prompt bloat

## Change Surface
- Surface Class: shell
- Guarded Surfaces: GOV-RUNTIME, GOV-DOCS
- Boundary Note: prompt assembly and process tests only; no product-plane logic

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.
- In Scope: compact intake/acceptor prompt builders, targeted process tests, task state updates
- Out of Scope: full prompt redesign, worker/remediation capsule redesign, PR gate changes
- Constraints: keep prompt budget tight, avoid duplicating full policy docs, preserve existing output contracts
- Done Evidence: focused pytest with local basetemp and loop gate on changed files

## Current Delta
- Session started and baseline scope captured.
- Change surface declared as `shell`.
- Planned patch: add one compact capsule to intake and one to acceptor, then rerun focused tests.
- Added a compact inline intake capsule without duplicating the full package policy doc.
- Added a compact inline acceptor capsule focused on PASS closure, adversarial review, and note/workaround blocking.
- Focused pytest and loop gate passed after cleanup of task-outcomes vocabulary drift from the prior task.

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
- Outcome Status: completed
- Decision Quality: correct_first_time
- Final Contexts: CTX-OPS
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: scripts/codex_from_package.py + scripts/codex_phase_orchestrator.py

## Blockers
- No blocker.

## Next Step
- Prepare PR-oriented summary; keep the shell surface declaration and mention the compact capsule design choice.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/process/test_codex_from_package.py tests/process/test_codex_phase_orchestrator.py -q --basetemp .tmp_pytest`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
