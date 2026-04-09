# Task Note
Updated: 2026-04-09 10:59 UTC

## Goal
- Deliver: Harden PR review contract and intake/policy enforcement for recurrence-risk and review lenses

## Change Surface
- Surface Class: shell
- Guarded Surfaces: GOV-DOCS, GOV-RUNTIME
- Boundary Note: keep changes inside governance/prompts/policy/tests; no product-plane trading logic

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.
- In Scope: docs/codex orchestration contracts, intake/acceptor prompts, codex phase policy/orchestrator runtime, targeted process tests
- Out of Scope: product-plane runtime logic, GitHub PR automation changes, broad skill catalog refactors
- Constraints: fail-closed review semantics, shell-only surface, ordered patching across contracts -> runtime -> tests
- Done Evidence: targeted pytest for intake/policy/orchestrator plus loop gate on changed files

## Current Delta
- Session started and baseline scope captured.
- Change surface declared as `shell`.
- Planned patch order: contracts/prompts -> runtime enforcement -> tests.
- Contracts, runtime, and tests were updated for fail-closed review enforcement.
- Local pytest passed with repo-local `--basetemp` to avoid the known host temp permission issue.
- Loop gate passed on the final changed-files snapshot.

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
- Improvement Artifact: docs/codex/prompts/phases/acceptor.md + scripts/codex_phase_policy.py + scripts/codex_from_package.py

## Blockers
- No blocker.

## Next Step
- Prepare PR-oriented summary and keep the same `shell` surface declaration in PR metadata.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/process/test_codex_from_package.py tests/process/test_codex_phase_policy.py tests/process/test_codex_phase_orchestrator.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
