# Task Note
Updated: 2026-05-08 13:00 UTC

## Goal
- Deliver: Fix MOEX product and verification staging runtime instance roots and Docker-visible names

## Solution Intent
- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only.
- Closure Evidence: runtime-ready surface is registry-backed for product and test staging roles; Docker compose exposes readable product/test staging mount paths; Dagster launch helper resolves run config from the registry and requires explicit product write approval; targeted unit tests, ruff, compose config, and local Dagster run status checks prove the new contract without restarting active containers.
- Shortcut Waiver: staged operational rollout because the current Dagster product run is still in progress; Docker containers should be redeployed after that run completes to expose the new readable mount aliases live.

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- MOEX staging instances are fixed as a registry-backed product-runtime staging and on-demand verification staging contract with Docker-visible product/test mount aliases.

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
- Decision Quality: correct_after_replan
- Final Contexts: MOEX runtime instances registry, Docker staging mounts, Dagster GraphQL launch helper, targeted unit tests, loop gate, PR gate.
- Route Match: expanded
- Primary Rework Cause: workflow_gap
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: MOEX product/test staging runtime instance contract recorded.
- Closed At: 2026-05-08T13:00:00Z

## Blockers
- No blocker.

## Next Step
- Ready for review or PR preparation; redeploy Docker staging only after the active Dagster product run completes.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
