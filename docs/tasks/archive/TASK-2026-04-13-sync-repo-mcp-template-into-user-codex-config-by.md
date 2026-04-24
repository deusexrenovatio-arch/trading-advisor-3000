# Task Note
Updated: 2026-04-13 13:11 UTC

## Goal
- Deliver: Sync repo MCP template into user Codex config by fixing bootstrap merge handling for existing TOML constructs.

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Repo-local MCP config was generated from the repository template.
- User-level Codex config was merged with the repository MCP server set after fixing TOML serialization for existing complex user config shapes.
- Focused MCP validation/tests passed, while the shared loop gate still reports an unrelated pre-existing task-outcomes ledger issue elsewhere in the dirty tree.

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
- Final Contexts: CTX-OPS, MCP bootstrap hardening, repo-local config sync, and user-level config merge
- Route Match: matched
- Primary Rework Cause: workflow_gap
- Incident Signature: none
- Improvement Action: test
- Improvement Artifact: tests/process/test_mcp_rollout_contracts.py

## Blockers
- No blocker for this completed MCP sync patch set.

## Next Step
- Use the synced base profile and only opt into ops/data_readonly when the required env-backed credentials are present.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_mcp_config.py`
- `python -m pytest tests/process/test_mcp_rollout_contracts.py -q`
- `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check --probe-commands --format json`
