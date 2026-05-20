# Task Note
Updated: 2026-05-19 15:23 UTC

## Goal
- Deliver: Implement PLAN_rbar_canon research_bar_views canonical usage contract

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Implemented `moex_bar_usage_v1` persisted usage contract for `research_bar_views`.
- Wired research L0 Spark inputs to canonical provenance and session intervals.
- Added focused unit coverage for schema, profile flags, explicit bar rules, dataclass round-trip, and Spark job input contract.

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
- Final Contexts: product-plane data/research plus governance task-session artifacts
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: test
- Improvement Artifact: `moex_bar_usage_v1` policy helper and focused tests

## Blockers
- No blocker.

## Next Step
- Ready for review.

## Validation
- `python -m pytest tests/product-plane/unit/test_research_dataset_layer.py tests/product-plane/unit/test_research_dagster_manifests.py -q`
- `python -m ruff check ...changed files...`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
