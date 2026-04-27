# Task Note
Updated: 2026-04-27 08:50 UTC

## Goal
- Deliver: Rewrite active product-plane research backtest route to vectorbt family-level parametric strategy search from TZ
- Change Surface: product-plane

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact
- Closure Evidence: focused research unit and integration tests prove vectorbt family-level parametric search, native-clock SurfSpec input resolution, param_hash result/ranking storage, campaign/Dagster route behavior, and fail-closed retired field handling; loop gate and PR policy checks must pass before merge.
- Shortcut Waiver: none
- Target: replace the active research/backtest execution path with family-level vectorbt parametric search.
- Staged: keep StrategyInstance storage only as downstream promotion infrastructure after param-level search.
- Fallback: fail closed on old per-instance campaign fields, missing materialized indicator/derived inputs, unsupported surface modes, or unverified MTF alignment rather than routing through the old per-instance signal loop.

## Task Request Contract
- Objective: active product-plane research strategy execution uses vectorbt family-search surfaces before StrategyInstance promotion.
- In Scope: `src/trading_advisor_3000/product_plane/research/*`, Dagster research assets, research campaign contract/configs, matching product-plane tests, and focused product-plane docs/runbook notes.
- Out of Scope: shell control-plane behavior, live execution, broker/runtime signal publication, and new 5m production strategy scope.
- Constraints: preserve materialized indicator/derived roots, do not recreate the retired feature layer, keep closed-bar signal shift explicit, and do not leave the old per-instance Python signal builder as a supported primary route.
- Done Evidence: focused product-plane tests plus `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Active route now uses `StrategyFamilySearchSpec`, `VectorBTInputBundle`, vectorbt signal surfaces, and param_hash-level result/gate tables.
- Campaign `strategy_space` now uses `max_parameter_combinations`; retired per-instance fields fail closed.
- Dagster exposes vectorbt search tables as first-class research assets.
- Docs/configs/tests describe StrategyInstance materialization only as post-ranking promotion infrastructure.
- Focused vectorbt, campaign, Dagster, route, and manifest-storage tests pass locally.

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
- Rerun loop gate after rebinding the task session to the current branch.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
