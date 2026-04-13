# Task Note
Updated: 2026-04-13 16:52 UTC

## Goal
- Deliver: Implement Stage 7 orchestration/jobs for the materialized research plane with Dagster bootstrap, backtest, ranking, and projection flow

## Task Request Contract
- Objective: promote `phase2b` from a bootstrap-only Dagster contour to a fuller orchestration layer that materializes bootstrap assets separately from backtest/ranking and projection jobs over the new research plane.
- In Scope: `src/trading_advisor_3000/dagster_defs/phase2b_assets.py`, related Dagster exports, Stage 7 unit/integration tests under `tests/product-plane/*`, and this task note.
- Out of Scope: replacing the legacy bridge completely, adding distributed execution, introducing a new Spark-first backtest engine, and unrelated runtime/API changes.
- Constraints: keep bootstrap and hot-path jobs separated; keep projection downstream from backtest/ranking instead of rebuilding candidates straight from raw features; preserve the runtime-compatible candidate contract; prefer the existing materialized research modules over any duplicate orchestration logic.
- Done Evidence: updated `phase2b` asset specs and job graph, green targeted Dagster tests, green relevant product-plane regressions, and green `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` plus `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Stage 7 target is now clear from the brief/spec: split `phase2b` into bootstrap, backtest/ranking, and projection job families over the already-materialized research plane.
- `phase2b_assets.py` now carries the fuller graph: bootstrap assets, backtest/result assets, ranking asset, and projection asset.
- Separate Dagster jobs now exist for bootstrap, backtest, and projection instead of a bootstrap-only contour.
- Targeted unit/integration tests for manifests, Dagster flow, Stage 5/6 regressions, and projection compatibility are green locally.

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
- Final Contexts: CTX-OPS, APP-PLANE, CTX-RESEARCH
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: test
- Improvement Artifact: tests/product-plane/integration/test_phase2b_dagster_bootstrap.py

## Blockers
- No blocker.

## Next Step
- Archive this Stage 7 task note, commit the process-state tail, and push the orchestration update into draft PR `#48`.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_phase2b_manifests.py -q`
- `python -m pytest tests/product-plane/integration/test_phase2b_dagster_bootstrap.py -q`
- `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q --basetemp .pytest_tmp/phase2b_research_plane_stage7`
- `python -m pytest tests/product-plane/integration/test_research_vectorbt_backtests.py -q --basetemp .pytest_tmp/research_vectorbt_backtests_stage7`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
