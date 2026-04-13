# Task Note
Updated: 2026-04-13 14:10 UTC

## Goal
- Deliver: Implement Stage 5 strategy specs and vectorbt backtests for the materialized research plane

## Task Request Contract
- Objective: implement Stage 5 typed strategy specs plus a real `vectorbt` backtest layer that consumes materialized research bars, indicators, and feature frames without recomputing them.
- In Scope: `src/trading_advisor_3000/product_plane/research/strategies/*`, `research/backtests/*`, `research/io/*`, Stage 5 unit/integration tests, and this task note.
- Out of Scope: Stage 6 ranking/projection rollout, Stage 7 Dagster backtest/projection jobs, replacing the legacy `research.backtest` bridge, and runtime publication changes.
- Constraints: keep canonical/materialized layers as source-of-truth; do not reintroduce custom hot-path indicator/feature recompute; support both `Portfolio.from_signals(...)` and at least one `Portfolio.from_order_func(...)` scenario; preserve the legacy backtest path for compatibility.
- Done Evidence: green Stage 5 unit/integration tests, green affected regression tests, and green `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: truthful Stage 5 closure and stable vectorbt semantics beat minimum diff size.

## Current Delta
- Session started and Stage 5 acceptance scope confirmed from the execution brief, technical spec, and acceptance plan.
- Added richer strategy specs and registry helpers for the five required strategy families.
- Added hot-run slice loading over materialized bars, indicators, and feature frames with in-process cache reuse.
- Added a real `vectorbt` backtest engine plus batch runner that writes batch/run/stats/trade artifacts and exercises both `from_signals` and `from_order_func`.

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
- Improvement Artifact: tests/product-plane/integration/test_research_vectorbt_backtests.py

## Blockers
- No blocker.

## Next Step
- Treat Stage 5 as closed and continue with Stage 6 result ranking plus runtime-compatible candidate projection.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_backtest_layer.py -q`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python -m pytest tests/product-plane/integration/test_research_vectorbt_backtests.py -q --basetemp .pytest_tmp/research_vectorbt_backtests`
- `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q --basetemp .pytest_tmp/phase2b_research_plane`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
