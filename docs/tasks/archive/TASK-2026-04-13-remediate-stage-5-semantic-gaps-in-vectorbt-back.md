# Task Note
Updated: 2026-04-13 15:00 UTC

## Goal
- Deliver: Remediate Stage 5 semantic gaps in vectorbt backtests and strategy parameter execution

## Task Request Contract
- Objective: finish the semantic Stage 5 remediation so strategy parameters, risk policy, direction modes, split semantics, and mass-sweep evidence are all wired into the real vectorbt path.
- In Scope: `src/trading_advisor_3000/product_plane/research/strategies/*`, `research/backtests/*`, `research/io/*`, Stage 5 unit/integration tests, and this task note.
- Out of Scope: Stage 6 ranking/projection rollout, Stage 7 Dagster backtest jobs, removing the legacy compatibility bridge, and unrelated governance changes.
- Constraints: keep the materialized bars/indicator/feature layers as the hot-path source; do not regress the compatibility bridge; make strategy parameters semantically live instead of decorative; keep split/fold behavior tied to dataset semantics where available.
- Done Evidence: green semantic Stage 5 unit/integration tests, green regression tests, and green `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` plus `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: semantic correctness and honest parameterization beat a smaller diff.

## Current Delta
- Session started from review findings against the first Stage 5 pass.
- Connected `breakout_window`, `atr_target_multiple`, and `risk_policy` into real execution semantics.
- Added real `short_only` handling and tied fold execution to dataset split manifests when present.
- Added stronger tests for parameter effects, direction-mode handling, split semantics, and a 100-combination batched sweep with cache reuse.

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
- Improvement Artifact: tests/product-plane/unit/test_research_backtest_engine_semantics.py

## Blockers
- No blocker.

## Next Step
- Treat the Stage 5 semantic remediation as closed and continue with Stage 6 ranking plus runtime-compatible projection.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_backtest_layer.py -q`
- `python -m pytest tests/product-plane/unit/test_research_backtest_engine_semantics.py -q`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python -m pytest tests/product-plane/integration/test_research_vectorbt_backtests.py -q --basetemp .pytest_tmp/research_vectorbt_backtests`
- `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q --basetemp .pytest_tmp/phase2b_research_plane`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
