# Task Note
Updated: 2026-04-13 07:03 UTC

## Goal
- Deliver: Take Phase 1 of the vectorbt+pandas-ta research-plane migration package into work as a product-plane foundation slice

## Task Request Contract
- Objective: deliver Phase 1 foundation for the vectorized research-plane migration without breaking the current legacy research hot path.
- In Scope: `product-plane` only; `pyproject.toml`, `src/trading_advisor_3000/product_plane/research/**`, `tests/product-plane/**`, and this task note.
- Out of Scope: dataset materialization from canonical data, pandas-ta indicator execution, vectorbt backtest execution, Dagster rewiring, contract-schema expansion, and runtime candidate cutover.
- Constraints: keep business logic out of shell files; keep legacy `research/backtest`, `research/features`, and `research/pipeline` behavior intact; treat `vectorbt` and `pandas-ta` as optional dependencies behind a clear adapter; do not create a big-bang rewrite.
- Done Evidence: targeted unit tests for the new scaffold, namespace import safety without optional deps, `python scripts/validate_task_request_contract.py`, `python scripts/validate_session_handoff.py`, and `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: prefer durable structure, typed interfaces, and migration safety over speed or minimum diff size.

## Current Delta
- Governed package intake was started first, and the phase work is now bound to a product-plane foundation slice.
- Existing research-plane remains legacy/custom: hand-built feature engine, sample strategies, and custom backtest loop are still the live path.
- This patch set introduces the vectorized Phase 1 scaffold alongside legacy code instead of replacing it in-place.
- Added `research-vectorized` optional dependencies plus a compatibility adapter that accepts either `pandas-ta-classic` or `pandas-ta` import layouts.
- Added typed Phase 1 scaffold for dataset, indicator, feature, strategy, backtest, IO, and compatibility layers.
- Added focused unit coverage for the new foundation and rechecked legacy phase2b research tests.

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
- Outcome Status: partial
- Decision Quality: environment_blocked
- Final Contexts: CTX-OPS, APP-PLANE, CTX-RESEARCH
- Route Match: matched
- Primary Rework Cause: environment
- Incident Signature: none
- Improvement Action: env
- Improvement Artifact: scripts/run_phase2a_spark_proof.py docker proof lane / PyYAML runtime image

## Blockers
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` still fails in the existing data-proof Docker/Spark proof lane.
- Failure details: inside the container, `import yaml` crashes with `SyntaxError: source code string cannot contain null bytes`; the local `foundation.py` file has zero null bytes, so the blocker points to container/runtime packaging rather than to this Phase 1 research scaffold.

## Next Step
- Keep the Phase 1 scaffold as the accepted foundation slice, then remediate the separate Docker/PyYAML proof-image issue before attempting PR closeout again.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python -m pytest tests/product-plane/unit/test_phase2b_features.py -q`
- `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q --basetemp .pytest_tmp/integration_phase2b`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` -> blocked by pre-existing Docker/PyYAML proof-lane failure
