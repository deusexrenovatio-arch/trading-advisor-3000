# Task Note
Updated: 2026-04-13 16:12 UTC

## Goal
- Deliver: Publish Stage 5 remediation as clean commits/PR update and close Stage 6 results, ranking, and projection for the materialized research plane

## Task Request Contract
- Objective: publish the already-validated Stage 5 semantic remediation as a reviewable patch series, then implement Stage 6 so the materialized research plane persists full result artifacts, ranks strategy variants robustly, and projects runtime-compatible candidates.
- In Scope: `src/trading_advisor_3000/product_plane/research/strategies/*`, `research/io/*`, `research/backtests/*`, `research/compat/*` if required for projection compatibility, Stage 5/6 unit and integration tests, Stage 5 archive task notes, and the active task note for this session.
- Out of Scope: Stage 7 Dagster backtest/ranking/projection orchestration, live runtime publication changes, removal of the legacy compatibility bridge, and unrelated shell-governance refactors.
- Constraints: keep the materialized dataset/indicator/feature layers as the hot-path source; do not recompute indicators inside the backtest/ranking loop; keep projection runtime-compatible with existing candidate expectations; preserve PR-readable commit hygiene by splitting Stage 5 code, tests, and process state.
- Done Evidence: Stage 5 commits pushed into the existing draft PR, green Stage 5/6 unit and integration tests, green `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`, and green `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Session started for a two-part closeout: publish Stage 5 first, then implement Stage 6 on the same branch/PR.
- Stage 5 semantic remediation has been sliced into four commits, pushed to the remote branch, and annotated in draft PR `#48`.
- Stage 6 now adds a materialized results layer over Stage 5 artifacts, robust ranking over fold/trade outcomes, and a runtime-compatible candidate projection bridge that reuses the real signal builders.
- Stage 6 proof is green on the dedicated unit/integration tests plus Stage 5 compatibility regressions.

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
- Improvement Artifact: tests/product-plane/unit/test_research_ranking_projection.py

## Blockers
- No blocker.

## Next Step
- Archive this task note, commit the process-state tail, and push the final Stage 6 update into draft PR `#48`.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_backtest_layer.py -q`
- `python -m pytest tests/product-plane/unit/test_research_backtest_engine_semantics.py -q`
- `python -m pytest tests/product-plane/unit/test_research_ranking_projection.py -q`
- `python -m pytest tests/product-plane/contracts/test_phase1_contracts.py -q`
- `python -m pytest tests/product-plane/integration/test_research_vectorbt_backtests.py -q --basetemp .pytest_tmp/research_vectorbt_backtests`
- `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q --basetemp .pytest_tmp/phase2b_research_plane_stage6`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
