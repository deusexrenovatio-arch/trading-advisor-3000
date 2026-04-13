# Task Note
Updated: 2026-04-13 16:39 UTC

## Goal
- Deliver: Remediate Stage 6 semantic gaps: make ranking policy metric_order live, make projection selection_policy selectable, and add order/drawdown result artifacts

## Task Request Contract
- Objective: remove the remaining semantic gaps in Stage 6 so declared ranking/projection policies actually influence behavior and the results layer emits the fuller artifact set expected by the brief/spec.
- In Scope: `src/trading_advisor_3000/product_plane/research/backtests/*`, `research/compat/*` if projection compatibility wording needs tightening, Stage 6 unit/integration tests, and this task note.
- Out of Scope: Stage 7 orchestration/jobs, unrelated runtime publication changes, and non-Stage-6 governance refactors.
- Constraints: keep the materialized research-plane path primary; do not introduce decorative policy fields again; preserve existing Stage 5 semantics while expanding result artifacts.
- Done Evidence: green Stage 6 unit/integration tests, green affected Stage 5 regressions, and green `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` plus `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- `metric_order` is now wired into ranking through policy-driven metric scoring, representative-run selection, and final rank ordering.
- `selection_policy` now has explicit supported modes with distinct row-selection behavior instead of a single hardcoded path.
- Results artifacts now include `research_order_records` and `research_drawdown_records` alongside trades/stats.
- Dedicated unit/integration tests are green for the semantic gaps plus Stage 5 regression slices.

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
- Archive this remediation note, commit the process-state tail, and push the Stage 6 semantic fix to draft PR `#48`.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_ranking_projection.py -q`
- `python -m pytest tests/product-plane/unit/test_research_backtest_layer.py -q`
- `python -m pytest tests/product-plane/unit/test_research_backtest_engine_semantics.py -q`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python -m pytest tests/product-plane/contracts/test_phase1_contracts.py -q`
- `python -m pytest tests/product-plane/integration/test_research_vectorbt_backtests.py -q --basetemp .pytest_tmp/research_vectorbt_backtests_stage6_remediate`
- `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q --basetemp .pytest_tmp/phase2b_research_plane_stage6_semantic`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
