# Task Note
Updated: 2026-04-13 13:11 UTC

## Goal
- Deliver: Close the remaining Stage 3 gap by wiring indicator bootstrap into the orchestration/job path

## Task Request Contract
- Objective: close the remaining Stage 3 gap by wiring the indicator bootstrap into the executable orchestration/job path.
- In Scope: `src/trading_advisor_3000/dagster_defs/phase2b_assets.py`, related `dagster_defs` exports, Phase 2/3 bootstrap tests, and this task note.
- Out of Scope: Stage 4 derived features, vectorbt execution, ranking/projection, and broader runtime orchestration beyond the Stage 3 bootstrap path.
- Constraints: preserve the code-level Stage 3 indicator semantics already implemented; keep the new bootstrap path executable and testable; do not regress the existing legacy phase2b surfaces while adding the new dataset/bar-view/indicator bootstrap path.
- Done Evidence: green `phase2b` manifest tests, green Dagster bootstrap integration test, green indicator integration regression, and `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: executable orchestration closure and truthful acceptance alignment beat minimal diff size.

## Current Delta
- Session started for the Stage 3 orchestration closeout pass.
- Added executable `phase2b` bootstrap assets for `research_datasets`, `research_bar_views`, and `research_indicator_frames`.
- Added Dagster bootstrap definitions/job/materialization helper and integration coverage for the new path.
- Hardened indicator bootstrap to emit null-filled rows instead of failing on short warmup-only partitions.

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
- Treat Stage 3 as closed on both the indicator semantics and bootstrap orchestration axes, then continue with Stage 4 derived features.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_phase2b_manifests.py -q`
- `python -m pytest tests/product-plane/integration/test_phase2b_dagster_bootstrap.py -q --basetemp .pytest_tmp/phase2b_bootstrap`
- `python -m pytest tests/product-plane/integration/test_research_indicator_materialization.py -q --basetemp .pytest_tmp/research_stage3_final`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
