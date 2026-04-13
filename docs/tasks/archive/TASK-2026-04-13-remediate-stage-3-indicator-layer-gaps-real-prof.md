# Task Note
Updated: 2026-04-13 12:59 UTC

## Goal
- Deliver: Remediate Stage 3 indicator layer gaps: real profile registry, richer profile contract, incremental partition refresh, and missing metadata

## Task Request Contract
- Objective: close the identified code-level Stage 3 gaps by turning the indicator layer into a real profile registry with richer spec contracts, partition-aware incremental refresh, and complete bootstrap metadata.
- In Scope: `product-plane` only; `src/trading_advisor_3000/product_plane/research/indicators/**`, targeted indicator tests, and this task note.
- Out of Scope: Stage 4 derived features, full orchestration/Dagster bootstrap integration, vectorbt execution, and unrelated governance/runtime issues.
- Constraints: preserve Stage 1/2 behavior; align vocabulary and contracts with the package spec (`core_v1`, `core_intraday_v1`, `core_swing_v1`); recompute only affected logical partitions; keep point-in-time behavior stable.
- Done Evidence: green indicator unit/integration remediation tests, green adjacent Phase 1/2 tests, `python scripts/validate_task_request_contract.py`, and `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: truthful contract alignment and extensibility matter more than minimal diff size.

## Current Delta
- Session started for the Stage 3 remediation pass.
- Added a real versioned profile registry with `core_v1`, `core_intraday_v1`, and `core_swing_v1`.
- Expanded `IndicatorSpec` to carry parameters, required inputs, expected outputs, and warmup requirements.
- Added explicit metadata fields `profile_version` and `null_warmup_span`.
- Reworked materialization to refresh only affected logical partitions while preserving unchanged ones.
- Added regression coverage for the richer registry/spec contract and for incremental partition refresh.

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
- Improvement Artifact: tests/product-plane/integration/test_research_indicator_materialization.py

## Blockers
- No blocker.

## Next Step
- Treat the code-level Stage 3 contract gaps as remediated; if package-level Stage 3 acceptance is still required, the remaining explicit gap is orchestration/bootstrap integration rather than indicator-layer semantics.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_indicator_layer.py -q`
- `python -m pytest tests/product-plane/integration/test_research_indicator_materialization.py -q --basetemp .pytest_tmp/research_stage3_remed`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python -m pytest tests/product-plane/unit/test_research_dataset_layer.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
