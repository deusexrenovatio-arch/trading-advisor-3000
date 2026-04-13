# Task Note
Updated: 2026-04-13 12:21 UTC

## Goal
- Deliver: Continue with Stage 2 of the vectorbt+pandas-ta migration package using the same source document

## Task Request Contract
- Objective: deliver Stage 2 dataset layer for the new vectorized research-plane so canonical data can be materialized into versioned datasets and reloaded by `dataset_version`.
- In Scope: `product-plane` only; `src/trading_advisor_3000/product_plane/research/datasets/**`, targeted tests under `tests/product-plane/**`, and this task note.
- Out of Scope: indicator materialization, derived feature computation, vectorbt execution, ranking/projection logic, Dagster rewiring, and external governance/runtime-route remediation.
- Constraints: build on canonical bars/session calendar/roll map as the only upstream source of truth; support `contract` and `continuous_front` series modes; include holdout/walk-forward split metadata and warmup-aware slicing; keep the existing legacy research path working.
- Done Evidence: green Stage 2 unit/integration dataset tests, no regressions in adjacent Phase 1/phase2b tests, `python scripts/validate_task_request_contract.py`, and `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: correctness, replayability, and explicit lineage beat speed or shortest implementation.

## Current Delta
- Session started for Stage 2 continuation from the same package brief.
- Package requirements were narrowed to the real Stage 2 scope: `research_datasets`, `research_bar_views`, split manifests, continuous-front selection, and warmup-aware slicing.
- Implemented a real dataset materialization path over canonical bars/session calendar/roll map with Delta-backed reload by `dataset_version`.
- Added targeted tests for contract mode, continuous-front mode, manifest/store contract fields, and canonical -> research bootstrap materialization.
- Rechecked adjacent canonical data-plane integration to confirm Stage 2 still sits cleanly on top of existing canonical surfaces.

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
- Improvement Artifact: tests/product-plane/integration/test_research_bootstrap_pipeline.py

## Blockers
- No blocker.

## Next Step
- Treat Stage 2 as the stable dataset foundation and use it as the input boundary for Stage 3 indicator materialization.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_dataset_layer.py -q`
- `python -m pytest tests/product-plane/integration/test_research_bootstrap_pipeline.py -q --basetemp .pytest_tmp/research_stage2`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python -m pytest tests/product-plane/unit/test_phase2b_features.py -q`
- `python -m pytest tests/product-plane/integration/test_phase2a_data_plane.py -q --basetemp .pytest_tmp/phase2a_data_plane`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
