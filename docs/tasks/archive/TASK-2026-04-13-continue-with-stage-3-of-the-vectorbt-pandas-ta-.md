# Task Note
Updated: 2026-04-13 12:41 UTC

## Goal
- Deliver: Continue with Stage 3 of the vectorbt+pandas-ta migration package: implement the indicator layer on top of the dataset layer

## Task Request Contract
- Objective: deliver Stage 3 indicator layer so the Stage 2 dataset layer can materialize and reload versioned indicator frames through `pandas_ta_classic`.
- In Scope: `product-plane` only; `src/trading_advisor_3000/product_plane/research/indicators/**`, targeted indicator tests, and this task note.
- Out of Scope: derived features, MTF feature alignment beyond base indicator partitions, vectorbt execution, ranking/projection, Dagster orchestration, and unrelated infrastructure remediation.
- Constraints: compute indicators only from `research_bar_views`; keep calculations point-in-time safe; preserve deterministic column naming; emit reloadable `research_indicator_frames` keyed by `dataset_version + indicator_set_version`; keep prior Phase 1/2 slices working.
- Done Evidence: green Stage 3 unit/integration indicator tests, no regressions in adjacent Phase 1/2 tests, `python scripts/validate_task_request_contract.py`, and `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: reproducibility and explicit indicator contracts matter more than shortest implementation.

## Current Delta
- Session started for Stage 3 continuation on top of the finished dataset layer.
- Confirmed the local runtime actually exposes the required `pandas_ta_classic` functions for the Stage 3 minimum set.
- Expanded the indicator profile from a naming skeleton into a real minimum coverage contract.
- Implemented materialization and reload of `research_indicator_frames` with metadata fields (`source_bars_hash`, `row_count`, `warmup_span`, `created_at`).
- Added point-in-time, contract-shape, and reload tests for the new indicator layer.
- Verified the new indicator slice through loop gate without breaking prior Stage 1/2 test seams.

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
- Validate the indicator slice through loop gate and then use these materialized frames as the source boundary for Stage 4 derived features.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_indicator_layer.py -q`
- `python -m pytest tests/product-plane/integration/test_research_indicator_materialization.py -q --basetemp .pytest_tmp/research_stage3`
- `python -m pytest tests/product-plane/unit/test_research_dataset_layer.py -q`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
