# Task Note
Updated: 2026-04-30 10:08 UTC

## Goal
- Deliver: Expand continuous-front formula gate to validate all base indicators and all derived indicators

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Formula sample gate now derives its required coverage from the full active base and derived profiles.
- Current coverage is 84 base outputs plus 118 derived outputs, including MTF derived outputs that are expected to remain NULL without source timeframes.
- The focused test asserts `checked_columns_count == required_columns_count == 202`.

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
- Decision Quality: correct_after_replan
- Final Contexts: CTX-RESEARCH, CTX-CONTRACTS, CTX-ARCHITECTURE
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: validator
- Improvement Artifact: tests/product-plane/continuous_front_indicators/test_rules_and_projection.py

## Blockers
- No blocker.

## Next Step
- Ready for review.

## Validation
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators/test_rules_and_projection.py -q --basetemp=.tmp/pytest-cf-all-formulas-v2`
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators -q --basetemp=.tmp/pytest-cf-indicators-full-all-formulas-v1`
- `py -3.11 -m pytest tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py -q --basetemp=.tmp/pytest-indicator-derived-units-all-formulas-v1`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_can_source_indicators_from_continuous_front -q --basetemp=.tmp/pytest-cf-dagster-all-formulas-v1`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_bootstrap_pipeline.py::test_research_dataset_materialization_supports_continuous_front_mode -q --basetemp=.tmp/pytest-cf-bootstrap-all-formulas-v1`
- `py -3.11 -m pytest tests/product-plane/contracts/test_continuous_front_contracts.py tests/product-plane/unit/test_continuous_front.py tests/product-plane/unit/test_continuous_front_spark_job.py -q --basetemp=.tmp/pytest-cf-regression-all-formulas-v1`
- `py -3.11 -m compileall src/trading_advisor_3000/product_plane/research/continuous_front_indicators src/trading_advisor_3000/product_plane/research/indicators src/trading_advisor_3000/product_plane/research/derived_indicators`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
