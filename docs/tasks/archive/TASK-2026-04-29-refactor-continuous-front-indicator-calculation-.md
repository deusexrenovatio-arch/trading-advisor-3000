# Task Note
Updated: 2026-04-29 16:44 UTC

## Goal
- Deliver: Refactor continuous-front indicator calculation to pandas-ta-classic owner

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Replaced the active continuous-front indicator refresh route with a pandas-ta-classic/pandas calculation job.
- Removed active Spark calculation naming from indicator contracts, docs, config, Dagster wiring, and tests.
- Verified that the job computes sidecar outputs directly from `research_bar_views` plus the adjustment ladder instead of reading precomputed indicator tables.

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
- Final Contexts: CTX-RESEARCH, CTX-CONTRACTS, CTX-ARCHITECTURE, CTX-ORCHESTRATION
- Route Match: expanded
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: architecture
- Improvement Artifact: src/trading_advisor_3000/product_plane/research/continuous_front_indicators/pandas_job.py

## Blockers
- No blocker.

## Next Step
- Close task session after loop gate acceptance.

## Validation
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators/test_rules_and_projection.py -q --basetemp=.tmp/pytest-cf-pandas-job-rules`
- `py -3.11 -m compileall src\trading_advisor_3000\product_plane\research\continuous_front_indicators src\trading_advisor_3000\dagster_defs\research_assets.py`
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators/test_legacy_contract_compatibility.py -q --basetemp=.tmp/pytest-cf-legacy-compat-pandas`
- `py -3.11 -m pytest tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py -q --basetemp=.tmp/pytest-cf-existing-pandas`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_can_source_indicators_from_continuous_front -q --basetemp=.tmp/pytest-cf-dagster-pandas`
- `py -3.11 -m pytest tests/product-plane/unit/test_continuous_front.py tests/product-plane/unit/test_continuous_front_spark_job.py tests/product-plane/unit/test_research_dataset_layer.py tests/product-plane/continuous_front_indicators/test_rules_and_projection.py tests/product-plane/continuous_front_indicators/test_legacy_contract_compatibility.py -q --basetemp=.tmp/pytest-cf-pandas-regression`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
