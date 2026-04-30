# Task Note
Updated: 2026-04-29 18:24 UTC

## Goal
- Deliver: Fix continuous-front indicator formula gate, prefix leakage, ts_close, native derived rules, and runtime lineage evidence

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Fixed the continuous-front indicator gates and roll-aware calculation contract.
- Formula sample gate now covers the TZ minimum indicator/derived classes.
- Prefix invariance rebuilds from roll-centered prefix cuts and compares hashes, null masks, and cross-window evidence.
- `ts_close`/watermark now use actual bar close time, native derived outputs use the native volume/OI group, and lineage runtime evidence is fail-closed.

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
- Final Contexts: continuous-front indicators, base indicator materialization, derived indicator materialization, Dagster research asset wiring.
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
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators -q --basetemp=.tmp/pytest-cf-indicators-full-v4`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_can_source_indicators_from_continuous_front -q --basetemp=.tmp/pytest-cf-dagster-v3`
- `py -3.11 -m pytest tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py -q --basetemp=.tmp/pytest-indicator-derived-units-v3`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_bootstrap_pipeline.py::test_research_dataset_materialization_supports_continuous_front_mode -q --basetemp=.tmp/pytest-cf-bootstrap-v3`
- `py -3.11 -m pytest tests/product-plane/contracts/test_continuous_front_contracts.py tests/product-plane/unit/test_continuous_front.py tests/product-plane/unit/test_continuous_front_spark_job.py -q --basetemp=.tmp/pytest-cf-regression-v3`
- `py -3.11 -m compileall src/trading_advisor_3000/product_plane/research/continuous_front_indicators src/trading_advisor_3000/product_plane/research/indicators src/trading_advisor_3000/product_plane/research/derived_indicators`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
