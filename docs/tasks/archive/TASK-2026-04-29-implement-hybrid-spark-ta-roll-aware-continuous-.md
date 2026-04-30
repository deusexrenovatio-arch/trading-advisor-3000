# Task Note
Updated: 2026-04-29 15:33 UTC

## Goal
- Deliver: Implement hybrid Spark/TA roll-aware continuous-front indicator and derived-indicator storage contour

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Implemented the continuous-front indicator rule registry, roll-aware input projection, governed sidecar storage, QC/manifest/acceptance outputs, and Dagster data-prep integration.
- Adapted base and derived indicator materialization so continuous-front calculations use explicit roll groups instead of implicit legacy behavior.
- Added architecture/runbook documentation and focused tests for rule coverage, projection semantics, sidecar Delta outputs, and existing data-prep compatibility.

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
- Final Contexts: CTX-RESEARCH, CTX-DATA, CTX-ORCHESTRATION, CTX-CONTRACTS
- Route Match: expanded
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: docs/tasks/active/TASK-2026-04-29-implement-hybrid-spark-ta-roll-aware-continuous-.md

## Blockers
- No blocker.

## Next Step
- Close the task session after the loop gate accepts the completed outcome.

## Validation
- `py -3.11 -m compileall src\trading_advisor_3000\product_plane\research\continuous_front_indicators src\trading_advisor_3000\product_plane\research\indicators\materialize.py src\trading_advisor_3000\product_plane\research\derived_indicators\materialize.py src\trading_advisor_3000\dagster_defs\research_assets.py`
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators/test_rules_and_projection.py -q --basetemp=.tmp/pytest-cf-indicator-rules`
- `py -3.11 -m pytest tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py -q --basetemp=.tmp/pytest-cf-indicator-existing`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_can_source_indicators_from_continuous_front -q --basetemp=.tmp/pytest-cf-indicator-dagster`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_materializes_dataset_indicator_and_derived_layers_only -q --basetemp=.tmp/pytest-cf-indicator-dagster-contract`
- `py -3.11 -m pytest tests/product-plane/unit/test_continuous_front.py tests/product-plane/unit/test_continuous_front_spark_job.py tests/product-plane/unit/test_research_dataset_layer.py tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_can_source_indicators_from_continuous_front -q --basetemp=.tmp/pytest-cf-indicator-continuous`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
