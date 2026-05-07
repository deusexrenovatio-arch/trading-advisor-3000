# Task Note
Updated: 2026-05-07 11:05 UTC

## Goal
- Deliver: Wire nightly continuous-front refresh through Dagster product-plane definitions

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Added product-plane Dagster definitions to load baseline schedule, research sensor, and downstream research jobs in the staging runtime.
- Switched scheduled continuous-front refresh to the accepted calendar-expiry policy and added Spark-native roll-map handling for that policy.
- Rebuilt `dagster-staging` and verified the live schedule/sensor plus an isolated Docker Spark proof against `D:/TA3000-data`.

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
- Final Contexts: CTX-ORCHESTRATION, CTX-DATA
- Route Match: matched product-plane runtime and data-plane proof route.
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: added regression tests for product-plane Dagster definitions, scheduled policy selection, campaign schema, and calendar-expiry Spark behavior.
- Improvement Artifact: PR branch `codex/continuous-front-nightly-wiring`.

## Blockers
- No blocker.

## Next Step
- Open PR and review CI.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `py -3.11 -m pytest tests/product-plane/unit/test_continuous_front_spark_job.py tests/product-plane/unit/test_research_campaign_runner.py tests/product-plane/integration/test_product_plane_dagster_definitions.py tests/product-plane/integration/test_research_dagster_jobs.py::test_research_definitions_expose_product_jobs_and_moex_success_sensor tests/product-plane/integration/test_research_dagster_jobs.py::test_scheduled_research_refresh_uses_calendar_expiry_policy tests/product-plane/integration/test_research_dagster_jobs.py::test_scheduled_research_refresh_policy_can_be_overridden_from_env tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_can_source_indicators_from_continuous_front -q --basetemp=.tmp/pytest-nightly-wiring-full-focused`
- Docker proof: `continuous-front-nightly-wiring-20260507T110500Z`, status PASS, `_delta_log` present for all four continuous-front tables.
