# Task Note
Updated: 2026-04-29 12:42 UTC

## Goal
- Deliver: Repair continuous-front P0 as-of adjustment and remove non-target Spark route surface

## Task Request Contract
- Objective: close P0 where static continuous-front bars used future roll gaps as research truth.
- In Scope: Spark continuous-front bars/ladder, indicator as-of adjustment, tests, and acceptance docs.
- Out of Scope: direct `main` integration, production promotion, 5m scope, and Windows Hadoop DLL install.
- Constraints: no Python continuous-front materializer route; current active contract prices stay native.
- Done Evidence: focused/broad pytest, validators, loop gate, PR gate, and explicit Spark runtime blocker.
- Priority Rule: TZ causality and acceptance semantics over fast closure.

## Current Delta
- `continuous_front_bars` now emits native active OHLC and zero row-level additive offset.
- Spark ladder keeps the backward current-anchor gap for downstream as-of computation.
- Indicator materialization applies ladder by current roll epoch and requires ladder rows for rolled series.
- Added tests for as-of SMA behavior and missing ladder rejection.
- Removed the Spark report surface that described a Python route as merely disabled.
- Updated acceptance docs with the point-in-time ladder repair and validation evidence.

## First-Time-Right Report
1. Confirmed coverage: P0 static adjusted bar route is removed from Spark output and indicator path.
2. Missing or risky scenarios: full Spark Delta proof still needs a provisioned runtime.
3. Resource/time risks and chosen controls: kept repair to Spark, indicator, tests, and docs.
4. Highest-priority fixes or follow-ups: run loop/PR gates after this task note update.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: point-in-time ladder application and Spark/report surface.
- Next Probe: run full Spark Delta proof in a provisioned runtime.

## Task Outcome
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-RESEARCH, CTX-CONTRACTS, CTX-OPS
- Route Match: expanded
- Primary Rework Cause: none
- Incident Signature: continuous_front_static_future_adjustment
- Improvement Action: test
- Improvement Artifact: docs/tasks/active/continuous-front-acceptance-problems-2026-04-29.md

## Blockers
- Full local Spark Delta proof remains blocked by Windows Hadoop NativeIO before data processing.

## Next Step
- Run full Spark Delta proof in a provisioned Spark runtime before target acceptance.

## Validation
- `py -3.11 -m pytest tests/product-plane/unit/test_research_indicator_layer.py -q --basetemp=.tmp/pytest-cf-asof-indicators`
- `py -3.11 -m pytest tests/product-plane/unit/test_continuous_front_spark_job.py -q --basetemp=.tmp/pytest-cf-asof-spark`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_indicator_materialization.py -q --basetemp=.tmp/pytest-cf-asof-indicator-materialization`
- `py -3.11 -m pytest tests/product-plane/contracts/test_continuous_front_contracts.py tests/product-plane/unit/test_continuous_front.py tests/product-plane/unit/test_research_dataset_layer.py tests/product-plane/unit/test_continuous_front_spark_job.py tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py tests/product-plane/unit/test_research_campaign_runner.py tests/product-plane/unit/test_research_dagster_manifests.py tests/product-plane/unit/test_research_backtest_layer.py tests/product-plane/unit/test_research_backtest_engine_semantics.py tests/product-plane/integration/test_research_vectorbt_backtests.py tests/product-plane/integration/test_research_dagster_jobs.py -q --basetemp=.tmp/pytest-cf-asof-broad-rerun`
- `py -3.11 -m pytest tests/product-plane/integration/test_historical_data_spark_execution.py -q --basetemp=.tmp/pytest-cf-asof-historical-spark`
- `py -3.11 scripts/validate_task_request_contract.py`
- `py -3.11 scripts/validate_session_handoff.py`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `py -3.11 scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
