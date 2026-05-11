# Task Note
Updated: 2026-05-10 08:00 UTC

## Goal
- Deliver: MOEX raw Spark Delta review follow-up: source_rows_path staging, fail-closed scope matching, chunked reconcile deletes, remove legacy Python helpers

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: integration test evidence via Docker Spark proof for `source_rows_path`, scoped fail-closed matching, and raw Delta reconcile behavior.
- Shortcut Waiver: none

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Foundation stages MOEX source rows to run-scoped JSONL and calls Spark with `source_rows_path`.
- Raw Spark ingest fail-closes source rows outside declared key/time-window scopes.
- Affected-window deletes stream chunked predicates via `toLocalIterator()` instead of driver `collect()`.
- Legacy Python raw signature/read/delete helpers are removed from foundation after reference check.

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
- Final Contexts: CTX-OPS, CTX-DATA, data-integration-closure, Docker Spark integration.
- Route Match: expanded
- Primary Rework Cause: architecture_gap
- Incident Signature: Python source-row materialization and soft scope matching remained in the raw ingest update path.
- Improvement Action: architecture
- Improvement Artifact: source_rows_path staging, fail-closed scope matching, chunked reconcile deletes, and legacy helper cleanup.

## Blockers
- No blocker.

## Next Step
- Ready for review or PR preparation.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/unit/test_moex_raw_ingest_spark_delta_contract.py tests/product-plane/unit/test_moex_baseline_update.py tests/product-plane/contracts/test_moex_historical_foundation_contracts.py tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py -q`
- `docker exec ta3000-dagster-test-webserver sh -lc "rm -rf /tmp/ta3000-pytest-raw-all /tmp/ta3000-pytest-cache && python -m pytest tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py -q --basetemp=/tmp/ta3000-pytest-raw-all -o cache_dir=/tmp/ta3000-pytest-cache"`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
