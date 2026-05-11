# Task Note
Updated: 2026-05-10 07:59 UTC

## Goal
- Deliver: Make MOEX historical update path use Spark/Delta-native raw bootstrap and raw-Delta canonical Spark input

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, synthetic upstream, scaffold-only
- Closure Evidence: integration test coverage plus test Dagster proof over the canonical dataset contour; verification staging evidence includes raw/canonical/provenance Delta `_delta_log` row counts and `spark_input_mode=raw_delta`.
- Shortcut Waiver: none

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Raw bootstrap delegates hot raw Delta diff/merge/write to the Spark/Delta ingest job.
- Canonical Spark route reads raw Delta directly with changed-window control input instead of normalized source JSONL.
- Test Dagster verification run proved the direct raw-Delta canonical route on isolated verification staging.
- Review P1 fix: raw Spark ingest now reconciles affected windows with scoped Delta delete plus append, so target rows missing from source are removed.
- Review P1 fix: raw Spark ingest now uses deterministic Spark-side row fingerprints over source timestamp, provider metadata, OHLCV, open_interest, and stable provenance fields.
- Review follow-up: foundation stages MOEX source rows into a run-scoped JSONL source file and passes `source_rows_path` to Spark instead of materializing the refresh payload as a Python list.
- Review follow-up: raw Spark ingest now fail-closes source rows that do not match declared key and time-window scopes before any Delta write.
- Review follow-up: affected-window reconciliation streams window predicates with chunked `toLocalIterator()` deletes instead of collecting the full window set on the driver.
- Review follow-up: unused legacy Python raw signature/read/delete helpers were removed from the foundation route after reference check.

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
- Final Contexts: PLAN_delta_spark phase 1, native runtime ownership, MOEX staging split, targeted pytest, test Dagster verification staging, loop gate.
- Route Match: expanded
- Primary Rework Cause: workflow_gap
- Incident Signature: Python hot-table mutation and normalized JSONL Spark input still existed inside the phase-1 path.
- Improvement Action: workflow
- Improvement Artifact: Spark/Delta-native raw bootstrap, raw-Delta canonical input, scoped raw reconcile, and fingerprint diff recorded with verification staging evidence.
- Closed At: 2026-05-08T15:58:31Z

## Blockers
- No blocker.

## Next Step
- Ready for review or PR preparation.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/unit/test_moex_baseline_update.py tests/product-plane/unit/test_moex_canonical_route.py tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py -q`
- `python -m pytest tests/product-plane/unit/test_moex_raw_ingest_spark_delta_contract.py tests/product-plane/unit/test_moex_baseline_update.py tests/product-plane/unit/test_moex_canonical_route.py tests/product-plane/contracts/test_moex_historical_foundation_contracts.py tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py -q`
- `docker exec ta3000-dagster-test-webserver python -m pytest tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py -q --basetemp=/tmp/ta3000-pytest-raw-all -o cache_dir=/tmp/ta3000-pytest-cache`
- `python -m pytest tests/product-plane/unit/test_moex_baseline_update.py -k "raw_window_updates_only_scoped_rows_without_full_table_read or bootstrap_raw_window_uses_spark_delta_hot_table_runtime" tests/product-plane/unit/test_moex_raw_ingest_spark_delta_contract.py -q`
- `python -m pytest tests/product-plane/unit/test_moex_raw_ingest_spark_delta_contract.py tests/product-plane/unit/test_moex_baseline_update.py tests/product-plane/contracts/test_moex_historical_foundation_contracts.py tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py -q`
- `docker exec ta3000-dagster-test-webserver sh -lc "rm -rf /tmp/ta3000-pytest-raw-all /tmp/ta3000-pytest-cache && python -m pytest tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py -q --basetemp=/tmp/ta3000-pytest-raw-all -o cache_dir=/tmp/ta3000-pytest-cache"`
