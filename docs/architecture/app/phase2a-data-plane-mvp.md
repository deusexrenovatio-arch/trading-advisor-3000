# Phase 2A - Data Plane MVP

## Goal
Deliver a runnable data-plane baseline:
- ingestion from sample backfill,
- canonical bars builder,
- data-quality gates,
- physical Delta table write/read path with `_delta_log`,
- executable Dagster definitions with a local materialization proof profile,
- Spark execution proof profile.

## Deliverables
- `src/trading_advisor_3000/app/data_plane/ingestion/backfill.py`
- `src/trading_advisor_3000/app/data_plane/canonical/builder.py`
- `src/trading_advisor_3000/app/data_plane/canonical/quality.py`
- `src/trading_advisor_3000/app/data_plane/delta_runtime.py`
- `src/trading_advisor_3000/app/data_plane/schemas/delta.py`
- `src/trading_advisor_3000/app/data_plane/pipeline.py`
- `src/trading_advisor_3000/dagster_defs/phase2a_assets.py`
- `scripts/run_phase2a_dagster_proof.py`
- `src/trading_advisor_3000/spark_jobs/canonical_bars_job.py`
- `tests/app/fixtures/data_plane/raw_backfill_sample.jsonl`
- `tests/app/integration/test_phase2a_data_plane.py`
- `tests/app/integration/test_phase2a_dagster_execution.py`
- `tests/app/unit/test_phase2a_builder.py`
- `tests/app/unit/test_phase2a_quality.py`
- `tests/app/unit/test_phase2a_manifests.py`

## Design Decisions
1. Ingestion validates shape and types before canonical conversion.
2. Canonical builder deduplicates by `(contract_id, timeframe, ts)` and keeps the row with max `ts_close`.
3. Canonical bars conform to spec shape: `instrument_id`, single `ts`, and `open_interest`.
4. Quality gate enforces whitelist membership and monotonic timeline.
5. Delta schemas are represented as a deterministic manifest in code and used to write physical Delta tables.
6. Data-plane outputs are materialized as local Delta directories (`*.delta`) with `_delta_log` and runtime read-back.
7. Dagster closure is tracked with executable `Definitions` and a local materialization proof path (`scripts/run_phase2a_dagster_proof.py`) for the same canonical phase2a slice.
8. Dagster asset ownership is explicit: `raw_market_backfill` materializes only raw Delta output, while each canonical asset materializes its own Delta table so partial selection cannot pass through hidden side effects.
9. Spark closure is tracked with an executable proof path (`scripts/run_phase2a_spark_proof.py`) that runs Spark in a Linux/Docker proof profile and validates Spark-written Delta outputs against the manifest contract.

## Acceptance Commands
- `python -m pytest tests/app/integration/test_phase2a_data_plane.py -q`
- `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -q`
- `python -m pytest tests/app/integration/test_phase2a_spark_execution.py -q`
- `python -m pytest tests/app/unit/test_phase2a_quality.py -q`
- `python -m pytest tests/app/unit/test_phase2a_manifests.py -q`
- `python scripts/run_phase2a_dagster_proof.py --source tests/app/fixtures/data_plane/raw_backfill_sample.jsonl --output-dir .tmp/phase2a-dagster-proof --contracts BR-6.26,Si-6.26 --output-json artifacts/phase2a-dagster-proof.json`
- `python -m pytest tests/app -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`

## Out of Scope
- production data sources and scheduling,
- full distributed Spark orchestration, advanced Dagster scheduling/deployment, and cluster tuning,
- feature/research/runtime/execution phases beyond 2A.
