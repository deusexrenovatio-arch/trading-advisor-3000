# Phase 2A - Data Plane MVP

## Goal
Deliver a runnable data-plane baseline:
- ingestion from sample backfill,
- canonical bars builder,
- data-quality gates,
- Delta schema manifest,
- initial Dagster/Spark skeleton artifacts.

## Deliverables
- `src/trading_advisor_3000/app/data_plane/ingestion/backfill.py`
- `src/trading_advisor_3000/app/data_plane/canonical/builder.py`
- `src/trading_advisor_3000/app/data_plane/canonical/quality.py`
- `src/trading_advisor_3000/app/data_plane/schemas/delta.py`
- `src/trading_advisor_3000/app/data_plane/pipeline.py`
- `src/trading_advisor_3000/dagster_defs/phase2a_assets.py`
- `src/trading_advisor_3000/spark_jobs/canonical_bars_job.py`
- `tests/app/fixtures/data_plane/raw_backfill_sample.jsonl`
- `tests/app/integration/test_phase2a_data_plane.py`
- `tests/app/unit/test_phase2a_builder.py`
- `tests/app/unit/test_phase2a_quality.py`
- `tests/app/unit/test_phase2a_manifests.py`

## Design Decisions
1. Ingestion validates shape and types before canonical conversion.
2. Canonical builder deduplicates by `(contract_id, timeframe, ts)` and keeps the row with max `ts_close`.
3. Canonical bars conform to spec shape: `instrument_id`, single `ts`, and `open_interest`.
4. Quality gate enforces whitelist membership and monotonic timeline.
5. Delta schemas are represented as a deterministic manifest in code.
6. Dagster/Spark artifacts are delivered as executable skeletons without external runtime dependency.

## Acceptance Commands
- `python -m pytest tests/app/integration/test_phase2a_data_plane.py -q`
- `python -m pytest tests/app/unit/test_phase2a_quality.py -q`
- `python -m pytest tests/app/unit/test_phase2a_manifests.py -q`
- `python -m pytest tests/app -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`

## Out of Scope
- production data sources and scheduling,
- full Delta/Spark runtime wiring,
- feature/research/runtime/execution phases beyond 2A.
