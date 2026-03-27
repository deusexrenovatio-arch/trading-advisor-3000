# Phase 2 Delta Runtime Runbook

## Goal
Prove that Phase 2 data/research outputs are physical Delta tables, not manifest-only artifacts.
Spark execution proof is tracked separately in `docs/runbooks/app/phase2-spark-execution-runbook.md`.

## Preconditions
1. Python env is prepared from repository root:
   - `python -m pip install -e .[dev]`
2. Source fixtures are available:
   - `tests/app/fixtures/data_plane/raw_backfill_sample.jsonl`
   - `tests/app/fixtures/research/canonical_bars_sample.jsonl`

## Validation Commands
Run phase-scoped checks:

```bash
python -m pytest tests/app/integration/test_phase2a_data_plane.py -q
python -m pytest tests/app/integration/test_phase2b_research_plane.py -q
```

Expected evidence:
- each output path is a Delta directory (`*.delta`);
- each directory contains `_delta_log`;
- integration tests read rows back through Delta runtime APIs.

## Disprover
The data-plane integration suite includes a disprover:
- remove a physical parquet file from a Delta output while keeping `_delta_log` and manifest metadata;
- verify runtime read fails.

This disprover is enforced by:
- `tests/app/integration/test_phase2a_data_plane.py::test_sample_backfill_disprover_fails_when_physical_delta_data_is_deleted`
