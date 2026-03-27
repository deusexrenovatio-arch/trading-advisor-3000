# Phase 2 Spark Execution Runbook

## Goal
Prove that the canonical Spark slice executes in a stable Docker/Linux profile and writes contract-valid Delta outputs through Spark itself.

## Preconditions
1. Docker Desktop / Docker Engine is available.
2. Docker can build the shared Linux phase-proof image from:
   - `deployment/docker/phase-proofs/Dockerfile`
3. Source fixture exists:
   - `tests/app/fixtures/data_plane/raw_backfill_sample.jsonl`

## Proof Execution
Run the canonical Spark proof entrypoint:

```bash
python scripts/run_phase2a_spark_proof.py \
  --source tests/app/fixtures/data_plane/raw_backfill_sample.jsonl \
  --output-dir .tmp/phase2a-spark-proof \
  --contracts BR-6.26,Si-6.26 \
  --output-json artifacts/phase2a-spark-proof.json
```

Default behavior:
- the script builds or reuses the Linux proof image;
- runs Spark inside Docker, not on the Windows host;
- writes Delta outputs from Spark itself (`DataFrame.write.format("delta")`).

Expected report characteristics:
- `source_rows=4`, `whitelisted_rows=3`, `canonical_rows=2`;
- Delta outputs exist for `raw_market_backfill`, `canonical_bars`, `canonical_instruments`, `canonical_contracts`, `canonical_session_calendar`, `canonical_roll_map`;
- contract check errors list is empty.

## Validation Commands
```bash
python -m pytest tests/app/integration/test_phase2a_spark_execution.py -q
python scripts/validate_stack_conformance.py
```

## Disprover
The Spark integration suite contains a negative proof:
- keep SQL-plan builders intact;
- break Spark bootstrap factory;
- assert execution fails.

Enforced by:
- `tests/app/integration/test_phase2a_spark_execution.py::test_phase2a_spark_disprover_fails_when_runtime_bootstrap_is_broken`
