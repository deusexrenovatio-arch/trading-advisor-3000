# Phase2 Dagster Execution Runbook

## Purpose
Provide an executable proof path for Dagster orchestration on the canonical phase2a data slice.

This runbook is phase-scoped.
It documents phase-05 (D3) closure evidence for local materialization on the agreed phase2a outputs and does not claim full Dagster platform closure.

## Preconditions
1. Install project dependencies, including `dagster`.
2. Keep the source fixture available at `tests/app/fixtures/data_plane/raw_backfill_sample.jsonl`.
3. Use an isolated output directory (default: `.tmp/phase2a-dagster-proof`).

## Proof Command
Run the canonical proof entrypoint:

```bash
python scripts/run_phase2a_dagster_proof.py --source tests/app/fixtures/data_plane/raw_backfill_sample.jsonl --output-dir .tmp/phase2a-dagster-proof --contracts BR-6.26,Si-6.26 --output-json artifacts/phase2a-dagster-proof.json
```

Expected result:
1. JSON report is printed and written to `artifacts/phase2a-dagster-proof.json`.
2. `success` is `true`.
3. Delta outputs contain `_delta_log` for:
   - `raw_market_backfill`
   - `canonical_bars`
   - `canonical_instruments`
   - `canonical_contracts`
   - `canonical_session_calendar`
   - `canonical_roll_map`

## Validation Tests
Run phase-scoped validation:

```bash
python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -q
python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k metadata_only -q
python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k partial_selection -q
python -m pytest tests/app/unit/test_phase2a_manifests.py -q
```

The integration suite covers:
1. successful materialization for the selected phase2a slice.
2. partial selection behavior where selecting one canonical asset only materializes that asset plus required upstream dependencies.
3. a disprover path (missing source fixture) that must fail closed.
4. a metadata-only disprover path that monkeypatches static `AssetSpec` placeholders and must fail with `metadata-only or incomplete`.

## Fail-Closed Disprover Contract
Use this command to verify that static metadata-only replacements cannot pass phase closure:

```bash
python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k metadata_only -q
```

Expected result:
1. the test passes because the runtime rejects metadata-only definitions.
2. failure text contains `metadata-only or incomplete` and `phase2a_materialization_job`.

Partial-selection ownership check:
1. run `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k partial_selection -q`.
2. expected behavior: `canonical_bars` selection materializes only `raw_market_backfill` and `canonical_bars`.
3. no `_delta_log` should appear for `canonical_instruments`, `canonical_contracts`, `canonical_session_calendar`, or `canonical_roll_map` in the partial-selection output directory.

## Troubleshooting
1. If materialization fails with missing fixture path, verify `--source` points to a real JSONL file.
2. If Dagster import fails, reinstall dependencies from `pyproject.toml`.
3. If metadata-only disprover fails unexpectedly, check `assert_phase2a_definitions_executable(...)` is still called before materialization.
4. If Delta outputs are missing `_delta_log`, rerun proof with a clean output directory and inspect JSON report `output_paths`.
