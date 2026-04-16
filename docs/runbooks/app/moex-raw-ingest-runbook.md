# MOEX Raw-Ingest Runbook

## Purpose
Execute the MOEX raw-ingest step for historical refresh with bounded, replayable evidence.

This runbook covers:
- versioned universe + mapping validation,
- MOEX candleborders coverage discovery,
- multi-contract chain discovery per instrument from MOEX history snapshots,
- bootstrap ingest into raw Delta output with native source granularity (`1m`/`10m`/`1h`/`1d`/`1w`),
- idempotent rerun proof where the second pass adds zero rows.

## Preconditions
1. Python env is bootstrapped (`python -m pip install -e .[dev]`).
2. Network access to `https://iss.moex.com` is available.
3. Raw-ingest configs are present:
   - `configs/moex_phase01/universe/moex-futures-priority.v1.yaml`
   - `configs/moex_phase01/instrument_mapping_registry.v1.yaml`

## Execute
Run from repository root:

```bash
export TA3000_MOEX_HISTORICAL_DATA_ROOT=/absolute/path/outside/repo

python scripts/run_moex_raw_ingest.py \
  --timeframes 5m,15m,1h,4h,1d,1w \
  --bootstrap-window-days 1461 \
  --expand-contract-chain \
  --contract-discovery-step-days 14 \
  --refresh-overlap-minutes 180 \
  --stability-lag-minutes 20
```

The runner executes two passes with the same `ingest_till_utc` and fails if rerun idempotency is broken.
`stability_lag_minutes` must stay positive on live markets to exclude still-forming bars from proof windows.
`refresh_overlap_minutes` keeps small near-watermark refetches for corrections while avoiding full historical reloads on every rerun.

## Required Artifacts
Per run folder (`$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-raw-ingest/<run_id>/`):
- `coverage-report.pass1.json`
- `coverage-report.pass2.json`
- `coverage-report.pass1.csv`
- `coverage-report.pass2.csv`
- `raw-ingest-report.pass1.json`
- `raw-ingest-report.pass2.json`
- `raw-ingest-summary-report.json`
- `delta/raw_moex_history.delta/` with `_delta_log`

## Fail-Closed Signals
Treat raw ingest as blocked when any condition appears:
1. Duplicate active mapping keys are detected by validator.
2. Any required source interval (`1/10/60/24/7`, mapped from requested runtime targets `5m/15m/1h/4h/1d/1w`) has no candleborders coverage.
3. `raw-ingest-summary-report.json` shows `idempotent_rerun=false`.
4. `raw-ingest-report.pass2.json` has `incremental_rows > 0` for the same ingest window.

## Operator Notes
- Mapping changes must be versioned and soft-deactivated. Do not hard-delete historical rows.
- Raw table `timeframe` stores native MOEX source timeframe labels. Runtime targets (`5m/15m/1h/4h/1d/1w`) stay a downstream resampling concern.
- This runbook proves only the raw-ingest step and does not claim canonical refresh, Dagster-route proof, or reconciliation closure.
