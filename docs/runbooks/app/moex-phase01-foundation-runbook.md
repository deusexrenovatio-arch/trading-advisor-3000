# MOEX Phase-01 Foundation Runbook

## Purpose
Execute `Этап 1 - Foundation` for MOEX historical ingest scope with bounded, replayable evidence.

This runbook covers:
- versioned universe + mapping validation,
- MOEX candleborders coverage discovery,
- multi-contract chain discovery per instrument from MOEX history snapshots,
- bootstrap ingest into raw Delta output with native source granularity (`1m`/`1h`/`1d`/`1w`),
- chunked MOEX request scheduling with request-level logs for timeout/disconnect diagnostics,
- idempotent rerun proof (second pass adds zero rows).

Route role:
- This is a legacy migration artifact for bootstrap-era proof and local diagnostics.
- It is not the target-state scheduled historical route and it is not the canonical operator-facing path after route consolidation.
- The active governed module is `moex-historical-route-consolidation`; this runbook remains bounded migration evidence inside that module only.

## Preconditions
1. Python env is bootstrapped (`python -m pip install -e .[dev]`).
2. Network access to `https://iss.moex.com` is available.
3. Phase-01 configs are present:
   - `configs/moex_phase01/universe/moex-futures-priority.v1.yaml`
   - `configs/moex_phase01/instrument_mapping_registry.v1.yaml`

## Execute Foundation Proof
Run from repository root:

```bash
python scripts/run_moex_phase01_foundation.py \
  --output-root artifacts/codex/moex-phase01 \
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
Per run folder (`artifacts/codex/moex-phase01/<run_id>/`):
- `coverage-report.pass1.json`
- `coverage-report.pass2.json`
- `coverage-report.pass1.csv`
- `coverage-report.pass2.csv`
- `raw-ingest-report.pass1.json`
- `raw-ingest-report.pass2.json`
- `phase01-foundation-report.json`
- `delta/raw_moex_history.delta/` (with `_delta_log`)
- `moex-request-log.jsonl`
- `moex-request.latest.json`
- `raw-ingest-progress.jsonl`
- `raw-ingest-progress.latest.json`
- `raw-ingest-errors.jsonl` and `raw-ingest-error.latest.json` when a coverage item fails

## Fail-Closed Signals
Treat Phase-01 as blocked when any condition appears:
1. Duplicate active mapping keys are detected by validator.
2. Any required source interval (`1/60/24/7`, mapped from requested runtime targets `5m/15m/1h/4h/1d/1w`) has no candleborders coverage.
3. `phase01-foundation-report.json` shows `idempotent_rerun=false`.
4. `raw-ingest-report.pass2.json` has `incremental_rows > 0` for the same ingest window.
5. `raw-ingest-error.latest.json` or shard-level `shard-error.json` shows unresolved request/disconnect failures.

## Accepted Baseline Storage
For official retained storage, do not treat the latest run folder as authoritative by default.

The accepted 4-year baseline is pinned into:
- raw: `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current`
- canonical: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`

Pin a successful run into the data-root baseline layout with:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/pin_moex_baseline_storage.ps1 `
  -RunId 20260409T162421Z
```

The baseline storage exposes stable Delta paths:
- `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current/raw_moex_history.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bars.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bar_provenance.delta`

Downstream consumers must bind to the stable raw/canonical baseline paths, not to the newest rerun folder under `moex-phase01`, `moex-phase02`, or `moex-nightly`.

For the explicit job-routing decision, see:
- `docs/architecture/product-plane/moex-historical-route-decision.md`

## Operator Notes
- Mapping changes must be versioned and soft-deactivated (no hard deletion of historical rows).
- Raw table `timeframe` stores native MOEX source timeframe labels; runtime targets (`5m/15m/1h/4h/1d/1w`) remain a downstream resampling concern.
- `15m` is built from `1m`; `10m` is no longer an authoritative raw source interval in this contour.
- This runbook proves only the foundation contour (`moex_history_ingest_contour`) and does not claim canonical/resampling or reconciliation closure.
- Under the active route-consolidation planning truth, this script is retained only as a migration artifact until final cleanup and must not be presented as the steady-state operator route.
