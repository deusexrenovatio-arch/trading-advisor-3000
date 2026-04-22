# MOEX Canonical-Refresh Runbook

## Purpose
Execute the MOEX Spark canonical-refresh step for historical refresh with deterministic resampling, fail-closed QC, and contract/runtime compatibility evidence.

This runbook covers:
- Spark canonical/resampling build for runtime timeframes `5m`, `15m`, `1h`, `4h`, `1d`, `1w`,
- mandatory QC gates (`unique key`, `monotonic timeline`, `OHLC validity`, `provenance completeness`),
- contract compatibility check against `canonical_bar.v1`,
- runtime hot-path decoupling proof with no Spark dependency in runtime modules.

## Preconditions
1. Python env is bootstrapped (`python -m pip install -e .[dev]`).
2. Raw-ingest artifacts already exist with raw Delta output:
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-raw-ingest/<run_id>/delta/raw_moex_history.delta`
3. `canonical_bar.v1` contract files are present in source tree.

## Execute
Run from repository root:

```bash
export TA3000_MOEX_HISTORICAL_DATA_ROOT=/absolute/path/outside/repo

python scripts/run_moex_canonical_refresh.py \
  --raw-ingest-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-raw-ingest \
  --output-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh
```

Optional explicit source binding:

```bash
python scripts/run_moex_canonical_refresh.py \
  --raw-table-path $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-raw-ingest/<run_id>/delta/raw_moex_history.delta \
  --output-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh \
  --run-id 20260402T120000Z
```

The runner is fail-closed: it exits non-zero when QC, contract, or runtime checks fail and blocks publish.

Operator route note:
- the active canonical-refresh engine is Spark,
- Python remains only the orchestration/report layer around Spark execution, not the resampling engine itself.

## Required Artifacts
Per run folder (`$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh/<run_id>/`):
- `canonical-refresh-report.json`
- `canonical-snapshot.json`
- `resampling-snapshot.json`
- `qc-report.json`
- `contract-compatibility-report.json`
- `runtime-decoupling-proof.json`
- `delta/canonical_bars.delta/` with `_delta_log` on PASS
- `delta/canonical_bar_provenance.delta/` with `_delta_log` on PASS

## Fail-Closed Signals
Treat canonical refresh as blocked when any condition appears:
1. `qc-report.json` has `status=FAIL` or `publish_decision=blocked`.
2. `contract-compatibility-report.json` has `status=FAIL`.
3. `runtime-decoupling-proof.json` has `status=FAIL`.
4. `canonical-refresh-report.json` has `status=BLOCKED`.

## Operator Notes
- `canonical_bar.v1` payload must remain unchanged; provenance stays in `canonical_bar_provenance` technical table.
- Runtime timeframe policy currently includes `5m/15m/1h/4h/1d/1w`; any further enum expansion requires a versioned contract change.
- This runbook proves only canonical-refresh closure and does not claim reconciliation or final release readiness.
