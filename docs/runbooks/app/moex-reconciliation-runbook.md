# MOEX Reconciliation Runbook

## Purpose
Execute cross-source Finam vs MOEX reconciliation with threshold-governed, fail-closed decisions.

This runbook covers:
- Finam archive snapshot ingest with source timestamp and latency-class metadata,
- automated overlap comparison for `close`, `volume`, `missing bars`, and `lag class`,
- versioned threshold policy and escalation simulation artifacts,
- persisted reconciliation metrics table for replay and query use.

Route role:
- This is an auxiliary verification contour for reconciliation proof and local diagnostics.
- It is not the scheduled historical route and it is not the canonical overnight operator path.
- The active governed route stays `Dagster -> Python raw ingest -> Spark canonical refresh`.

## Mandatory Finam Metadata
Each Finam snapshot row must provide explicit non-empty fields:
- `source_ts_utc`
- `received_at_utc`
- `archive_batch_id`
- `source_provider`
- `source_binding`

Alias-only payloads such as `source_timestamp_utc`, `archived_at_utc`, or implicit defaults are rejected.

## Preconditions
1. Python env is bootstrapped (`python -m pip install -e .[dev]`).
2. Canonical-refresh artifacts exist with canonical bars and provenance:
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh/<run_id>/delta/canonical_bars.delta`
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh/<run_id>/delta/canonical_bar_provenance.delta`
3. Finam archive snapshot source is prepared as `.json`, `.csv`, or Delta table.
4. Threshold policy exists:
   - `configs/moex_phase03/reconciliation_thresholds.v1.yaml`

## Execute
Run from repository root:

```bash
export TA3000_MOEX_HISTORICAL_DATA_ROOT=/absolute/path/outside/repo

python scripts/run_moex_reconciliation.py \
  --canonical-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh \
  --canonical-run-id 20260402T124500Z \
  --finam-archive-source-path /absolute/path/outside/repo/moex-reconciliation-input/finam-archive-20260402.json \
  --output-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-reconciliation \
  --run-id 20260402T150000Z
```

Optional explicit canonical bindings:

```bash
python scripts/run_moex_reconciliation.py \
  --canonical-bars-path $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh/<run_id>/delta/canonical_bars.delta \
  --canonical-provenance-path $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh/<run_id>/delta/canonical_bar_provenance.delta \
  --finam-archive-source-path /absolute/path/outside/repo/moex-reconciliation-input/finam-archive-20260402.json \
  --output-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-reconciliation
```

The runner is fail-closed and exits non-zero when hard threshold gates block publish.

## Required Artifacts
Per run folder (`$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-reconciliation/<run_id>/`):
- `reconciliation-report.json`
- `finam-archive-ingest-report.json`
- `finam-archive-provenance.json`
- `overlap-metrics.json`
- `alert-simulation.json`
- `escalation-trace.json`
- `delta/finam_archive_snapshots.delta/` with `_delta_log`
- `delta/reconciliation_metrics.delta/` with `_delta_log`

## Fail-Closed Signals
Treat reconciliation as blocked when any condition appears:
1. `reconciliation-report.json` has `status=BLOCKED`.
2. Any mandatory dimension gate fails (`close_drift_bps`, `volume_drift_ratio`, `missing_bars_ratio`, `lag_class`).
3. Alert simulation is missing or not executable while threshold policy requires escalation evidence.
4. Reconciliation metrics table is missing (`delta/reconciliation_metrics.delta/_delta_log`).
5. `finam-archive-provenance.json` is missing or does not include capture fingerprint plus binding/provider evidence.

## Operator Notes
- Threshold changes are policy-controlled via `reconciliation_thresholds.v1.yaml`; versioning must stay explicit.
- `allow-degraded-publish` is opt-in and must be accompanied by incident and escalation evidence. Do not allow a warning-only silent downgrade.
- This runbook proves only reconciliation closure and does not claim operations readiness or final release readiness.
- This script remains a bounded verification tool and must not be presented as the steady-state overnight operator route.
