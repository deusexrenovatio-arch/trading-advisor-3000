# MOEX Phase-03 Reconciliation Runbook

## Purpose
Execute `Этап 3 - Reconciliation` for cross-source Finam vs MOEX overlap with threshold-governed, fail-closed decisions.

This runbook covers:
- Finam archive snapshot ingest with source timestamp and latency-class metadata,
- automated overlap comparison for `close`, `volume`, `missing bars`, `lag class`,
- versioned threshold policy and escalation simulation artifacts,
- persisted reconciliation metrics table for replay/query use.

Route role:
- This is a legacy migration artifact for reconciliation proof and local diagnostics.
- It is not the target-state scheduled historical route and it is not the canonical operator-facing path after route consolidation.
- The active governed module is `moex-historical-route-consolidation`; this runbook remains bounded migration evidence inside that module only.

## Mandatory Finam Metadata (Fail-Closed)
Each Finam snapshot row must provide explicit non-empty fields:
- `source_ts_utc`
- `received_at_utc`
- `archive_batch_id`
- `source_provider`
- `source_binding`

Alias-only payloads (for example `source_timestamp_utc`, `archived_at_utc`, or implicit defaults) are rejected.

## Preconditions
1. Python env is bootstrapped (`python -m pip install -e .[dev]`).
2. Canonicalization artifacts exist with canonical bars and provenance:
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase02/<run_id>/delta/canonical_bars.delta`
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase02/<run_id>/delta/canonical_bar_provenance.delta`
3. Finam archive snapshot source is prepared as `.json`, `.csv`, or Delta table.
4. Threshold policy exists:
   - `configs/moex_phase03/reconciliation_thresholds.v1.yaml`

## Execute Reconciliation Proof
Run from repository root:

```bash
export TA3000_MOEX_HISTORICAL_DATA_ROOT=/absolute/path/outside/repo

python scripts/run_moex_phase03_reconciliation.py \
  --phase02-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase02 \
  --phase02-run-id 20260402T124500Z \
  --finam-archive-source-path /absolute/path/outside/repo/moex-phase03-input/finam-archive-20260402.json \
  --output-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase03 \
  --run-id 20260402T150000Z
```

Optional explicit canonical bindings:

```bash
python scripts/run_moex_phase03_reconciliation.py \
  --canonical-bars-path $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase02/<run_id>/delta/canonical_bars.delta \
  --canonical-provenance-path $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase02/<run_id>/delta/canonical_bar_provenance.delta \
  --finam-archive-source-path /absolute/path/outside/repo/moex-phase03-input/finam-archive-20260402.json \
  --output-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase03
```

The runner is fail-closed and exits non-zero when hard threshold gates block publish.

## Required Artifacts
Per run folder (`$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-phase03/<run_id>/`):
- `phase03-reconciliation-report.json`
- `finam-archive-ingest-report.json`
- `finam-archive-provenance.json`
- `overlap-metrics.json`
- `alert-simulation.json`
- `escalation-trace.json`
- `delta/finam_archive_snapshots.delta/` (with `_delta_log`)
- `delta/reconciliation_metrics.delta/` (with `_delta_log`)

## Fail-Closed Signals
Treat Phase-03 as blocked when any condition appears:
1. `phase03-reconciliation-report.json` has `status=BLOCKED`.
2. Any mandatory dimension gate fails (`close_drift_bps`, `volume_drift_ratio`, `missing_bars_ratio`, `lag_class`).
3. Alert simulation is missing or not executable while threshold policy requires escalation evidence.
4. Reconciliation metrics table is missing (`delta/reconciliation_metrics.delta/_delta_log`).
5. `finam-archive-provenance.json` is missing or does not include capture fingerprint plus binding/provider evidence.

## Operator Notes
- Threshold changes are policy-controlled via `reconciliation_thresholds.v1.yaml`; change versioning must stay explicit.
- `allow-degraded-publish` is opt-in and must be accompanied by incident/escalation evidence (no warning-only silent downgrade).
- This runbook proves only reconciliation contour closure and does not claim phase-04 operations hardening or final release readiness.
- Under the active route-consolidation planning truth, this script is retained only as a migration artifact until final cleanup and must not be presented as the steady-state operator route.
