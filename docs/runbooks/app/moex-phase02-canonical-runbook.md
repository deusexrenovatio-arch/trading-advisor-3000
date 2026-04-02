# MOEX Phase-02 Canonical Runbook

## Purpose
Execute `Этап 2 - Canonical` for MOEX historical contour with deterministic resampling, fail-closed QC, and contract/runtime compatibility evidence.

This runbook covers:
- canonical/resampling build for runtime timeframes `5m`, `15m`, `1h`, `4h`, `1d`, `1w`,
- mandatory QC gates (`unique key`, `monotonic timeline`, `OHLC validity`, `provenance completeness`),
- contract compatibility check against `canonical_bar.v1`,
- runtime hot-path decoupling proof (no Spark dependency in runtime modules).

## Preconditions
1. Python env is bootstrapped (`python -m pip install -e .[dev]`).
2. Phase-01 artifacts already exist with raw Delta output:
   - `artifacts/codex/moex-phase01/<run_id>/delta/raw_moex_history.delta`
3. `canonical_bar.v1` contract files are present in source tree.

## Execute Canonical Proof
Run from repository root:

```bash
python scripts/run_moex_phase02_canonical.py \
  --phase01-root artifacts/codex/moex-phase01 \
  --output-root artifacts/codex/moex-phase02
```

Optional explicit source binding:

```bash
python scripts/run_moex_phase02_canonical.py \
  --raw-table-path artifacts/codex/moex-phase01/<run_id>/delta/raw_moex_history.delta \
  --output-root artifacts/codex/moex-phase02 \
  --run-id 20260402T120000Z
```

The runner is fail-closed: it exits non-zero when QC/contract/runtime checks fail and blocks publish.

## Required Artifacts
Per run folder (`artifacts/codex/moex-phase02/<run_id>/`):
- `phase02-canonical-report.json`
- `canonical-snapshot.json`
- `resampling-snapshot.json`
- `qc-report.json`
- `contract-compatibility-report.json`
- `runtime-decoupling-proof.json`
- `delta/canonical_bars.delta/` (with `_delta_log`) on PASS
- `delta/canonical_bar_provenance.delta/` (with `_delta_log`) on PASS

## Fail-Closed Signals
Treat Phase-02 as blocked when any condition appears:
1. `qc-report.json` has `status=FAIL` or `publish_decision=blocked`.
2. `contract-compatibility-report.json` has `status=FAIL`.
3. `runtime-decoupling-proof.json` has `status=FAIL`.
4. `phase02-canonical-report.json` has `status=BLOCKED`.

## Operator Notes
- `canonical_bar.v1` payload must remain unchanged; provenance stays in `canonical_bar_provenance` technical table.
- Runtime timeframe policy currently includes `5m/15m/1h/4h/1d/1w`; any further enum expansion requires versioned contract change.
- This runbook proves only canonical/resampling contour closure and does not claim reconciliation or final release readiness.
