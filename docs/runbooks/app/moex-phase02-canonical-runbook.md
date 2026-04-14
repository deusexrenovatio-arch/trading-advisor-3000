# MOEX Phase-02 Canonical Runbook

## Purpose
Execute `Этап 2 - Canonical` for MOEX historical contour with deterministic resampling, fail-closed QC, and contract/runtime compatibility evidence.

This runbook covers:
- canonical/resampling build for runtime timeframes `5m`, `15m`, `1h`, `4h`, `1d`, `1w`,
- changed-window scoped canonical recompute driven by `raw_ingest_run_report.v2`,
- deterministic parity manifests and parity reports for the governed proof window set,
- mandatory QC gates (`unique key`, `monotonic timeline`, `OHLC validity`, `provenance completeness`),
- contract compatibility check against `canonical_bar.v1`,
- runtime hot-path decoupling proof (no Spark dependency in runtime modules),
- explicit `PASS-NOOP` without canonical-table mutation when `changed_windows=[]`.

Route role:
- This is a legacy migration artifact for canonical proof and local diagnostics.
- It is not the target-state scheduled historical route and it is not the canonical operator-facing path after route consolidation.
- The active governed module is `moex-historical-route-consolidation`; this runbook remains bounded migration evidence inside that module only.
- After Phase-03 Dagster cutover, use this script only as a bounded rebuild/repair aid behind Dagster-owned route governance, not as the operator's primary overnight path.

## Preconditions
1. Python env is bootstrapped (`python -m pip install -e .[dev]`).
2. Phase-01 artifacts already exist with raw Delta output:
   - `artifacts/codex/moex-phase01/<run_id>/delta/raw_moex_history.delta`
   - `artifacts/codex/moex-phase01/<run_id>/raw-ingest-report.pass1.json` or an equivalent `raw_ingest_run_report.v2` input
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
  --raw-ingest-report-path artifacts/codex/moex-phase01/<run_id>/raw-ingest-report.pass1.json \
  --output-root artifacts/codex/moex-phase02 \
  --run-id 20260402T120000Z
```

The runner is fail-closed: it exits non-zero when QC/contract/runtime checks fail and blocks publish.
`PASS-NOOP` is valid only when the input report has `changed_windows=[]`; in that case existing canonical tables must remain byte-for-byte unchanged.

## Accepted Baseline Storage
The current accepted retained baseline is pinned into:
- raw: `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current`
- canonical: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`

That storage is promoted from a successful nightly run and exposes stable Delta paths for downstream consumers:
- `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current/raw_moex_history.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bars.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bar_provenance.delta`

Promote a successful run with:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/pin_moex_baseline_storage.ps1 `
  -RunId 20260409T162421Z
```

Use the stable baseline paths for downstream reads instead of binding phase-03+ jobs to whichever `moex-phase02/<run_id>` folder happened to be created last.

For the explicit job-routing decision, see:
- `docs/architecture/product-plane/moex-historical-route-decision.md`

## Required Artifacts
Per run folder (`artifacts/codex/moex-phase02/<run_id>/`):
- `phase02-canonical-report.json`
- `changed-window-set-manifest.json`
- `raw-parity-report.json`
- `canonical-parity-report.json`
- `canonical-snapshot.json`
- `resampling-snapshot.json`
- `qc-report.json`
- `contract-compatibility-report.json`
- `runtime-decoupling-proof.json`
- `delta/canonical_bars.delta/` (with `_delta_log`) on PASS
- `delta/canonical_bar_provenance.delta/` (with `_delta_log`) on PASS

## Fail-Closed Signals
Treat Phase-02 as blocked when any condition appears:
1. `raw-parity-report.json` has `status=FAIL`.
2. `canonical-parity-report.json` has `status=FAIL`.
3. `qc-report.json` has `status=FAIL` or `publish_decision=blocked`.
4. `contract-compatibility-report.json` has `status=FAIL`.
5. `runtime-decoupling-proof.json` has `status=FAIL`.
6. `phase02-canonical-report.json` has `status=BLOCKED`.
7. `phase02-canonical-report.json` claims `PASS-NOOP` while `mutation_applied=true`.

## Operator Notes
- `canonical_bar.v1` payload must remain unchanged; provenance stays in `canonical_bar_provenance` technical table.
- Runtime timeframe policy currently includes `5m/15m/1h/4h/1d/1w`; any further enum expansion requires versioned contract change.
- The phase-02 input contract is `raw_ingest_run_report.v2`; canonical recompute scope must come from its governed `changed_windows`, not from ad hoc full-table heuristics.
- `changed-window-set-manifest.json` is the machine-readable proof boundary for parity and `PASS-NOOP`.
- Once the Dagster cutover contour is accepted, scheduled nightly, repair, and backfill operations must run through the Dagster-owned cutover job; direct CLI use of this script is then limited to explicit bounded repair flows.
- This runbook proves only canonical/resampling contour closure and does not claim reconciliation or final release readiness.
- Under the active route-consolidation planning truth, this script is retained only as a migration artifact until final cleanup and must not be presented as the steady-state operator route.
