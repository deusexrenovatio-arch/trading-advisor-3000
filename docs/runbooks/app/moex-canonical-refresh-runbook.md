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

## Frozen Raw Session Input

`moex_data_rebuild_job` with `profile_name=canonical_from_existing_raw`
accepts `raw_session_schedule_path` and the required pinned
`raw_session_schedule_version`. In `staging_only` mode, it calls the existing
session normalizer before canonical refresh and writes
`canonical/delta/canonical_session_intervals.delta` relative to the selected
run layout. The exact output is `<canonical_output_dir>/delta/canonical_session_intervals.delta`.
Use `mapping_registry_path` to bind the registry used for the frozen snapshot.
Set `downstream_mode=none` to disable downstream research; canonical
materialization still runs.

The input Delta table remains unchanged. Unmapped SECIDs, overlapping paths,
missing versions, existing interval targets and production prewrites are rejected.
Source policy IDs and document hashes are preserved; normalization does not
certify a reconstructed calendar or bypass canonical QC. The input receipt is
`<canonical_output_dir>/session-input-report.json`, including when later QC fails.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only closure
- Closure Evidence: real frozen Delta version -> existing session normalizer -> existing canonical job; regression tests and run-specific reports.
- Shortcut Waiver: none

This connects an existing normalization primitive inside the existing Dagster
job. Price transforms remain Spark-owned; Delta owns the session metadata
read/write. This is partial closure, not full target closure: downstream
research and runtime-ready surface admission remain gated by their own evidence.


## Explicit Closed Dates

Frozen-session preparation also writes
`<canonical_output_dir>/delta/canonical_closed_session_dates.delta` and passes it
through the existing Dagster canonical asset and Spark runner. This optional
input has one row per `(instrument_id, session_date)` with the original policy,
source ID and source document hash. It contains only explicit `closed` records.
No trading interval is fabricated for a closed date.

Weekly coverage requires an interval or an explicit closed-date record for
every spanned calendar date. Missing records remain blocking. A weekly source
span containing only closed dates, conflicting open/closed evidence, duplicate
closed keys and missing provenance fail before bar materialization. Daily and
intraday source rows still require trading intervals, including on dates marked
closed. Existing callers without the optional input retain fail-closed coverage.

Extending calendar history requires dated source evidence and a new frozen raw
snapshot. The accepted raw input remains unchanged; a later/current timetable
must not silently backfill historical dates. Reconstructed source limitations
remain visible after normalization. The existing Solution Intent remains staged:
calendar coverage proof is distinct from a successful full canonical run.

For MOEX ISS weekly candles (`source_provider=moex_iss`, `1w`, interval `10080`),
the source `end` timestamp labels the last included trading date. Calendar
coverage includes that Moscow date even when the label is midnight. Other
providers and daily/intraday inputs retain the existing exclusive-end rule.
Raw timestamps and source provenance are preserved. Regression evidence covers
the midnight boundary and provider/timeframe isolation; dated ISS weekly/daily
comparisons belong in the run's source receipts.

The published W1 session calendar keeps the source period's Moscow start date
as its key, including a closed Monday. Its open/close bounds aggregate the
actual official trading intervals across the included source period. Every
spanned date must have open or explicit closed evidence; unknown dates and
fully closed periods block publication. Daily/intraday bounds and roll-map
selection remain unchanged. The optional closed-date input is passed through
both local and Docker publish runners.
