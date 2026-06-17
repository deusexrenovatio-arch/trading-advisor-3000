# MOEX Baseline Storage Runbook

## Purpose
Pin one successful MOEX raw+canonical run into the authoritative data-root layout and expose stable storage paths for downstream consumers.

This runbook exists so the team does not treat "latest rerun" as equivalent to "accepted baseline".
This file is the storage truth source for MOEX historical data paths.

## Authoritative Data Root
- Data root: `D:/TA3000-data/trading-advisor-3000-nightly`
- Raw root: `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current`
- Canonical root: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- Derived root: `D:/TA3000-data/trading-advisor-3000-nightly/derived/moex`

As of `2026-04-10`, the accepted pinned run is:
- `20260409T162421Z`

## Promotion Rule
Only a run with:
- `route-refresh-report.json` status `PASS`
- canonical-refresh `publish_decision = publish`
- stable raw-ingest and canonical-refresh Delta outputs

may be promoted into the data-root baseline layout.

Later refresh attempts are non-authoritative until they are explicitly re-pinned.

## Daily Baseline Update Rule
After the baseline is pinned, routine updates must use the baseline updater, not per-run candidate folders:

```bash
export TA3000_MOEX_HISTORICAL_DATA_ROOT=D:/TA3000-data/trading-advisor-3000-nightly

python scripts/run_moex_baseline_update.py \
  --refresh-window-days 7 \
  --contract-discovery-lookback-days 45 \
  --refresh-overlap-minutes 180 \
  --max-changed-window-days 10
```

The same code path is used by Dagster job `moex_baseline_update_job` and schedule `moex_baseline_daily_update_schedule`.
The updater writes directly to the stable raw/canonical baseline paths and stores evidence under `moex-baseline-update/<run_id>/`.

## Pin Command
Run from repository root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/pin_moex_baseline_storage.ps1 `
  -RunId 20260409T162421Z
```

## What The Baseline Layout Contains
Raw root:
- `raw_moex_history.delta`

Raw Delta physical layout:
- partition columns: `ts_close_year`
- `ts_close_year` is a storage/layout column derived from `ts_close`
- logical raw consumers must continue to key rows by `internal_id`, `timeframe`, `moex_secid`, `ts_open`, `ts_close`
- `source_interval` remains a MOEX request/provenance column, not a raw identity or watermark key
- daily Spark raw ingest writes the same partitioned layout after the migration is promoted

Canonical root:
- `baseline-manifest.json`
- `README.md`
- `reports/route-refresh-report.json`
- `reports/canonical-refresh-report.json`
- `canonical_bars.delta`
- `canonical_bar_provenance.delta`
- `canonical_session_calendar.delta`
- `canonical_roll_map.delta`

Derived root placeholders:
- `features/`
- `indicators/`

The retained baseline is materialized directly into the data root. It must not depend on junction-style links back to historical source runs.

## Raw Layout Migration Procedure
The raw layout migration is a one-time controlled Spark rewrite, not a second raw-ingest route.

Stage the new layout into a migration run folder:

```bash
export TA3000_MOEX_HISTORICAL_DATA_ROOT=D:/TA3000-data/trading-advisor-3000-nightly

python scripts/run_moex_raw_layout_migration.py stage \
  --run-id 20260617T120000Z
```

The stage command:
- reads the stable `raw_moex_history.delta`
- writes `moex-raw-layout-migration/<run_id>/raw_moex_history.layout-staged.delta`
- writes `raw-layout-migration-report.json`
- fails closed unless row count, distinct raw key count, duplicate-key count, watermark comparison, schema, partition columns, file profile, and `_delta_log` checks pass

Promote only after the report status is `PASS`:

```bash
python scripts/run_moex_raw_layout_migration.py promote \
  --run-id 20260617T120000Z \
  --staged-table-path D:/TA3000-data/trading-advisor-3000-nightly/moex-raw-layout-migration/20260617T120000Z/raw_moex_history.layout-staged.delta \
  --report-path D:/TA3000-data/trading-advisor-3000-nightly/moex-raw-layout-migration/20260617T120000Z/raw-layout-migration-report.json
```

Promotion moves the current stable raw table to a backup path named like
`raw_moex_history.pre-layout-<run_id>.delta`, then moves the staged table into the stable
`raw_moex_history.delta` path. The backup is rollback material only; downstream readers must keep using the stable raw path.

## Consumer Rule
Downstream readers must use these stable data-root paths:
- `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current/raw_moex_history.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bars.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bar_provenance.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_session_calendar.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_roll_map.delta`

Do not bind research, reconciliation, or later refresh jobs to the newest `moex-raw-ingest`, `moex-canonical-refresh`, or `moex-route-refresh` run folder by default.

## Derived Data Rule
All downstream computed layers must stay under the same data root:
- features: `D:/TA3000-data/trading-advisor-3000-nightly/derived/moex/features`
- indicators: `D:/TA3000-data/trading-advisor-3000-nightly/derived/moex/indicators`

Do not hide derived datasets back under repo-local `artifacts/`, `docs/`, or worktree-like folders.

## Retention Rule
After a successful baseline is pinned:
1. Keep the stable raw root and canonical root in the data root.
2. Keep derived root placeholders even if they are still empty.
3. Remove superseded failed reruns and worktree-like debug roots once diagnostics are no longer needed.
4. Treat the data-root layout, not an ad hoc rerun folder, as the stable operator-facing entrypoint.
