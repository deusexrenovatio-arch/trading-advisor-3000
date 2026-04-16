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

## Pin Command
Run from repository root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/pin_moex_baseline_storage.ps1 `
  -RunId 20260409T162421Z
```

## What The Baseline Layout Contains
Raw root:
- `raw_moex_history.delta`

Canonical root:
- `baseline-manifest.json`
- `README.md`
- `reports/route-refresh-report.json`
- `reports/canonical-refresh-report.json`
- `canonical_bars.delta`
- `canonical_bar_provenance.delta`

Derived root placeholders:
- `features/`
- `indicators/`

The retained baseline is materialized directly into the data root. It must not depend on junction-style links back to historical source runs.

## Consumer Rule
Downstream readers must use these stable data-root paths:
- `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current/raw_moex_history.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bars.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bar_provenance.delta`

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
