# MOEX Money Math Runbook

## Scope

Money math is a product-plane data route. It refreshes contract economics side
tables only:

- `raw/economics/raw_moex_contract_securities.delta`
- `raw/economics/raw_moex_indicative_fx_rates.delta`
- `raw/economics/raw_moex_rms_limits.delta`
- `raw/economics/raw_moex_rms_staticparams.delta`
- `canonical/economics/canonical_fx_rates.delta`
- `canonical/economics/canonical_asset_risk_parameters.delta`
- `canonical/economics/canonical_contract_economics.delta`

It does not reload or replace historical bar tables.

## One-Time Bootstrap

Use this when the side tables do not exist yet. The default source pulls all
four raw inputs from MOEX ISS: contract specifications, indicative FX, RMS
limits, and RMS static parameters.

```powershell
python scripts/run_moex_contract_economics_update.py `
  --mode bootstrap `
  --trade-date 2026-06-11 `
  --canonical-session-calendar-path D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_session_calendar.delta `
  --moex-timeout-seconds 45 `
  --moex-max-retries 5 `
  --moex-retry-backoff-seconds 1.25 `
  --moex-retry-jitter-ratio 0.2
```

`bootstrap` is intentionally a first-load mode. If any raw side table already
exists, it stops instead of overwriting it. Use `--allow-bootstrap-overwrite`
only for an intentional audited rebuild into a known target root.

Use the JSONL mode only for controlled recovery or audited import of RMS
snapshots:

```powershell
python scripts/run_moex_contract_economics_update.py `
  --mode bootstrap `
  --trade-date 2026-06-11 `
  --canonical-session-calendar-path D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_session_calendar.delta `
  --rms-source jsonl `
  --rms-limits-jsonl D:/TA3000-data/imports/moex/rms-limits.jsonl `
  --rms-staticparams-jsonl D:/TA3000-data/imports/moex/rms-staticparams.jsonl
```

The command writes the raw side tables, then materializes canonical contract
economics through the Spark/Delta job.

The canonical interval rule is as-of, not per-bar duplication: source economics
for trading day `D` becomes effective for the next trading session found in
`canonical_session_calendar`. If no calendar path is supplied, the standalone
job falls back to the next calendar day and records that fallback in
`source_flags_json`.

For a multi-day or historical economics backfill, use an explicit date range and
date-stamped contract snapshots:

```powershell
python scripts/run_moex_contract_economics_update.py `
  --mode bootstrap `
  --date-from 2026-06-10 `
  --date-till 2026-06-15 `
  --contracts-jsonl D:/TA3000-data/imports/moex/contract-securities-snapshots.jsonl `
  --canonical-session-calendar-path D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_session_calendar.delta
```

The MOEX ISS contract securities endpoint is a current-snapshot endpoint. The
script refuses multi-day backfill without `--contracts-jsonl` so a historical
store cannot be silently filled with today's contract specification snapshot.
FX and RMS inputs can still be pulled from MOEX ISS by date range; audited JSONL
inputs are available for recovery and source preservation. When JSONL inputs are
used with `--date-from` / `--date-till`, the updater only imports rows whose
date stamp belongs to that explicit window. Undated JSONL rows are accepted only
for a single-day run.

The job fails closed when required inputs are missing: `MINSTEP`, `LOTVOLUME`,
FX rate, `MR1`, or `LASTSETTLEPRICE`.

Every standalone run writes MOEX request events under the evidence root:

- `moex-request-log.jsonl`;
- `moex-request.latest.json`.

If the live ISS request fails before raw tables are written, these files are the
first diagnostic artifact to inspect. They include attempted URL context,
timeout, retry count, retry sleep, error type, and final status.

## Regular Update

The regular route is the Dagster `moex_baseline_update_job`. Its generated run
config enables:

```yaml
economics_mode: refresh
raw_economics_root: <baseline-root>/raw/economics
canonical_economics_root: <baseline-root>/canonical/economics
```

For standalone regular refresh of only the money-math side tables, use
`--mode update`. This replaces only rows for the refreshed `trade_date` in raw
side tables, then merges canonical rows by their declared business keys:

```powershell
python scripts/run_moex_contract_economics_update.py `
  --mode update `
  --trade-date 2026-06-11 `
  --raw-economics-root D:/TA3000-data/trading-advisor-3000-nightly/raw/economics `
  --canonical-economics-root D:/TA3000-data/trading-advisor-3000-nightly/canonical/economics `
  --canonical-session-calendar-path D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_session_calendar.delta `
  --moex-timeout-seconds 45 `
  --moex-max-retries 5
```

The refresh order is:

1. raw bar tail update
2. money-math raw side table refresh from MOEX ISS
3. canonical bar refresh
4. continuous-front and research-bar catch-up reporting

Manual baseline execution uses the same mode:

```powershell
python scripts/run_moex_baseline_update.py --economics-mode refresh
```

The baseline update report publishes MOEX request evidence in `artifact_paths`
as `moex_request_log` and `moex_request_latest`. Use these paths first when a
regular refresh fails before economics raw side tables are written.

Use `--economics-mode skip` only for controlled recovery when side tables are
known to be unavailable and the run is not meant to update execution economics.
