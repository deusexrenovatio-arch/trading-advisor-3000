# TA3000 Production Nightly Runbook

## Purpose
Run the MOEX baseline nightly update from a dedicated production checkout while
keeping durable market data outside the repository checkout.

This is an operating runbook for the Windows nightly contour. It does not make
the product plane live-trading ready and does not authorize intraday decisions
from historical or batch MOEX data.

## Runtime Layout
- Production checkout: `D:/TA3000-production`
- Production branch: `ta3000-production`
- Data root: `D:/TA3000-data/trading-advisor-3000-nightly`
- Launcher: `C:/Users/Admin/run_ta3000_production_nightly.cmd`
- Product staging bootstrap: `scripts/run_ta3000_product_staging_bootstrap.cmd`
- Log file: `D:/TA3000-data/logs/ta3000-production-nightly.log`
- Current scheduler entry: `TA3000-MOEX-Nightly-Backfill`

The scheduler entry may keep its historical name until a separate scheduler
rename is approved, but its action must point to the production launcher above.
If local permissions block the action update, the old launcher path may remain
only as a compatibility shim that calls the production launcher and does not run
the retired route.

Preferred elevated scheduler update:

```powershell
SCHTASKS /Change /TN "TA3000-MOEX-Nightly-Backfill" /TR "C:\Users\Admin\run_ta3000_production_nightly.cmd"
```

## Branch Policy
`ta3000-production` is a runtime branch, not a development branch.

- Do not commit feature work directly on `ta3000-production`.
- Do not develop or test experimental changes in `D:/TA3000-production`.
- Promote only already-verified `main` into `ta3000-production`.
- Keep `main -> ta3000-production` promotion manual until a separate decision
  defines cadence and ownership.

## Data-Root Separation
`D:/TA3000-production` and
`D:/TA3000-data/trading-advisor-3000-nightly` must stay separate.

The scheduler must not turn the data root into a git checkout. It must not clean
or recreate these data-root folders as part of launcher startup:

- `raw`
- `canonical`
- `research`
- `staging`
- `verification`
- `moex-baseline-update`

The production checkout may contain ordinary runtime/cache traces produced by
Python, git, Docker, Dagster, or local tooling. Durable market data and nightly
evidence remain under the data root.

## Production Launcher Contract
The Windows launcher is a bootstrapper, not the owner of the data job. It must:

1. verify `D:/TA3000-production/.git` exists;
2. switch to `D:/TA3000-production`;
3. fetch `origin`;
4. checkout `ta3000-production`;
5. pull `origin/ta3000-production` with `--ff-only`;
6. set process-local Git `safe.directory` for `D:/TA3000-production`, because
   the scheduled task runs as `SYSTEM` while the checkout is owned by `Admin`;
7. call `scripts/run_ta3000_product_staging_bootstrap.cmd`;
8. append output to `D:/TA3000-data/logs/ta3000-production-nightly.log`.

The launcher must not call the MOEX baseline updater through host Python. That
command path is reserved for development/manual diagnostics. The production
nightly data job is owned by the Dagster daemon running in product staging.

## Product Staging Bootstrap
Run from `D:/TA3000-production` after the launcher has pulled
`ta3000-production`:

```cmd
scripts\run_ta3000_product_staging_bootstrap.cmd
```

The bootstrap script starts or refreshes the `dagster-product-staging` compose
project, binds `/workspace` to `D:/TA3000-production`, binds
`/ta3000-data/moex-historical` to
`D:/TA3000-data/trading-advisor-3000-nightly`, waits for
`ta3000-dagster-webserver` health, verifies `ta3000-dagster-daemon`, and checks
that `moex_baseline_daily_update_schedule` and `moex_baseline_update_job` are
available.

Dagster daemon owns the actual nightly run through:

- schedule: `moex_baseline_daily_update_schedule`;
- job: `moex_baseline_update_job`;
- cron: `0 2 * * *`;
- timezone: `Europe/Moscow`.

The baseline updater writes to the stable raw/canonical baseline paths and
stores run evidence under `moex-baseline-update/<run_id>/` inside the data root.

## Retired Launcher
`C:/Users/Admin/run_moex_nightly_backfill.cmd` is retired for production use.

Keep it on disk temporarily for forensic reference, but detach it from the
active Windows scheduled task. It must not use
`D:/TA3000-data/trading-advisor-3000-nightly` as a git checkout and must not be
the normal nightly entrypoint.

If an elevated scheduler update is unavailable, preserve the retired command in
`C:/Users/Admin/run_moex_nightly_backfill.retired-YYYYMMDD.cmd` and leave
`C:/Users/Admin/run_moex_nightly_backfill.cmd` only as a compatibility shim that
attempts to update the scheduled-task action, then delegates to
`C:/Users/Admin/run_ta3000_production_nightly.cmd`. If the self-update is denied,
the shim must continue through the production launcher and must not run the
retired route.

## Operator Checks
Before treating the contour as active:

```powershell
Test-Path D:/TA3000-production/.git
Test-Path D:/TA3000-data/trading-advisor-3000-nightly/.git
git -C D:/TA3000-production branch --show-current
```

Expected state:

- `D:/TA3000-production/.git` exists.
- `D:/TA3000-data/trading-advisor-3000-nightly/.git` does not exist.
- `git -C D:/TA3000-production branch --show-current` returns
  `ta3000-production`.
- The scheduled-task action points to
  `C:/Users/Admin/run_ta3000_production_nightly.cmd`, or the historical action
  path is only a compatibility shim delegating to that launcher.

After a manual smoke run, check:

- `D:/TA3000-data/logs/ta3000-production-nightly.log` has a fresh entry;
- `docker ps` shows `ta3000-dagster-webserver` and `ta3000-dagster-daemon`;
- `ta3000-dagster-webserver` is healthy;
- the product staging container reports `moex_baseline_daily_update_schedule`
  and `moex_baseline_update_job`;
- after the Dagster nightly tick, a fresh evidence folder exists under
  `D:/TA3000-data/trading-advisor-3000-nightly/moex-baseline-update/<run_id>`;
- if MOEX is unavailable, record it as an external connectivity failure, not as
  a cleanup or data-root failure.

## Repository Validation
The old session/handoff lifecycle is retired in this repository. Do not recreate
that validator for this production contour. Use the current workflow proof:

```powershell
python scripts/validate_docs_links.py --roots AGENTS.md docs
python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none
python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none
```
