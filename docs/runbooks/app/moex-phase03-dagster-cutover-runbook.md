# MOEX Phase-03 Dagster Cutover Runbook

## Purpose
Collect real staging Dagster evidence for the governed MOEX Phase-03 cutover contour and bind it into the canonical Phase-03 runner without downgrading the proof class.

This runbook covers:
- two governed nightly cycles on the canonical Dagster-owned route,
- repair and backfill runs through the same Dagster job,
- one recovery drill from the same staging owner path,
- creation of `staging-binding-report.json` for the canonical Phase-03 runner,
- packaging immutable evidence so governed acceptance can stay at `staging-real`.

This runbook does not claim final Phase-03 acceptance by itself. It only closes the operational bridge between real staging Dagster runs and the Phase-03 evidence package.

## Why This Exists
Phase-03 code and local tests already prove the route shape, single-writer discipline, and fail-closed recovery logic.

The remaining blocker is stricter:
- local `integration` proof is not enough;
- acceptance requires `staging-real`;
- the Phase-03 runner already supports `--staging-binding-report-path`, but the repo previously had no explicit operator flow for building that report from real Dagster evidence.

This runbook is that missing flow.

## Preconditions
1. A staging Dagster deployment is reachable from the host where this runbook is executed.
2. The canonical job name is `moex_historical_cutover_job`.
3. Five real staging runs already exist and all finished with `SUCCESS`:
   - nightly cycle 1
   - nightly cycle 2
   - repair
   - backfill
   - recovery
4. Phase-scoped artifacts from those runs are available locally for packaging into the evidence bundle.
5. Authoritative raw inputs for the Phase-03 runner are available from the accepted route root or another governed source:
   - raw Delta table path
   - `raw_ingest_run_report.v2` path

If you do not already have a staging Dagster host, use the Docker profile in:
- `deployment/docker/dagster-staging/`

That profile is acceptable as a practical staging base only when it is exposed on a non-loopback host name or IP.

## Step 1 - Collect Real Run Ids And Artifacts
Prepare five real Dagster run ids and five matching artifacts.

Minimum artifact set:
- nightly 1 artifact
- nightly 2 artifact
- repair artifact
- backfill artifact
- recovery artifact

The artifact can be a JSON report file or a phase-scoped evidence directory, but it must come from the real staging owner path and must be stable enough to preserve for reruns and review.

## Step 2 - Build The Staging Binding Report
Run from repository root:

```bash
python scripts/build_moex_phase03_staging_binding_report.py \
  --dagster-url https://dagster-staging.example.internal \
  --token-env-var TA3000_DAGSTER_TOKEN \
  --nightly-1-run-id phase03-staging-nightly-1 \
  --nightly-2-run-id phase03-staging-nightly-2 \
  --repair-run-id phase03-staging-repair-1 \
  --backfill-run-id phase03-staging-backfill-1 \
  --recovery-run-id phase03-staging-recovery-1 \
  --nightly-1-artifact-path staging-evidence/nightly-1.json \
  --nightly-2-artifact-path staging-evidence/nightly-2.json \
  --repair-artifact-path staging-evidence/repair.json \
  --backfill-artifact-path staging-evidence/backfill.json \
  --recovery-artifact-path staging-evidence/recovery.json \
  --real-binding delta://TA3000-data/moex-baseline \
  --output-root artifacts/codex/moex-phase03-staging \
  --run-id 20260413T230000Z
```

What this step does:
- queries Dagster GraphQL for each supplied run id;
- fails closed if any run is missing, belongs to another job, or is not `SUCCESS`;
- fails closed if `--dagster-url` points to `localhost`, `127.0.0.1`, or another loopback-style host;
- copies the supplied staging artifacts into one immutable local bundle;
- writes:
  - `staging-binding-report.json`
  - `dagster-run-verification.json`

If the Dagster endpoint does not require auth, omit `--token-env-var`.

## Step 3 - Execute The Canonical Phase-03 Runner With Real Binding
After the staging binding report exists, rerun the canonical Phase-03 contour and point it at the real staging evidence:

```bash
python scripts/run_moex_phase03_dagster_cutover.py \
  --raw-table-path D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current/raw_moex_history.delta \
  --raw-ingest-report-path artifacts/codex/moex-phase01/<run_id>/raw-ingest-report.pass1.json \
  --output-root artifacts/codex/moex-phase03 \
  --run-id 20260413T231500Z \
  --nightly-readiness-observed-at-utc 2026-04-12T02:40:00Z \
  --nightly-readiness-observed-at-utc 2026-04-13T02:45:00Z \
  --staging-binding-report-path artifacts/codex/moex-phase03-staging/20260413T230000Z/staging-binding-report.json
```

Expected result:
- `phase03-dagster-cutover-report.json` upgrades from `proof_class=integration` to `proof_class=staging-real`;
- real staging Dagster bindings appear in the report;
- the runner preserves the same local fail-closed checks for route order, single-writer behavior, and recovery drill.

## Step 4 - Rerun Governed Acceptance
Once the canonical Phase-03 report is generated with the real staging binding report attached, rerun governed continuation for the same Phase-03 module.

The acceptance rerun should now judge the real evidence package instead of blocking on missing `staging-real` bindings.

## Output Bundle
The staging bundle is written under:
- `artifacts/codex/moex-phase03-staging/<run_id>/`

Expected files:
- `staging-binding-report.json`
- `dagster-run-verification.json`
- `staging-artifacts/nightly_1/...`
- `staging-artifacts/nightly_2/...`
- `staging-artifacts/repair/...`
- `staging-artifacts/backfill/...`
- `staging-artifacts/recovery/...`

## Fail-Closed Signals
Treat the contour as still blocked when any condition appears:
1. Any supplied Dagster run is not `SUCCESS`.
2. Any supplied Dagster run belongs to a job other than `moex_historical_cutover_job`.
3. `--dagster-url` resolves to local host, loopback, or another non-external staging binding.
4. Any required staging artifact path is missing.
5. The canonical Phase-03 report still ends with `proof_class=integration`.
6. The acceptance rerun still reports missing real governed nightly or recovery evidence.

## Notes
- This flow does not reintroduce a fallback route.
- This flow does not restore trust-gate scope.
- This flow preserves the current route decision: `Dagster -> Python raw ingest -> Spark canonical refresh`.
