# MOEX Operations Readiness Runbook

## Purpose
Verify live-real operations readiness for:
- scheduler run-state observability,
- recovery replay with rollback-safe publish-pointer behavior,
- monitoring coverage for latency, failures, QC, drift, and freshness,
- final Gate A-D release decision bundle.

Nightly production profile is pinned to:
- backfill bootstrap start: `21:00` local time,
- execution mode: `parallel_sharded`,
- worker count: `4`,
- expected canonical readiness: by `06:00` local time when upstream MOEX is reachable.

Route role:
- This runbook documents operations-readiness proof for the consolidated route.
- It is not an independent competing route or planning module.

## Preconditions
1. Python env is ready: `python -m pip install -e .[dev]`.
2. Prior route reports and governed acceptance closures exist:
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-raw-ingest/<run_id>/raw-ingest-summary-report.json` for raw ingest
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh/<run_id>/canonical-refresh-report.json` for canonical refresh
   - `$TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-reconciliation/<run_id>/reconciliation-report.json` for reconciliation
   - `artifacts/codex/orchestration/<raw-ingest-route-run>/attempt-<n>/acceptance.json` with `verdict=PASS`
   - `artifacts/codex/orchestration/<canonical-refresh-route-run>/attempt-<n>/acceptance.json` with `verdict=PASS`
   - `artifacts/codex/orchestration/<reconciliation-route-run>/attempt-<n>/acceptance.json` with `verdict=PASS`
3. Operations-readiness policy files exist:
   - `configs/moex_phase04/scheduler_jobs.v1.yaml`
   - `configs/moex_phase04/monitoring_requirements.v1.yaml`
4. Live-real input files include `source_provenance` with immutable export path and hash:
   - scheduler: `--scheduler-status-source-path`
   - monitoring: `--monitoring-evidence-source-path`
   - recovery: `--recovery-drill-source-path`
5. Optional defects snapshot is available through `--defects-source-path`.

## Execute

```bash
export TA3000_MOEX_HISTORICAL_DATA_ROOT=/absolute/path/outside/repo

python scripts/verify_moex_operations_readiness.py \
  --raw-ingest-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-raw-ingest \
  --raw-ingest-run-id 20260402T090251Z \
  --raw-ingest-acceptance-path artifacts/codex/orchestration/<raw-ingest-route-run>-moex-historical-route-consolidation-phase-01/attempt-<n>/acceptance.json \
  --canonical-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-canonical-refresh \
  --canonical-run-id 20260402T124500Z \
  --canonical-acceptance-path artifacts/codex/orchestration/<canonical-refresh-route-run>-moex-historical-route-consolidation-phase-02/attempt-<n>/acceptance.json \
  --reconciliation-root $TA3000_MOEX_HISTORICAL_DATA_ROOT/moex-reconciliation \
  --reconciliation-run-id 20260402T152500Z \
  --reconciliation-acceptance-path artifacts/codex/orchestration/<reconciliation-route-run>-moex-historical-route-consolidation-phase-03/attempt-<n>/acceptance.json \
  --scheduler-status-source-path /absolute/path/outside/repo/moex-operations-input/scheduler-status-20260402T172000Z.json \
  --monitoring-evidence-source-path /absolute/path/outside/repo/moex-operations-input/monitoring-evidence-20260402T172000Z.json \
  --recovery-drill-source-path /absolute/path/outside/repo/moex-operations-input/recovery-drill-20260402T172000Z.json \
  --defects-source-path /absolute/path/outside/repo/moex-operations-input/defects-20260402T172000Z.json \
  --output-root /absolute/path/outside/repo/moex-operations-readiness \
  --run-id 20260402T172000Z \
  --route-signal remediation:phase-only
```

The runner exits non-zero on `BLOCKED`.

## Manual Route Refresh
The manual sharded route runner `scripts/run_moex_route_refresh.py` is not the scheduled operating path.

It may be used only for explicit forensic reruns and is blocked by default.

Do not use that script as the normal overnight historical refresh path.

The authoritative routing decision is:
- scheduled ownership: `Dagster -> Python raw ingest -> Spark canonical refresh`
- scheduled raw-ingest mode: shared contract discovery once per run, then shard-only candle ingest
- scheduled refresh horizon: bounded rolling window (`bootstrap_window_days=180`, `contract_discovery_lookback_days=180`) instead of nightly full-history replay
- manual step tools only: `scripts/run_moex_raw_ingest.py` and `scripts/run_moex_canonical_refresh.py`

Reference:
- `docs/architecture/product-plane/moex-historical-route-decision.md`

## Incident Classes And Triage
Use these classes for deterministic operator action:
1. `SCHEDULER_STALL`: required job tick or run state is stale, failed, or queue exceeds policy.
2. `MONITORING_GAP`: mandatory metric, dashboard, or query evidence is missing or placeholder URL detected.
3. `RECOVERY_FAILURE`: replay requires manual cleanup, mandatory stage is missing, or pointer restore is unhealthy.
4. `INTEGRITY_DEFECT`: unresolved `P1/P2` defect remains open.

Triage flow:
1. Open `operations-readiness-report.json` and identify `failed_gates`.
2. Map each failed condition to one incident class.
3. Use the class-specific playbook below.
4. Re-run the operations-readiness runner only after evidence is refreshed from the same production channel.

## Restart/Replay Playbook
### 1. Scheduler Recovery (`SCHEDULER_STALL`)
1. Check `scheduler-config-snapshot.json` -> `scheduler_status_validation.job_results`.
2. Isolate the failing `job_id` and the last successful `run_id`.
3. Trigger controlled replay from scheduler or orchestrator tooling for the affected job only.
4. Confirm a fresh successful tick and run pair for the replayed job.

### 2. Monitoring Recovery (`MONITORING_GAP`)
1. Check `monitoring-dashboard-query-evidence.json` -> `monitoring_validation.errors`.
2. Ensure all required dashboards and queries are present and URLs are non-placeholder, production-resolvable links.
3. Re-export monitoring evidence with an updated immutable export hash.
4. Re-run the operations-readiness runner and verify Gate D returns `PASS`.

### 3. Replay/Restart Recovery (`RECOVERY_FAILURE`)
1. Check `recovery-drill-artifact.json` -> `recovery_validation.sequence`.
2. Sequence must contain `fail -> retry -> replay -> recover`, all `SUCCESS`.
3. Confirm `manual_cleanup_performed=false` and `deterministic_replay=true`.
4. Validate publish-pointer behavior:
   - `rolled_back_to == last_healthy_snapshot`
   - `restored_healthy=true`

## Rollback And Publish-Pointer Handling
1. If replay is unstable, rollback to `publish_pointer.last_healthy_snapshot`.
2. Do not publish a new snapshot until recovery evidence reaches deterministic `recover`.
3. If rollback target and last healthy snapshot diverge, treat it as a hard failure and block the release decision.

## Required Evidence Files
Per run in `/absolute/path/outside/repo/moex-operations-readiness/<run_id>/`:
- `operations-readiness-report.json`
- `scheduler-config-snapshot.json`
- `monitoring-dashboard-query-evidence.json`
- `recovery-drill-artifact.json`
- `source-authenticated-provenance.json`
- `operations-runbook-checklist.json`
- `release-decision-bundle.json`

## Evidence Inspection Paths
1. `operations-readiness-report.json`
   - `status`, `failed_gates`, `real_bindings`, `artifact_paths`
2. `source-authenticated-provenance.json`
   - immutable export path and hash plus collection metadata for scheduler, monitoring, and recovery sources
3. `release-decision-bundle.json`
   - `target_release_decision`, `gate_results`, `checklist_status`, `upstream_acceptance_artifacts`

## Block Conditions
Keep the route blocked when any condition appears:
1. Gate D is `FAIL`.
2. `target_release_decision=DENY_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR`.
3. Any unresolved `P1/P2` data-integrity defect exists.
4. Source-provenance hash or path validation fails.
5. Recovery pointer restore is not healthy.
