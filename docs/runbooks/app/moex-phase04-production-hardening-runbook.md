# MOEX Phase-04 Production Hardening Runbook

## Purpose
Execute `Etap 4 - Production hardening` with fail-closed, live-real evidence for:
- scheduler run-state observability,
- recovery replay with rollback-safe publish-pointer behavior,
- monitoring coverage for latency/failures/QC/drift/freshness,
- final Gate A-D release decision bundle.

Nightly production profile is pinned to:
- backfill bootstrap start: `21:00` (local),
- execution mode: `parallel_sharded`,
- worker count: `4`,
- expected canonical readiness: by `06:00` (local) when upstream MOEX is reachable.

## Preconditions
1. Python env is ready: `python -m pip install -e .[dev]`.
2. Prior phase reports and governed acceptance closures exist:
   - `artifacts/codex/moex-phase01/<run_id>/phase01-foundation-report.json`
   - `artifacts/codex/moex-phase02/<run_id>/phase02-canonical-report.json`
   - `artifacts/codex/moex-phase03/<run_id>/phase03-reconciliation-report.json`
   - `artifacts/codex/orchestration/<phase-01-run>/attempt-<n>/acceptance.json` with `verdict=PASS`
   - `artifacts/codex/orchestration/<phase-02-run>/attempt-<n>/acceptance.json` with `verdict=PASS`
   - `artifacts/codex/orchestration/<phase-03-run>/attempt-<n>/acceptance.json` with `verdict=PASS`
3. Phase-04 policy files exist:
   - `configs/moex_phase04/scheduler_jobs.v1.yaml`
   - `configs/moex_phase04/monitoring_requirements.v1.yaml`
4. Live-real input files include `source_provenance` with immutable export path/hash:
   - scheduler: `--scheduler-status-source-path`
   - monitoring: `--monitoring-evidence-source-path`
   - recovery: `--recovery-drill-source-path`
5. Optional defects snapshot is available: `--defects-source-path`.

## Execute Phase-04 Runner

```bash
python scripts/run_moex_phase04_production_hardening.py \
  --phase01-root artifacts/codex/moex-phase01 \
  --phase01-run-id 20260402T090251Z \
  --phase01-acceptance-path artifacts/codex/orchestration/20260402T083632Z-moex-spark-deltalake-phase-01/attempt-02/acceptance.json \
  --phase02-root artifacts/codex/moex-phase02 \
  --phase02-run-id 20260402T124500Z \
  --phase02-acceptance-path artifacts/codex/orchestration/20260402T092448Z-moex-spark-deltalake-phase-02/attempt-03/acceptance.json \
  --phase03-root artifacts/codex/moex-phase03 \
  --phase03-run-id 20260402T152500Z \
  --phase03-acceptance-path artifacts/codex/orchestration/20260402T101446Z-moex-spark-deltalake-phase-03/attempt-02/acceptance.json \
  --scheduler-status-source-path artifacts/codex/moex-phase04-input/scheduler-status-20260402T172000Z.json \
  --monitoring-evidence-source-path artifacts/codex/moex-phase04-input/monitoring-evidence-20260402T172000Z.json \
  --recovery-drill-source-path artifacts/codex/moex-phase04-input/recovery-drill-20260402T172000Z.json \
  --defects-source-path artifacts/codex/moex-phase04-input/defects-20260402T172000Z.json \
  --output-root artifacts/codex/moex-phase04 \
  --run-id 20260402T172000Z \
  --route-signal remediation:phase-only
```

Runner exits non-zero on `BLOCKED`.

## Nightly Backfill + Canonical Build (21:00 Profile)
Use the sharded nightly entrypoint when you need overnight bootstrap/incremental refresh with canonical output by morning:

```bash
python scripts/run_moex_nightly_backfill.py \
  --workers 4 \
  --timeframes 5m,15m,1h,4h,1d,1w \
  --bootstrap-window-days 1461 \
  --expand-contract-chain \
  --contract-discovery-step-days 14 \
  --refresh-overlap-minutes 180
```

Outputs:
- `artifacts/codex/moex-phase01/<run_id>/...` (consolidated raw + shard artifacts),
- `artifacts/codex/moex-phase02/<run_id>/...` (canonical),
- `artifacts/codex/moex-nightly/<run_id>/nightly-backfill-report.json` (final nightly verdict).

## Incident Classes And Triage
Use these classes for deterministic operator action:
1. `SCHEDULER_STALL`: required job tick/run state is stale, failed, or queue exceeds policy.
2. `MONITORING_GAP`: mandatory metric/dashboard/query evidence missing or placeholder URL detected.
3. `RECOVERY_FAILURE`: replay requires manual cleanup, mandatory stage missing, or pointer restore unhealthy.
4. `INTEGRITY_DEFECT`: unresolved `P1/P2` defect remains open.

Triage flow:
1. Open `phase04-production-hardening-report.json` and identify `failed_gates`.
2. Map each failed condition to one incident class.
3. Use the class-specific playbook below.
4. Re-run phase-04 runner only after evidence is refreshed from the same production channel.

## Restart/Replay Playbook
### 1. Scheduler Recovery (`SCHEDULER_STALL`)
1. Check `scheduler-config-snapshot.json` -> `scheduler_status_validation.job_results`.
2. Isolate failing `job_id` and last successful `run_id`.
3. Trigger controlled replay from scheduler/orchestrator tooling for the affected job only.
4. Confirm fresh successful tick/run pair for the replayed job.

### 2. Monitoring Recovery (`MONITORING_GAP`)
1. Check `monitoring-dashboard-query-evidence.json` -> `monitoring_validation.errors`.
2. Ensure all required dashboards/queries are present and URLs are non-placeholder, production-resolvable links.
3. Re-export monitoring evidence with updated immutable export hash.
4. Re-run phase-04 runner and verify Gate D returns `PASS`.

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
3. If rollback target and last healthy snapshot diverge, treat as hard failure and block release decision.

## Required Evidence Files
Per run in `artifacts/codex/moex-phase04/<run_id>/`:
- `phase04-production-hardening-report.json`
- `scheduler-config-snapshot.json`
- `monitoring-dashboard-query-evidence.json`
- `recovery-drill-artifact.json`
- `source-authenticated-provenance.json`
- `operations-runbook-checklist.json`
- `release-decision-bundle.json`

## Evidence Inspection Paths
1. `phase04-production-hardening-report.json`
   - `status`, `failed_gates`, `real_bindings`, `artifact_paths`
2. `source-authenticated-provenance.json`
   - immutable export path/hash and collection metadata for scheduler/monitoring/recovery sources
3. `release-decision-bundle.json`
   - `target_release_decision`, `gate_results`, `checklist_status`, `phase_acceptance_artifacts`

## Block Conditions
Keep phase blocked when any condition appears:
1. Gate D is `FAIL`.
2. `target_release_decision=DENY_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR`.
3. Any unresolved `P1/P2` data-integrity defect exists.
4. Source provenance hash/path validation fails.
5. Recovery pointer restore is not healthy.
