# Real Execution Transport Runbook

## Purpose
Operate staging-first real execution path:

`bridge (HTTP transport) -> sidecar gateway -> stocksharp->quik->finam`

with fail-closed controls, canary rollout, and controlled batch recovery.

## Preconditions
1. Live flags are explicitly enabled (`TA3000_ENABLE_*`).
2. Env-only secrets are present and redacted in logs.
3. Secret rotation timestamps are set when age policy is enabled.
4. Staging gateway profile is healthy (`/ready` green).

## 3-Stage Rollout
1. Stage 1: Connectivity and readiness (no order send)
   - check `runtime-profile /ready`;
   - check sidecar `/ready` and `/health`;
   - verify telemetry metrics in Prometheus (`submit latency`, `sidecar errors`, `sync lag`).
2. Stage 2: Canary live-intent
   - submit one minimal intent on test account;
   - verify no ghost intent and ack+stream consistency by `intent_id`.
3. Stage 3: Controlled batch
   - submit limited batch size;
   - reconcile after each poll cycle;
   - if incidents cross threshold, activate kill-switch and cancel active intents.

Reference command:
- `python scripts/run_staging_real_execution_rollout.py --base-url http://localhost:18081 --stage all --batch-size 3`
- for dry-run planning: `python scripts/run_staging_real_execution_rollout.py --base-url http://localhost:18081 --dry-run --format json`

## Incident Scenarios
1. Sidecar unreachable
   - symptoms: `sidecar_unreachable`, retry exhaustion.
   - action: pause submissions, check gateway process/network route, rerun canary only after health recovers.

2. Stale broker cursor
   - symptoms: stream returns cursor lower than already consumed or no progress.
   - action: restart cursor from known safe point, replay updates/fills, compare reconciliation output.

3. Duplicate fills
   - symptoms: repeated fill IDs or unexpected cumulative quantity.
   - action: quarantine fill ingestion, dedupe by `fill_id`, replay from broker cursor snapshot.

4. Broker ack mismatch
   - symptoms: ack `external_order_id` does not map to stream updates.
   - action: block new submissions, rebuild intent->order map from sidecar history, re-run canary.

5. Partial replace/cancel failure
   - symptoms: request accepted but stream never reaches terminal expected state.
   - action: retry operation with idempotency key, confirm order state by broker stream, escalate if mismatch persists.

6. Safe stop live path
   - set gateway kill-switch (`TA3000_GATEWAY_KILL_SWITCH=1`);
   - cancel active intents;
   - verify no new accepted live submit acknowledgements.

## Operational Evidence
Required evidence package:
1. runtime `/health` and `/metrics` snapshots;
2. sidecar `/health`, `/ready`, `/metrics`;
3. canary and batch reconciliation report;
4. operation log tail with correlation IDs:
   - `intent_id`
   - `external_order_id`
   - `transport_key`
