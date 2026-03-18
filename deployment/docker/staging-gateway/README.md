# Staging Gateway Profile (Real Transport Path)

This profile is the staging-first rollout surface for the real execution transport:

`runtime bridge (HTTP transport) -> sidecar gateway -> stocksharp->quik->finam route`

## Scope
1. Connectivity and readiness proving without mandatory order submission.
2. Canary and controlled batch flow with kill-switch support.
3. Env-only temporary secrets with fail-closed startup policy.

## Services
- `runtime-profile` (`localhost:8088`) - live bridge health/readiness/metrics.
- `stocksharp-sidecar-gateway` (`localhost:18081`) - wire-level sidecar API v1.
- `prometheus` (`localhost:9092`) - runtime + gateway metrics.

## Setup
1. Prepare env from template:
   - `deployment/docker/staging-gateway/.env.staging-gateway.example`
2. Export all required variables in shell (flags + secrets + rotated_at timestamps).

## Start
```bash
docker compose -f deployment/docker/staging-gateway/docker-compose.staging-gateway.yml up -d
```

## Staging Rollout Procedure
1. Stage 1 (connectivity + readiness):
   - verify `http://localhost:8088/ready` and `http://localhost:18081/ready`.
2. Stage 2 (canary live-intent on test account):
   - submit a single minimal intent via rollout script/runbook procedure.
3. Stage 3 (controlled batch + reconciliation + kill-switch):
   - submit limited batch only while `TA3000_GATEWAY_KILL_SWITCH=0`;
   - if incidents spike, set `TA3000_GATEWAY_KILL_SWITCH=1` and cancel active intents.

Automation command:
```bash
python scripts/run_staging_real_execution_rollout.py --base-url http://localhost:18081 --stage all --batch-size 3
```

## Rollback
1. Set `TA3000_GATEWAY_KILL_SWITCH=1`.
2. Stop stack:
```bash
docker compose -f deployment/docker/staging-gateway/docker-compose.staging-gateway.yml down
```
3. Revoke/rotate env-only secrets and reset to non-live profile.
