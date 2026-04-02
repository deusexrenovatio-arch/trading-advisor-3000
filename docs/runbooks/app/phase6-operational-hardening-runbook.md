# Phase 6 Operational Hardening Runbook

## Purpose
Operate live-path execution with fail-closed controls:
`submit intent -> retry/idempotent bridge -> broker sync -> reconciliation -> recovery plan`.

## Preconditions
- Phase 6 unit/integration tests are green.
- Live feature flags are explicitly set for current environment.
- Required live secrets are present when `enforce_live_secrets=1`.
- Operator has rollback authority for live submissions.

## Secrets Policy
Required secrets by default:
- `TA3000_STOCKSHARP_API_KEY`
- `TA3000_FINAM_API_TOKEN`

Policy behavior:
- if `enforce_live_secrets=1` and one secret is missing, live submit is blocked;
- health payload returns only redacted values;
- readiness remains degraded until policy is satisfied.

## Failure Scenarios Replay
1. Retryable transport failure:
   - inject transient sidecar failures;
   - verify submit/cancel/replace succeeds before retry budget is exhausted.
2. Retry exhaustion:
   - exceed retry budget;
   - verify explicit `LiveExecutionRetryExhaustedError` and no ghost intent in sync state.
3. Reconciliation drift:
   - produce quantity/state mismatch;
   - verify recovery plan includes freeze + deterministic remediation steps.
4. Idempotent replay:
   - submit same intent twice;
   - verify second call reuses existing ack and does not duplicate sidecar submit.

## Operational Validation
1. Run Phase 6 integration tests:
   - `python -m pytest tests/product-plane/integration/test_phase6_operational_hardening.py -q`
2. Run Phase 6 unit tests:
   - `python -m pytest tests/product-plane/unit/test_phase6_live_bridge_hardening.py -q`
   - `python -m pytest tests/product-plane/unit/test_phase6_recovery_and_idempotency.py -q`
   - `python -m pytest tests/product-plane/unit/test_phase6_runtime_profile_ops.py -q`
   - `python -m pytest tests/product-plane/unit/test_phase6_production_profile_deployment.py -q`
3. Run full app regression:
   - `python -m pytest tests/product-plane -q`

## Production-like Compose Smoke
1. Start profile:
   - `docker compose -f deployment/docker/production-like/docker-compose.production-like.yml up -d`
2. Validate runtime profile:
   - `http://localhost:8088/health`
   - `http://localhost:8088/ready`
   - `http://localhost:8088/metrics`
3. Validate Prometheus scrape:
   - open `http://localhost:9091`
   - query `ta3000_live_bridge_ready`
   - query `ta3000_live_bridge_missing_secrets_total`
4. Stop profile:
   - `docker compose -f deployment/docker/production-like/docker-compose.production-like.yml down`

## Disaster Recovery Notes
1. Containment:
   - freeze new live submissions immediately when high-severity incidents appear.
2. State reconstruction:
   - replay broker updates and fills from last known clean cursor;
   - rebuild order mapping and recompute positions.
3. Verification:
   - run controlled reconciliation and ensure no high-severity incidents remain.
4. Controlled restore:
   - resume live submissions only after clean reconciliation and operator approval.
5. Escalation:
   - if critical reason repeats after one replay cycle, escalate to manual incident command.
