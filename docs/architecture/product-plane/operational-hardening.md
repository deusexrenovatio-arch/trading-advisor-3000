# Operational Hardening - Operational Hardening

## Goal
Close operational hardening for controlled live execution:
- deterministic failure recovery paths,
- retry/idempotency hardening on bridge and orchestration boundaries,
- explicit secrets policy and fail-closed live preflight,
- disaster recovery notes for incident containment,
- production-like local compose profile for operational smoke.

## Deliverables
- `src/trading_advisor_3000/product_plane/runtime/config/security.py`
- `src/trading_advisor_3000/product_plane/execution/adapters/live_bridge.py` (retry + secrets preflight)
- `src/trading_advisor_3000/product_plane/execution/adapters/stocksharp_sidecar_stub.py` (transient failure simulation)
- `src/trading_advisor_3000/product_plane/execution/broker_sync/controlled_live.py` (idempotent submission reuse + hardened cycle)
- `src/trading_advisor_3000/product_plane/execution/recovery/playbook.py`
- `src/trading_advisor_3000/product_plane/runtime/ops/profile_server.py`
- `deployment/docker/production-like/docker-compose.production-like.yml`
- `deployment/docker/production-like/prometheus/prometheus.yml`
- `deployment/docker/production-like/.env.production-like.example`
- `deployment/docker/production-like/README.md`
- `tests/product-plane/unit/test_live_bridge_resilience.py`
- `tests/product-plane/unit/test_recovery_and_idempotency.py`
- `tests/product-plane/unit/test_runtime_profile_operations.py`
- `tests/product-plane/unit/test_production_profile_deployment.py`
- `tests/product-plane/integration/test_operational_hardening.py`
- `docs/runbooks/app/operational-hardening-runbook.md`

## Design Decisions
1. Live execution remains fail-closed: missing feature flags or required secrets keep bridge status degraded and block live submit.
2. Retry is explicit and bounded (`max_attempts`, `backoff_seconds`), and retry exhaustion is surfaced as a distinct execution error.
3. Controlled-live submit path is idempotent-aware: if an intent already has synced broker order state, submission is reused without second bridge call.
4. Recovery plan is deterministic and action-oriented: incidents are mapped to concrete recovery steps with escalation flags.
5. Operational profile is observable by contract (`/health`, `/ready`, `/metrics`) and can be scraped by Prometheus in a production-like local topology.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_live_bridge_resilience.py -q`
- `python -m pytest tests/product-plane/unit/test_recovery_and_idempotency.py -q`
- `python -m pytest tests/product-plane/unit/test_runtime_profile_operations.py -q`
- `python -m pytest tests/product-plane/unit/test_production_profile_deployment.py -q`
- `python -m pytest tests/product-plane/integration/test_operational_hardening.py -q`
- `python -m pytest tests/product-plane -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Out of Scope
- automated incident remediation without operator confirmation,
- real broker network transport and credential vault integration,
- full multi-region DR infrastructure orchestration.
