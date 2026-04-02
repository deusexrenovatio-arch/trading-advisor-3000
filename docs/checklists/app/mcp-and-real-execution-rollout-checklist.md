# MCP + Real Execution Rollout Checklist

Date: 2026-03-18

## Scope
- [x] MCP rollout Wave 1-3 (project-scoped config, governance, preflight).
- [x] Real execution path staging-first (HTTP transport, sidecar wire contract, gateway profile).
- [x] Fundamentals/news external onboarding is excluded from this wave.

## MCP Acceptance
- [x] Server matrix + ownership/auth model is documented.
- [x] Project-scoped config contract has required profiles (`base`, `ops`, `data_readonly`).
- [x] Static config validation exists and is wired to gates.
- [x] Tracked secret scan exists and is wired to gates.
- [x] Runtime preflight smoke command supports negative scenarios.
- [x] Deployment bundle includes troubleshooting, limitations, rollback.

## Real Execution Acceptance
- [x] Bridge has HTTP/JSON sidecar transport with idempotency semantics.
- [x] Retry/fail-closed behavior is explicit and test-covered.
- [x] Sidecar wire API v1 is documented.
- [x] Staging containerized gateway profile is added.
- [x] Env-only secrets mode has startup validation and redaction.
- [x] Staging rollout procedure includes connectivity -> canary -> controlled batch + kill-switch.
- [x] Observability includes submit latency, sidecar errors, retry exhaustion, sync lag, reconciliation incident rate.

## Evidence Commands
- [x] `python -m pytest tests/process/test_mcp_rollout_contracts.py -q`
- [x] `python -m pytest tests/product-plane/unit/test_real_execution_http_transport.py -q`
- [x] `python -m pytest tests/product-plane/unit/test_real_execution_staging_gateway_deployment.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_real_execution_staging_rollout.py -q`
- [x] `python scripts/validate_mcp_config.py`
- [x] `python scripts/validate_no_tracked_secrets.py`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`
