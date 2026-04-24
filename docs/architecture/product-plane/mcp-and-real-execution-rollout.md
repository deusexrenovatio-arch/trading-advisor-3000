# MCP Rollout + Real Execution Path (Staging-First)

## Goal
Close two implementation gaps outside next-wave scope:
1. Full MCP rollout Wave 1-3 with governance gates, operational package, and local `mempalace` inclusion in the base profile.
2. Real execution transport path via HTTP sidecar gateway for staging-first proving.

## Deliverables
- `deployment/mcp/*` bundle (matrix, manifest, template, env placeholders, bootstrap, troubleshooting, limitations, rollback).
- `scripts/validate_mcp_config.py` + `scripts/mcp_preflight_smoke.py` + `scripts/validate_no_tracked_secrets.py`.
- Gate integration in `configs/change_surface_mapping.yaml` and `.github/workflows/ci.yml`.
- `src/trading_advisor_3000/product_plane/execution/adapters/stocksharp_http_transport.py`.
- `src/trading_advisor_3000/product_plane/execution/adapters/live_bridge.py` telemetry/fail-closed extensions.
- `deployment/docker/staging-gateway/*` containerized staging profile.
- `docs/architecture/product-plane/sidecar-wire-api-v1.md`.
- `docs/runbooks/app/real-execution-transport-runbook.md`.
- `scripts/run_f1e_real_broker_process.py` + `docs/runbooks/app/f1e-real-broker-process-runbook.md`.

## Design Decisions
1. MCP rollout is project-scoped and fail-closed: static contract checks + tracked secret scan are mandatory governance checks.
2. `mempalace` is part of the official repo MCP contract, but its palace path stays host-local through the MemPalace install/config instead of a machine path in repo files.
3. Runtime MCP smoke remains explicit operator command, while CI validates deterministic static contract.
4. Real transport is an adapter transport implementation; trading DTOs remain unchanged.
5. Sidecar streams use cursor semantics, while bridge keeps incremental drain and reconciliation compatibility.
6. Env-only secrets are temporary but guarded by redaction, startup policy, and optional secret-age enforcement.
7. Staging rollout is forced through connectivity -> canary -> controlled batch with kill-switch.

## Out of Scope
- fundamentals/news external providers onboarding;
- production direct rollout without staging proving;
- secrets backend migration to Vault/Secret Manager (only seam and policy hooks are included).
