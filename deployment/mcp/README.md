# MCP Deployment Bundle

This bundle defines the operational rollout contract for MCP Wave 1-3.

## Contents
- `mcp-rollout-matrix.yaml` - target servers, owners, auth model, limits, health probes.
- `rollout-manifest.yaml` - wave order, profile mapping, bootstrap/security contract.
- `config.template.toml` - project-scoped MCP config contract (`base`, `ops`, `data_readonly`).
- `.env.mcp.example` - non-secret environment placeholders.
- `troubleshooting.md` - operational diagnostics and remediation.
- `known-limitations.md` - explicit wave limitations.
- `rollback.md` - rollback sequence.

## Security Rules
1. Do not store credentials in repository files.
2. Keep production access disabled by default.
3. Use read-only roles for database MCP servers.
4. Treat this repository as trusted project only after review.

## Rollout
1. Validate config contract:
   - `python scripts/validate_mcp_config.py`
2. Run preflight smoke (env + command readiness):
   - `python scripts/mcp_preflight_smoke.py --profile ops`
3. Run strict data-readonly profile smoke:
   - `python scripts/mcp_preflight_smoke.py --profile data_readonly --strict-env-check`
4. Run tracked-secret check:
   - `python scripts/validate_no_tracked_secrets.py`

## Rollback
1. Disable MCP profile usage in local `.codex/config.toml`.
2. Revoke MCP credentials (GitHub token, workspace token, readonly DSN).
3. Re-run preflight in `base` profile to ensure safe fallback.
4. Follow `deployment/mcp/rollback.md` checklist and keep JSON evidence.
