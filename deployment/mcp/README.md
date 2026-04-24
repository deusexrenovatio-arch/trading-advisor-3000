# MCP Deployment Bundle

This bundle defines the operational rollout contract for MCP Wave 1-3, including the local `mempalace` memory server in the base profile.

## Contents
- `mcp-rollout-matrix.yaml` - target servers, owners, auth model, limits, health probes.
- `rollout-manifest.yaml` - wave order, profile mapping, bootstrap/security contract.
- `config.template.toml` - project-scoped MCP config contract (`base`, `ops`, `data_readonly`) with `mempalace` included in the base surface.
- `.env.mcp.example` - non-secret environment placeholders.
- `deployment/mcp/troubleshooting.md` - operational diagnostics and remediation.
- `deployment/mcp/known-limitations.md` - explicit wave limitations.
- `deployment/mcp/rollback.md` - rollback sequence.

## Security Rules
1. Do not store credentials in repository files.
2. Keep production access disabled by default.
3. Use read-only roles for database MCP servers.
4. Treat `mempalace` as local state: do not point it at shared or production-sensitive memory stores.
5. Treat this repository as trusted project only after review.

## Rollout
1. Validate config contract:
   - `python scripts/validate_mcp_config.py`
2. Bootstrap project config and merge active Codex Desktop MCP entries:
   - `python deployment/mcp/bootstrap_mcp_config.py --target .codex/config.toml --merge-home-config --force`
3. Run base preflight smoke with command probes so local memory/docs/GitHub surfaces are verified together:
   - `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check --probe-commands`
4. Run strict data-readonly profile smoke:
   - `python scripts/mcp_preflight_smoke.py --profile data_readonly --strict-env-check`
5. Run tracked-secret check:
   - `python scripts/validate_no_tracked_secrets.py`

Notes:
- `config.template.toml` remains the project contract.
- Codex Desktop currently discovers MCP servers from the user-level config in `~/.codex/config.toml`; `--merge-home-config` keeps that file aligned without overwriting unrelated personal settings.
- `mempalace` resolves its palace path from the local MemPalace installation/config and therefore remains host-specific without putting a machine path into the repository contract.

## Rollback
1. Disable MCP profile usage in local `.codex/config.toml`.
2. Revoke MCP credentials (GitHub token, workspace token, readonly DSN) and disable `mempalace` in the user config if the local memory install is the failing surface.
3. Re-run preflight in `base` profile to ensure safe fallback.
4. Follow `deployment/mcp/rollback.md` checklist and keep JSON evidence.
