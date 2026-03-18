# MCP Troubleshooting Guide

## 1. Token expired or invalid
Symptoms:
- preflight fails with missing/permission errors;
- MCP server starts but commands return unauthorized.

Actions:
1. Rotate the specific token (`GitHub`, `Dagster`, read-only DSN).
2. Re-run `python scripts/validate_mcp_config.py`.
3. Re-run `python scripts/mcp_preflight_smoke.py --profile <profile> --strict-env-check`.

## 2. Server command not found
Symptoms:
- preflight reports `command not found in PATH`.

Actions:
1. Install required runtime (`docker`, `npx`, `uvx`).
2. Verify command path from shell (`docker --version`, `npx --version`, `uvx --version`).
3. Re-run profile smoke.

## 3. Permission denied on repo/data
Symptoms:
- MCP handshake succeeds but operations fail by scope.

Actions:
1. Check least-privilege scopes for the server token.
2. Confirm repository/workspace allowlist.
3. Keep production write access disabled for MCP.

## 4. Profile drift between docs and config
Symptoms:
- rollout docs mention servers that are missing in active config.

Actions:
1. Run `python scripts/validate_mcp_config.py --format json`.
2. Align `deployment/mcp/config.template.toml` and runbook.
3. Commit both artifacts in one patch set.
