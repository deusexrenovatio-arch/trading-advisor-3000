# MCP Troubleshooting Guide

## 1. Token expired or invalid
Symptoms:
- preflight fails with missing/permission errors;
- MCP server starts but commands return unauthorized.

Actions:
1. Rotate the specific token (`GitHub`, `Dagster`, read-only DSN).
2. Re-run `python scripts/validate_mcp_config.py`.
3. Re-run `python scripts/mcp_preflight_smoke.py --profile <profile> --strict-env-check`.

## 1a. MemPalace module or palace config missing
Symptoms:
- base profile probe fails on `mempalace`;
- `py -3.11 -m mempalace.mcp_server --help` or `py -3.11 -m mempalace status` fails locally.

Actions:
1. Verify the Python 3.11 MemPalace install on the host.
2. Verify `~/.mempalace/config.json` points to a valid palace path.
3. Re-run `py -3.11 -m mempalace status`.
4. Re-run `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check --probe-commands`.

## 2. Server command not found
Symptoms:
- preflight reports `command not found in PATH`.

Actions:
1. Install required runtime (`docker`, `npx`, `uvx`, `py`).
2. Verify command path from shell (`docker --version`, `npx --version`, `uvx --version`, `py --version`).
3. On Windows, prefer executable wrappers such as `npx.cmd` and `uvx.exe` if PowerShell execution policy blocks `.ps1`.
4. Re-run profile smoke.

## 2a. Codex Desktop does not show MCP servers
Symptoms:
- `mcp_preflight_smoke.py` is green, but the Codex UI still shows no MCP servers.

Actions:
1. Merge the project MCP contract into the user-level Codex config:
   - `python deployment/mcp/bootstrap_mcp_config.py --target .codex/config.toml --merge-home-config --force`
2. Restart Codex Desktop completely.
3. Re-check `~/.codex/config.toml` and confirm `mcp_servers.*` entries exist there.

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
