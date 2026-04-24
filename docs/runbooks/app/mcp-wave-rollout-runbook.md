# MCP Wave Rollout Runbook

## Purpose
Deploy and validate MCP Wave 1-3 for `trading-advisor-3000` with fail-closed preflight checks.

## Rollout Artifacts
- `deployment/mcp/mcp-rollout-matrix.yaml`
- `deployment/mcp/rollout-manifest.yaml`
- `deployment/mcp/config.template.toml`
- `deployment/mcp/.env.mcp.example`
- `deployment/mcp/troubleshooting.md`
- `deployment/mcp/known-limitations.md`
- `deployment/mcp/rollback.md`

## Target Servers
Wave 1:
- `github`
- `openai_docs`
- `mempalace`

Wave 2:
- `docker`
- `dagster`

Wave 3:
- `postgres_readonly`
- `duckdb`

## Preconditions
1. Project trust policy is accepted for this repository.
2. Credentials are present only in local/staging environment variables.
3. No production write-access credentials are used for MCP servers.
4. Owners confirm least-privilege scopes for each server token.
5. `mempalace` must resolve to a local palace configured on the host; do not point it at shared or production-sensitive memory stores.

## Token and Role Preparation
1. `github`:
   - use repo-scoped token/app with minimum read/write PR permissions;
   - export both `TA3000_MCP_GITHUB_TOKEN` and `GITHUB_PERSONAL_ACCESS_TOKEN` to satisfy local preflight and Docker pass-through.
2. `dagster`:
   - use workspace token limited to dev/staging assets.
3. `postgres_readonly`:
   - use dedicated read-only role and DSN.
4. `duckdb`:
   - use dev/staging dataset path only.
5. `mempalace`:
   - ensure MemPalace is installed on Python 3.11 for the host launcher;
   - verify `~/.mempalace/config.json` resolves the intended local palace path;
   - verify `py -3.11 -m mempalace status` succeeds before relying on the base profile.

## Network Requirements
1. Host must reach required MCP endpoints (`github`, `openai_docs`, Dagster workspace endpoint).
2. `docker`, `npx`, `uvx`, and `py` executables must be available in PATH.
3. Production data networks are not allowed for Wave 1-3 default profiles.
4. A valid local MemPalace install/config must exist for `mempalace`.

## Bootstrap
1. Create project-scoped config:
   - `python deployment/mcp/bootstrap_mcp_config.py --target .codex/config.toml`
   - on Windows the bootstrap script normalizes `npx` and `uvx` command entries to runnable executable variants.
2. Merge MCP entries into the active Codex Desktop user config:
   - `python deployment/mcp/bootstrap_mcp_config.py --target .codex/config.toml --merge-home-config --force`
3. Validate static contract:
   - `python scripts/validate_mcp_config.py`
4. Validate tracked secret hygiene:
   - `python scripts/validate_no_tracked_secrets.py`

Desktop note:
- the project file `.codex/config.toml` is the repo contract;
- the desktop application currently reads MCP server definitions from `~/.codex/config.toml`, so the merge step above is part of the operational bootstrap.
- the repo contract intentionally does not hardcode a palace filesystem path; `mempalace` resolves it from the local MemPalace configuration on the host.

## Smoke Procedure
1. Base profile:
   - `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check --probe-commands`
2. Ops profile:
   - `python scripts/mcp_preflight_smoke.py --profile ops --strict-env-check`
3. Data read-only profile:
   - `python scripts/mcp_preflight_smoke.py --profile data_readonly --strict-env-check`

Optional command probe execution:
- `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check --probe-commands`
- `python scripts/mcp_preflight_smoke.py --profile ops --strict-env-check --probe-commands`

## Incident Scenarios
1. Token expired / invalid:
   - rotate the specific credential;
   - rerun `validate_mcp_config.py` and profile smoke.
2. Server command unavailable:
   - verify runtime (`docker`, `npx`, `uvx`, `py`) is installed on host;
   - rerun smoke for affected profile only.
3. MemPalace module/config unavailable:
   - verify `py -3.11 -m mempalace status`;
   - confirm `~/.mempalace/config.json` points to the intended local palace;
   - rerun `--profile base --probe-commands`.
4. Permission denied:
   - reduce token scope to least privilege;
   - verify repository/org scoping and rerun smoke.
5. Server unavailable:
   - run `--format json` smoke and keep failure payload;
   - downgrade profile to the previous healthy wave.

## Recovery
1. Downgrade to `base` profile if `ops` or `data_readonly` fails.
2. If the failure is isolated to `mempalace`, disable that user-level MCP entry temporarily and keep the rest of the base profile healthy while fixing the local memory install/config.
3. Revoke problematic credentials and reissue least-privilege tokens.
4. Keep failure evidence as JSON output from `mcp_preflight_smoke.py --format json`.
5. Follow `deployment/mcp/rollback.md` for full rollback.
