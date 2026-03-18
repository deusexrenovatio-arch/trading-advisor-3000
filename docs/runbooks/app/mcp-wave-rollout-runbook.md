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

## Token and Role Preparation
1. `github`:
   - use repo-scoped token/app with minimum read/write PR permissions.
2. `dagster`:
   - use workspace token limited to dev/staging assets.
3. `postgres_readonly`:
   - use dedicated read-only role and DSN.
4. `duckdb`:
   - use dev/staging dataset path only.

## Network Requirements
1. Host must reach required MCP endpoints (GitHub API, Dagster workspace endpoint).
2. `docker`, `npx`, and `uvx` executables must be available in PATH.
3. Production data networks are not allowed for Wave 1-3 default profiles.

## Bootstrap
1. Create project-scoped config:
   - `python deployment/mcp/bootstrap_mcp_config.py --target .codex/config.toml`
2. Validate static contract:
   - `python scripts/validate_mcp_config.py`
3. Validate tracked secret hygiene:
   - `python scripts/validate_no_tracked_secrets.py`

## Smoke Procedure
1. Base profile:
   - `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check`
2. Ops profile:
   - `python scripts/mcp_preflight_smoke.py --profile ops --strict-env-check`
3. Data read-only profile:
   - `python scripts/mcp_preflight_smoke.py --profile data_readonly --strict-env-check`

Optional command probe execution:
- `python scripts/mcp_preflight_smoke.py --profile ops --strict-env-check --probe-commands`

## Incident Scenarios
1. Token expired / invalid:
   - rotate the specific credential;
   - rerun `validate_mcp_config.py` and profile smoke.
2. Server command unavailable:
   - verify runtime (`docker`, `npx`, `uvx`) is installed on host;
   - rerun smoke for affected profile only.
3. Permission denied:
   - reduce token scope to least privilege;
   - verify repository/org scoping and rerun smoke.
4. Server unavailable:
   - run `--format json` smoke and keep failure payload;
   - downgrade profile to the previous healthy wave.

## Recovery
1. Downgrade to `base` profile if `ops` or `data_readonly` fails.
2. Revoke problematic credentials and reissue least-privilege tokens.
3. Keep failure evidence as JSON output from `mcp_preflight_smoke.py --format json`.
4. Follow `deployment/mcp/rollback.md` for full rollback.
