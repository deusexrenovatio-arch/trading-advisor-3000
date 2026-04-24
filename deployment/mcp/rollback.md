# MCP Rollback Steps

1. Switch active profile to `base` only.
2. Disable MCP-enabled workflow surfaces in local `.codex/config.toml`; if the incident is local-memory specific, disable `mempalace` first.
3. Revoke compromised/invalid tokens.
4. Run:
   - `python scripts/validate_mcp_config.py`
   - `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check --probe-commands`
5. Keep failure evidence from `--format json` outputs for incident review.
