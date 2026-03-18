# MCP Rollback Steps

1. Switch active profile to `base` only.
2. Disable MCP-enabled workflow surfaces in local `.codex/config.toml`.
3. Revoke compromised/invalid tokens.
4. Run:
   - `python scripts/validate_mcp_config.py`
   - `python scripts/mcp_preflight_smoke.py --profile base --strict-env-check`
5. Keep failure evidence from `--format json` outputs for incident review.
