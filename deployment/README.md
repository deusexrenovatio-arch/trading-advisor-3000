# Deployment

Product-plane deployment artifacts.
Bootstrap setup created baseline directories; later capability slices add concrete stubs and configurations.

- `deployment/docker/`
- `deployment/docker/observability/`
- `deployment/docker/dagster-staging/`
- `deployment/docker/staging-gateway/`
- `deployment/stocksharp-sidecar/`
- `deployment/mcp/`

## MCP Bundle
`deployment/mcp/` now contains Wave 1-3 rollout artifacts with `mempalace` included in the base local-memory profile:
- server ownership/auth matrix,
- rollout manifest (waves/profiles/bootstrap/security),
- project-scoped config template,
- environment placeholders,
- bootstrap helper for `.codex/config.toml`,
- troubleshooting, known limitations, and rollback guides.
