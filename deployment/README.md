# Deployment

Product-plane deployment artifacts.
Phase 0 created baseline directories; later phases add concrete stubs/configurations.

- `deployment/docker/`
- `deployment/docker/observability/`
- `deployment/docker/staging-gateway/`
- `deployment/stocksharp-sidecar/`
- `deployment/mcp/`

## MCP Bundle
`deployment/mcp/` now contains Wave 1-3 rollout artifacts:
- server ownership/auth matrix,
- rollout manifest (waves/profiles/bootstrap/security),
- project-scoped config template,
- environment placeholders,
- bootstrap helper for `.codex/config.toml`,
- troubleshooting, known limitations, and rollback guides.
