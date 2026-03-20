# Phase 9 MCP And Secrets Update

## Reading rule

The repo already has an `deployment/mcp/*` bundle.
Phase 9 must extend the current MCP usage rules; it must not invent a second MCP rollout.

## MCP role in Phase 9

`MCP` is allowed to support:

- readonly inspection of `PostgreSQL`
- docs and config validation
- battle-run evidence review
- dev/ops diagnostics

`MCP` is not allowed to:

- act as product runtime
- hold live broker credentials
- submit live orders
- replace gateway, sidecar, or runtime health checks

## Required secret families

### Phase 9A
- `MOEX` historical source credentials, if required by the chosen access method
- `QUIK` live-feed credentials or session material, if required
- `Telegram` bot token
- `Telegram` destination identifiers
- `PostgreSQL` DSN

### Phase 9B
- `HTTP gateway` auth material
- `StockSharp` sidecar auth material
- `Finam` broker credentials

## Policy

1. Secrets are env-only.
2. Logs must stay redacted.
3. Missing required secrets fail closed.
4. Rotation metadata may be tracked outside secrets themselves.
5. `MCP` profiles remain readonly where data access exists.

## Integration-specific notes

| Integration | Secret expectation | Where it may appear | Where it may not appear |
| --- | --- | --- | --- |
| `MOEX` | provider auth only if needed | local env or secret manager | tracked docs or fixtures |
| `QUIK` live feed | feed/session secret material | local env | tracked docs, evidence payloads |
| `Telegram` | bot token and destination ids | local env | tracked docs, screenshots with exposed ids |
| `PostgreSQL` | DSN | local env, readonly MCP DSN for dev/stage only | tracked docs with real values |
| `HTTP gateway` | API key or gateway auth | local env | MCP config |
| `StockSharp` | sidecar auth | local env | MCP config |
| `Finam` | broker token or equivalent | local env | tracked docs, MCP config |

## Operational note

Battle-run docs may reference secret categories and preflight checks, but never real values.
Evidence packages should record that a check passed, not the secret itself.
