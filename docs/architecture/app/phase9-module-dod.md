# Phase 9 Module DoD

## `data_plane.providers`

### Baseline now
Provider seams already exist.

### Phase 9 DoD
- `MOEX` is frozen as the historical source.
- `QUIK` is frozen as the primary live feed.
- Provider assumptions are documented at the docs level.
- Freshness and session checks are named in the acceptance surface.

### Acceptance
- historical bootstrap evidence exists
- live-feed freshness smoke exists
- docs do not leave provider identity ambiguous

## `data_plane.ingestion` and `data_plane.canonical`

### Baseline now
Scaffolding and integration slices exist.

### Phase 9 DoD
- pilot-universe bootstrap is reproducible from `MOEX`
- session and roll rules are frozen
- dataset version is emitted into evidence package
- gaps and duplicates policy is documented

### Acceptance
- 5 consecutive refresh cycles green
- dataset version recorded
- roll/session note attached

## `research.strategies`, `features`, `backtest`, `forward`

### Baseline now
Research seams and replay baseline exist.

### Phase 9 DoD
- one production strategy spec exists
- strategy assumptions explicitly cite `MOEX` historical data and `QUIK` live-feed context
- expected operating band is documented
- replay and backtest evidence are reproducible

### Acceptance
- strategy spec reviewed
- backtest artifact generated
- replay artifact generated
- no hidden provider drift between research and runtime assumptions

## `runtime.signal_store`

### Baseline now
Postgres store and migrations exist.

### Phase 9 DoD
- battle-run mode defaults to `PostgreSQL`
- restart and replay do not duplicate signal state
- migration sequence is part of runbook preflight

### Acceptance
- restart smoke green
- idempotent restart evidence attached
- no in-memory fallback in battle-run documentation contract

## `runtime.publishing`

### Baseline now
Telegram publication lifecycle exists.

### Phase 9 DoD
- real `Telegram` shadow destination is wired as an external dependency
- advisory destination is documented as optional
- message traceability is persisted
- edit, close, cancel, and anti-duplication behavior are evidenced

### Acceptance
- 10+ correct `Telegram` lifecycles
- publication samples attached
- duplicate-publication smoke is green

## `runtime.config` and `security`

### Baseline now
Fail-closed config and secrets policy exist.

### Phase 9 DoD
- env contract lists `Telegram`, `PostgreSQL`, `QUIK`, and optional 9B secrets
- missing required secrets fail closed
- `MCP` keeps readonly posture and no broker credentials

### Acceptance
- preflight green on correct env
- degraded or fail-closed behavior on missing secrets
- tracked-secret checks remain green

## `execution.adapters`, `live_bridge`, `broker_sync`, `reconciliation`

### Baseline now
Controlled live transport and reconciliation baselines exist.

### Phase 9A DoD
- execution stays outside acceptance closure
- docs keep `HTTP gateway` visible as an existing boundary, not a requirement for 9A

### Phase 9B DoD
- actual `StockSharp` delivery mode is frozen
- `HTTP gateway -> StockSharp -> QUIK -> Finam` route is explicitly documented
- canary and reconciliation expectations are defined

### Acceptance
- Stage 1 readiness green
- Stage 2 canary green
- Stage 3 batch green or formally deferred

## `deployment/stocksharp-sidecar`

### Baseline now
Placeholder only.

### Phase 9B DoD
One of these must be true:

- real in-repo sidecar project exists, or
- pinned external delivery package exists with build/run contract and compatibility note

### Acceptance
- exact sidecar version is recorded
- compatibility with wire contract is evidenced
- operator can reproduce the sidecar delivery mode

## Docs, runbooks, checklists, templates

### Phase 9 DoD
- external systems are named consistently
- `9A` and `9B` are separated
- `QUIK` dual role is called out explicitly
- strategy and evidence templates capture integration details

### Acceptance
- all new docs link cleanly
- no contradiction with [STATUS.md](docs/architecture/app/STATUS.md)
- no live secrets appear in docs
