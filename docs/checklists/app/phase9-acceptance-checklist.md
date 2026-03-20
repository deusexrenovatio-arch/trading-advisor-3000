# Phase 9 Acceptance Checklist

## Acceptance disposition
- [ ] Phase 9A accepted
- [ ] Phase 9B accepted
- [ ] Phase 9B intentionally deferred by owner

## Baseline
- [ ] current repo baseline confirmed against [STATUS.md](docs/architecture/app/STATUS.md)
- [ ] `tests/app` green
- [ ] loop gate green
- [ ] PR gate green
- [ ] Phase 8 proving green

## Integration freeze
- [ ] `MOEX` is fixed as historical source
- [ ] `QUIK` is fixed as primary live feed
- [ ] `QUIK` execution-route role is documented separately for 9B
- [ ] `Telegram` destinations are explicitly named
- [ ] `PostgreSQL` is mandatory for battle-run mode
- [ ] `HTTP sidecar gateway`, `StockSharp`, and `Finam` are marked as 9B-only execution surfaces
- [ ] `Prometheus / Loki / Grafana` evidence path is defined
- [ ] `MCP` is documented as readonly support only

## Phase 9A - historical and live data
- [ ] `MOEX` bootstrap note landed
- [ ] dataset versioning rule landed
- [ ] roll/session note landed
- [ ] `QUIK` freshness rule landed
- [ ] 5 consecutive refresh cycles green

## Phase 9A - strategy and runtime
- [ ] first production strategy spec landed
- [ ] backtest artifact generated
- [ ] replay artifact generated
- [ ] strategy assumptions mention `MOEX` and `QUIK`
- [ ] battle-run mode defaults to `PostgreSQL`

## Phase 9A - Telegram and evidence
- [ ] `Telegram` shadow/advisory path landed
- [ ] 10+ correct `Telegram` lifecycles captured
- [ ] no duplicate publication after restart smoke
- [ ] observability snapshots attached
- [ ] operator review note attached

## Phase 9B - execution canary
- [ ] actual `StockSharp` delivery mode frozen
- [ ] `HTTP sidecar gateway` readiness proof attached
- [ ] canary evidence attached
- [ ] reconciliation evidence attached
- [ ] kill-switch proof attached

## Explicit non-claims
- [ ] docs do not claim broker readiness for Phase 9A
- [ ] docs do not treat `QUIK` live feed as proof of `Finam` execution readiness
- [ ] docs do not describe `MCP` as runtime dependency
- [ ] docs do not include live secret values
