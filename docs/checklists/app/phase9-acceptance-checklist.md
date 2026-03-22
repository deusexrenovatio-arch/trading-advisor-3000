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
- [ ] `python scripts/run_phase9_provider_bootstrap.py ...` landed
- [ ] `python scripts/run_phase9_real_data_smoke.py ...` landed
- [ ] `python scripts/build_phase9_quik_connector.py` landed
- [ ] `MOEX ISS` external fetch path landed
- [ ] `QUIK` live snapshot file or URL path landed
- [ ] local `QUIK Lua` export path landed
- [ ] 5 consecutive refresh cycles green

## Phase 9A - strategy and runtime
- [ ] first production strategy spec landed
- [ ] `phase9-moex-breakout-v1` spec landed
- [ ] backtest artifact generated
- [ ] replay artifact generated
- [ ] `python scripts/run_phase9_strategy_replay.py ...` landed
- [ ] strategy assumptions mention `MOEX` and `QUIK`
- [ ] `WS-B` evidence is explicit that code closure is `shadow`-ready, not broker-ready
- [ ] battle-run mode defaults to `PostgreSQL`

## Phase 9A - Telegram and evidence
- [ ] `Telegram` shadow/advisory path landed
- [ ] `phase9-battle-run` env contract landed
- [ ] `python scripts/run_phase9_shadow_signal_smoke.py ...` landed
- [ ] battle-run preflight fails closed on missing `Telegram` or `PostgreSQL` requirements
- [ ] 10+ correct `Telegram` lifecycles captured
- [ ] no duplicate publication after restart smoke
- [ ] observability snapshots attached
- [ ] operator review note attached

## Phase 9B - execution canary
- [ ] sidecar delivery manifest landed
- [ ] `python scripts/run_phase9_sidecar_preflight.py ...` landed
- [ ] actual `StockSharp` delivery mode frozen
- [ ] dry-run rollout is part of reproducibility evidence
- [ ] `HTTP sidecar gateway` readiness proof attached
- [ ] canary evidence attached
- [ ] reconciliation evidence attached
- [ ] kill-switch proof attached

## Explicit non-claims
- [ ] docs do not claim broker readiness for Phase 9A
- [ ] docs do not treat `QUIK` live feed as proof of `Finam` execution readiness
- [ ] docs do not describe `MCP` as runtime dependency
- [ ] docs do not include live secret values
