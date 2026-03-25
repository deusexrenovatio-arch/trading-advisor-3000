# Phase 9 Workstreams And Patch Sets

## Workstream graph

### Blocking start
- `WS-0` baseline verification

### Parallel after `WS-0`
- `WS-A` historical and live data integration closure
- `WS-B` first production strategy slice
- `WS-C` Telegram and Postgres battle-run closure
- `WS-D` sidecar delivery preparation

### Integration after `WS-A + WS-B + WS-C`
- `WS-E` Phase 9A battle-run integration

### Optional after `WS-E`
- `WS-F` Phase 9B real broker canary

## `WS-0` - baseline verification

### Scope
- confirm current truth sources
- rerun app tests and gate lanes
- confirm existing external boundaries

### Deliverables
- baseline verification note
- fresh Phase 8 proving report
- accepted starting branch/SHA

### Acceptance
- baseline commands are green

## `WS-A` - data integration closure

### External systems
- `MOEX`
- `QUIK`

### Patch sets
1. freeze named sources and provider assumptions
2. document historical bootstrap and dataset/version evidence
3. document live-feed freshness and session checks
4. freeze pilot universe and roll/session rules

### Acceptance
- 5 consecutive refresh cycles
- live-feed smoke evidence

## `WS-B` - first production strategy slice

### External systems
- `MOEX`
- `QUIK`

### Patch sets
1. strategy spec and risk template
2. feature mapping with provider assumptions
3. backtest and replay evidence
4. acceptance note for pilot use

### Acceptance
- one strategy is accepted for shadow/advisory pilot

## `WS-C` - Telegram and Postgres battle-run closure

### External systems
- `Telegram`
- `PostgreSQL`
- `Prometheus / Loki / Grafana`

### Patch sets
1. battle-run env contract
2. Postgres-default runtime path
3. Telegram destination and lifecycle closure
4. observability-backed evidence package

### Acceptance
- 10+ correct Telegram lifecycles
- restart-safe Postgres evidence

## `WS-D` - sidecar delivery preparation

### External systems
- `HTTP sidecar gateway`
- `StockSharp`
- `QUIK`
- `Finam`

### Patch sets
1. freeze actual sidecar delivery mode
2. document build/run compatibility contract
3. document readiness and health expectations
4. document dry-run and canary path

### Acceptance
- sidecar delivery mode is formally frozen and reproducible

## `WS-E` - Phase 9A battle-run integration

### External systems
- `MOEX`
- `QUIK`
- `Telegram`
- `PostgreSQL`
- `Prometheus / Loki / Grafana`

### Scope
- combine data, strategy, and runtime closure
- run shadow/advisory pilot
- produce evidence package

### Acceptance
- Phase 9A accepted

## `WS-F` - Phase 9B real broker canary

### External systems
- `HTTP sidecar gateway`
- `StockSharp`
- `QUIK`
- `Finam`
- `Prometheus / Loki / Grafana`

### Scope
- execute staging-first real transport proving
- run canary under operator gate
- attach reconciliation evidence

### Acceptance
- Phase 9B accepted or explicitly deferred

## Patch-set policy

1. One patch set equals one engineering idea.
2. For risky integration surfaces, land contracts/config first, then code, then docs/runbooks/checklists.
3. If the same failure repeats twice, stop expansion and remediate before continuing.
