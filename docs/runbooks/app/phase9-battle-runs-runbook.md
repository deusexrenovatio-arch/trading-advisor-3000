# Phase 9 Battle Runs Runbook

## Purpose

Operate Phase 9 battle runs with explicit external integration checks and evidence discipline.
This runbook covers:

- `Phase 9A`: `MOEX`, `QUIK`, `Telegram`, `PostgreSQL`, and observability closure.
- `Phase 9B`: `HTTP gateway`, `StockSharp`, `QUIK`, `Finam`, and canary proving.

## Preconditions

### Baseline
- working Python environment is active
- `tests/app` are green
- loop gate is green
- PR gate is green
- Phase 8 proving is green

### Integration preconditions
- pilot universe is frozen
- `MOEX` is frozen as historical source
- `QUIK` is frozen as live feed
- private `Telegram` shadow destination is prepared
- `PostgreSQL` is available and migrations can run
- observability bundle is reachable for evidence collection

## Phase 9A preflight by integration

### `MOEX`
- confirm historical bootstrap route and pilot universe
- confirm dataset/version output format
- confirm roll/session rules note exists

### `QUIK` live feed
- confirm live-feed freshness window
- confirm session validity rules
- confirm this is treated as market-data ingress, not broker execution proof

### `Telegram`
- confirm bot token is present
- confirm private shadow destination id is present
- confirm advisory destination is optional and explicitly named if used

### `PostgreSQL`
- run migrations
- confirm DSN is present
- confirm battle-run path is configured for `PostgreSQL`

### `Prometheus / Loki / Grafana`
- confirm metrics scrape target is reachable
- confirm log capture path is reachable
- confirm dashboards or snapshots can be exported

## Phase 9A sequence

### Step A1 - historical bootstrap
Target:
- reproducible `MOEX` backfill for frozen pilot universe

Evidence:
- bootstrap log
- dataset version
- freshness summary
- roll/session note

### Step A2 - strategy replay on real data
Target:
- one approved strategy spec
- reproducible backtest and replay on `MOEX` data

Evidence:
- strategy spec
- backtest report
- replay report

### Step A3 - live-feed and runtime smoke
Target:
- `QUIK` freshness within allowed window
- runtime decision loop uses current live context

Evidence:
- live smoke report
- runtime context note

### Step A4 - Telegram lifecycle smoke
Target:
- create, edit, close, and cancel work in the real shadow/advisory contour

Evidence:
- publication samples
- lifecycle audit sample
- anti-duplication smoke note

### Step A5 - battle-run pilot
Target:
- 10+ correct lifecycles
- restart-safe `PostgreSQL` state
- observability evidence attached

Evidence:
- signal lifecycle artifact
- restart/replay artifact
- metrics and log snapshots
- operator review note

## Phase 9B preflight by integration

### `HTTP sidecar gateway`
- `/health` green
- `/ready` green
- `/metrics` reachable

### `StockSharp`
- delivery mode is frozen
- version/build hash is recorded
- compatibility with wire API is known

### `QUIK` broker hop
- route semantics confirmed for execution path
- explicitly documented as separate from 9A live-feed role

### `Finam`
- canary account and tiny sizing confirmed
- operator gate enabled

### `Prometheus / Loki / Grafana`
- canary metrics and logs can be captured with correlation ids

## Phase 9B sequence

### Step B1 - readiness only
No order send.

Check:
- runtime ready
- gateway ready
- sidecar ready
- metrics visible

### Step B2 - canary
Send one minimal live intent through:

`runtime -> HTTP sidecar gateway -> StockSharp -> QUIK -> Finam`

Check:
- `intent_id` is recorded
- ack maps to stream updates
- no orphan fill or position drift

### Step B3 - controlled batch
Only if owner enables it.

Check:
- batch size remains limited
- reconciliation after each cycle
- kill-switch path proven

## Target commands to add in Phase 9

These commands are target interfaces for later code phases.
They are documented here; they are not assumed to exist yet.

```bash
python scripts/run_phase9_provider_bootstrap.py --provider moex --universe <file>
python scripts/run_phase9_real_data_smoke.py --provider quik --universe <file>
python scripts/run_phase9_strategy_replay.py --strategy <strategy_id> --dataset <dataset_version>
python scripts/run_phase9_shadow_signal_smoke.py --channel shadow --duration 1h
python scripts/run_phase9_battle_run.py --mode advisory --duration 1d
python scripts/run_phase9_canary_live_intent.py --base-url http://localhost:18081 --qty 1
```

## Failure triage

### `MOEX` bootstrap drift
- stop battle run
- inspect source lag or missing sessions
- do not publish new signals until dataset is rebuilt

### `QUIK` live-feed drift
- stop new signals
- inspect freshness and session mapping
- keep battle-run evidence open until feed is stable

### `Telegram` duplication
- stop publication
- inspect publication idempotency state
- reconcile active signals before resume

### `PostgreSQL` mismatch
- freeze signal creation
- replay signal events
- rebuild snapshot from durable history

### Sidecar or broker mismatch
- stop live submit
- keep sync and reconciliation on
- collect `intent_id`, `external_order_id`, and metrics evidence

## Exit criteria

### Phase 9A
- `MOEX` bootstrap evidence attached
- `QUIK` freshness evidence attached
- `Telegram` 10+ lifecycles attached
- `PostgreSQL` restart evidence attached
- observability snapshots attached

### Phase 9B
- sidecar version is pinned
- readiness snapshots attached
- canary evidence attached
- reconciliation is clean or operator-approved
