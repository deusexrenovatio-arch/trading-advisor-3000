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
- manifest-backed JSONL outputs for the canonical data surface

Boundary:
- this step does not claim materialized Delta tables; Phase 9A stays on manifest-backed JSONL evidence

### Step A2 - strategy replay on real data
Target:
- one approved strategy spec
- reproducible backtest and replay on `MOEX` data

Evidence:
- strategy spec
- backtest report
- replay report
- optional `QUIK` live-smoke attachment on the same evidence run

### Step A3 - live-feed and runtime smoke
Target:
- `QUIK` freshness within allowed window
- runtime decision loop uses current live context

Evidence:
- live smoke report
- runtime context note
- strategy-produced signal ids that will be replayed into the runtime smoke

### Step A4 - Telegram lifecycle smoke
Target:
- create, edit, close, and cancel work through the real `Telegram Bot API` shadow/advisory contour

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

## Landed WS-A commands

These commands are now implemented for the Phase 9 data-foundation workstream.

```bash
python scripts/build_phase9_quik_connector.py
python scripts/run_phase9_provider_bootstrap.py --provider moex-history --from-date 2026-03-16 --till-date 2026-03-16 --output-dir artifacts/phase9-moex
python scripts/run_phase9_provider_bootstrap.py --provider moex-history --source-path <jsonl> --output-dir <dir>
python scripts/run_phase9_real_data_smoke.py --provider quik-live --snapshot-path <json> --as-of-ts <ts>
python scripts/run_phase9_real_data_smoke.py --provider quik-live --snapshot-url http://127.0.0.1:9001/quik/live-snapshot --as-of-ts <ts>
```

### WS-A external mode notes
- historical path can now fetch from real `MOEX ISS` over HTTP
- live smoke can now consume a real `QUIK` snapshot/export surface from either a file or an HTTP URL
- local `QUIK` can now be wired through a generated `Lua` export script under `deployment/quik-live-feed/`
- fixture import remains available for deterministic local replay and contract checks

## Landed WS-B commands

These commands are now implemented for the first production-strategy workstream.

```bash
python scripts/run_phase9_strategy_replay.py --strategy phase9-moex-breakout-v1 --bootstrap-report <bootstrap-report.json> --output-dir artifacts/phase9-strategy
python scripts/run_phase9_strategy_replay.py --strategy phase9-moex-breakout-v1 --bootstrap-report <bootstrap-report.json> --output-dir artifacts/phase9-strategy --snapshot-path <quik-snapshot.json> --as-of-ts <ts>
```

### WS-B runtime notes
- the landed runtime mode is `shadow`
- the strategy contract is frozen in `phase9-moex-breakout-v1`
- `QUIK` live-smoke can be attached to replay evidence, but that still does not prove `9B` broker execution readiness
- `advisory` stays a docs/operations posture until the runtime enum surface is intentionally expanded

## Landed WS-C commands

These commands are now implemented for the Telegram/PostgreSQL battle-run closure workstream.

```bash
python scripts/run_phase9_shadow_signal_smoke.py --output-dir artifacts/phase9-shadow-smoke
python scripts/run_phase9_shadow_signal_smoke.py --output-dir artifacts/phase9-shadow-smoke --dsn <postgres-dsn> --telegram-bot-token <token> --telegram-shadow-channel <channel>
```

### WS-C runtime notes
- battle-run preflight is fail-closed for `TA3000_APP_DSN`, `TA3000_TELEGRAM_BOT_TOKEN`, and `TA3000_TELEGRAM_SHADOW_CHANNEL`
- battle-run preflight is also fail-closed for `TA3000_TELEGRAM_TRANSPORT=bot-api`
- runtime stack now has an explicit `phase9-battle-run` profile with `PostgreSQL` as the default signal store
- `TA3000_TELEGRAM_API_BASE_URL` may point to a Bot API mirror or test server, but the transport contract stays `bot-api`
- restart smoke must show zero duplicate publications after replay
- observability evidence can be exported locally even when live `Prometheus / Loki / Grafana` URLs are still missing
- advisory Telegram destination remains optional and explicitly named only when used

## Landed WS-D commands

These commands are now implemented for the sidecar delivery preparation workstream.

```bash
python scripts/run_phase9_sidecar_preflight.py --base-url http://127.0.0.1:18081
python scripts/run_phase9_sidecar_preflight.py --base-url http://127.0.0.1:18081 --skip-rollout-dry-run
```

### WS-D delivery notes
- sidecar delivery mode is now frozen by machine-readable manifest
- preflight verifies `/health`, `/ready`, and `/metrics` against the frozen route
- rollout dry-run is part of reproducibility proof, not an optional extra
- this still does not count as broker canary evidence

## Landed WS-E commands

These commands are now implemented for the integrated Phase 9A battle-run workstream.

```bash
python scripts/run_phase9_battle_run.py --output-dir artifacts/phase9-battle-run --bootstrap-report <bootstrap-report.json> --snapshot-path <quik-snapshot.json>
python scripts/run_phase9_battle_run.py --output-dir artifacts/phase9-battle-run --bootstrap-report <bootstrap-report.json> --snapshot-path <quik-snapshot.json> --mode advisory --sidecar-base-url http://127.0.0.1:18081
```

### WS-E integration notes
- integrated status is `ready_for_review` or `blocked`; it does not silently self-upgrade to accepted
- `advisory` is still a publication posture, not a new runtime signal mode
- integrated runtime smoke must consume the same signal ids that were accepted by the strategy replay surface
- optional sidecar preflight may be attached to the same report, but it does not block 9A readiness
- missing Phase 8 proving evidence is kept as an explicit warning in the integrated report

## Target commands to add in later Phase 9 workstreams

The remaining commands stay planned until the next workstreams land.

```bash
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
