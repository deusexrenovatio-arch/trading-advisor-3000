# Phase 9 Battle-Run Env Contract

## Purpose

Freeze the explicit runtime contract for `WS-C`.
This document defines which external integrations must be present before Phase 9 battle-run mode is allowed to start.

## Mandatory external systems

- `Telegram` for shadow publication lifecycle
- `PostgreSQL` for durable runtime state
- `Prometheus / Loki / Grafana` as the observability and evidence contour

## Required configuration

| Env name | External system | Purpose | Required for start |
| --- | --- | --- | --- |
| `TA3000_RUNTIME_PROFILE` | internal runtime contract | must stay `phase9-battle-run` | yes |
| `TA3000_SIGNAL_STORE_BACKEND` | `PostgreSQL` | must stay `postgres` | yes |
| `TA3000_SIGNAL_STORE_SCHEMA` | `PostgreSQL` | runtime schema name, default `signal` | yes |
| `TA3000_APP_DSN` | `PostgreSQL` | durable state connection string | yes |
| `TA3000_TELEGRAM_TRANSPORT` | `Telegram` | must stay `bot-api` for real external publication proof | yes |
| `TA3000_TELEGRAM_API_BASE_URL` | `Telegram` | optional Bot API base URL override for mirrors/tests | no |
| `TA3000_TELEGRAM_BOT_TOKEN` | `Telegram` | publication credentials | yes |
| `TA3000_TELEGRAM_SHADOW_CHANNEL` | `Telegram` | mandatory shadow destination | yes |
| `TA3000_TELEGRAM_ADVISORY_CHANNEL` | `Telegram` | optional advisory destination | no |
| `TA3000_PROMETHEUS_BASE_URL` | `Prometheus` | optional live observability pointer | no |
| `TA3000_LOKI_BASE_URL` | `Loki` | optional live log pointer | no |
| `TA3000_GRAFANA_DASHBOARD_URL` | `Grafana` | optional dashboard pointer | no |

## Fail-closed rule

Battle-run mode is blocked when any of these are true:

- runtime profile is not `phase9-battle-run`
- signal store backend is not `postgres`
- `TA3000_APP_DSN` is missing
- `TA3000_TELEGRAM_TRANSPORT` is not `bot-api`
- `TA3000_TELEGRAM_BOT_TOKEN` is missing
- `TA3000_TELEGRAM_SHADOW_CHANNEL` is missing

Observability URLs are intentionally warnings, not hard blockers.
The smoke can still export metrics and logs as local evidence artifacts when those live URLs are not configured yet.

## Runtime consequence

When the env contract is valid:

- runtime stack is built with `PostgreSQL` as the default signal store
- publication lifecycle is anchored to the named `Telegram` shadow destination through `Telegram Bot API`
- restart safety is checked against durable publication and signal-event history
- evidence bundle exports Prometheus-style metrics and Loki-style logs for the same smoke window

## Landed smoke command

```bash
python scripts/run_phase9_shadow_signal_smoke.py --output-dir artifacts/phase9-shadow-smoke
```

## Expected evidence

- preflight report with redacted secrets posture
- publication event sample
- signal-event sample
- restart probe showing zero duplicate publications after replay
- Prometheus metrics artifact
- Loki event artifact

## Explicit non-claims

- this contract does not prove real broker execution
- this contract does not turn `Telegram` publication into an execution acknowledgement
- this contract does not make `Prometheus / Loki / Grafana` hard runtime dependencies for local evidence export
