# Phase 9 MOEX Breakout V1

## Purpose

Define the first production strategy slice accepted for the Phase 9 pilot contour.
This document freezes one concrete strategy contract so `WS-B` is not just a template.

## Metadata

- strategy id: `phase9-moex-breakout-v1`
- status: `shadow`
- owner surface: app research/runtime pilot
- pilot universe: `BR-6.26`, `Si-6.26`

## Integration scope

- historical source: `MOEX`
- primary live feed: `QUIK`
- publication surface: `Telegram`
- runtime state store: `PostgreSQL`
- optional 9B execution route: `HTTP gateway -> StockSharp -> QUIK -> Finam`

## Strategy hypothesis

The strategy looks for intraday breakout continuation on the frozen MOEX futures pilot universe.
It only acts when the fast trend is already ahead of the slow trend and current volume is at or above recent average.

## Contract and session assumptions

- instruments: `BR`, `Si`
- contract selection rule: Phase 9 pilot universe only
- primary timeframe: `15m`
- session assumption: operate only inside `07:00-21:00 UTC`
- roll rule: use the Phase 9 frozen roll/session note from the data bootstrap
- live-feed assumption: `QUIK` freshness must stay inside the Phase 9 live-smoke window

## Feature inputs

- feature set version: `feature-set-v1`
- required features: `atr`, `ema_fast`, `ema_slow`, `donchian_high`, `donchian_low`, `rvol`, `last_close`
- historical-data assumption: bars are produced from the `MOEX` bootstrap output and keep pilot-universe coverage intact
- live-data assumption: `QUIK` is used as runtime context and evidence source, not as proof of broker readiness

## Entry and exit logic

### Entry
- long when fast EMA is above slow EMA, relative volume is at least baseline, and the close is near the Donchian high after ATR buffering
- short when fast EMA is below slow EMA, relative volume is at least baseline, and the close is near the Donchian low after ATR buffering

### Exit and risk
- sizing model: fixed `1` shadow unit
- max parallel signals: `2`
- exposure caps: `1` active signal per contract and `2` across the pilot universe
- cooldown: `2` bars
- walk-forward windows: `2`
- commission assumption: `0.25`
- slippage assumption: `4.0` bps

## External systems and probes

| System | Used for | Version / route | Required secrets | Probe / smoke check |
| --- | --- | --- | --- | --- |
| `MOEX` | historical bars | Phase 9 provider bootstrap | none for public ISS path | bootstrap report with dataset version |
| `QUIK` | live feed and evidence context | local `Lua -> JSON snapshot` or snapshot URL | env-managed only | live-smoke freshness report |
| `Telegram` | shadow publication lifecycle | existing runtime publisher | bot token + destination id | lifecycle smoke / replay sample |
| `PostgreSQL` | battle-run state | existing runtime store | DSN in env | restart/idempotency smoke |
| `Prometheus / Loki / Grafana` | replay evidence | existing observability path | env-managed only | metrics/log artifact export |
| `HTTP gateway -> StockSharp -> QUIK -> Finam` | optional 9B route only | sidecar execution boundary | gateway + broker secrets in env | not required for WS-B acceptance |

## Expected operating band

- target signal frequency: `1-6` signals per trading day across the pilot universe
- acceptable dry periods: up to `3` consecutive sessions without a signal
- unacceptable overtrading threshold: repeated per-bar firing outside the fixed pilot universe

## Pilot acceptance note

The current landed `WS-B` code proves:

- one concrete strategy id exists and is frozen
- backtest config is derived from the strategy risk template
- replay/evidence flow consumes the `WS-A` bootstrap context
- `QUIK` live-smoke evidence can be attached to the same replay report

The current landed `WS-B` code does not claim:

- broker readiness
- `Phase 9B` execution closure
- an `advisory` runtime enum mode

Today the code path is explicitly `shadow`-ready.
`Advisory` remains a publication/operations posture described at the docs layer, not a new runtime enum.

## Evidence command

```bash
python scripts/run_phase9_strategy_replay.py --strategy phase9-moex-breakout-v1 --bootstrap-report <bootstrap-report.json> --output-dir artifacts/phase9-strategy --snapshot-path <quik-snapshot.json> --as-of-ts <utc-ts>
```

## Rejection criteria

- feature-set drift from the replay contract
- missing pilot-contract coverage in the supplied dataset
- zero backtest candidates or zero accepted replay candidates
- degraded `QUIK` live-smoke evidence when live proof is attached
- one-sided signal behavior across multiple consecutive sessions without owner review
