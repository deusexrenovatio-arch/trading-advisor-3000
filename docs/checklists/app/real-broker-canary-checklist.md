# Real Broker Canary Checklist

## Scope

Use only if owner enables Phase 9B.

## Pre-canary by integration

### `HTTP sidecar gateway`
- [ ] `/health` green
- [ ] `/ready` green
- [ ] `/metrics` reachable

### `StockSharp`
- [ ] delivery mode frozen
- [ ] version/build hash recorded
- [ ] compatibility note attached

### `QUIK` execution hop
- [ ] execution-route semantics confirmed
- [ ] role clearly separated from 9A live-feed role

### `Finam`
- [ ] canary account confirmed
- [ ] tiny size confirmed
- [ ] operator on call

### Observability
- [ ] metrics collection enabled
- [ ] log capture enabled
- [ ] correlation ids collected

## Canary submit
- [ ] submit exactly one minimal intent
- [ ] `intent_id` recorded
- [ ] idempotency key recorded
- [ ] ack received
- [ ] `external_order_id` recorded
- [ ] broker updates stream consistent

## After canary
- [ ] fills reconciled
- [ ] position snapshot reconciled
- [ ] no orphan fill
- [ ] no ack mismatch
- [ ] no cursor drift
- [ ] evidence package updated

## Stop immediately if
- [ ] gateway unreachable
- [ ] sidecar not ready
- [ ] duplicate fill
- [ ] ack mismatch
- [ ] unexpected position
- [ ] kill-switch fails
