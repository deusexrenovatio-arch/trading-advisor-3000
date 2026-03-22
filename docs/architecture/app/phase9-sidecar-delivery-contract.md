# Phase 9 Sidecar Delivery Contract

## Purpose

Freeze the actual sidecar delivery mode for `WS-D`.
This document makes the `9B` transport boundary reproducible without pretending that broker canary already happened.

## Frozen delivery mode

Phase 9 uses one explicit delivery mode:

- delivery mode: `pinned-staging-gateway-bundle`
- wire API version: `v1`
- transport route: `HTTP gateway -> StockSharp -> QUIK -> Finam`
- in-repo delivered artifact: pinned staging gateway bundle and manifest
- out-of-repo component: real `StockSharp` broker process

This is intentionally narrower than “real broker readiness”.
`WS-D` freezes how the sidecar surface is delivered and checked, not whether `9B` can already be accepted.

## Canonical artifact

The machine-readable delivery freeze lives here:

- `deployment/stocksharp-sidecar/phase9-sidecar-delivery-manifest.json`

It defines:

- delivery mode identity
- route identity
- required readiness probes
- canonical staging profile paths
- dry-run command
- canary command
- kill-switch env name

## Build and run compatibility

The current compatibility contract is:

- compose profile: `deployment/docker/staging-gateway/docker-compose.staging-gateway.yml`
- gateway implementation: `deployment/docker/staging-gateway/gateway/sidecar_gateway_stub.py`
- env template: `deployment/docker/staging-gateway/.env.staging-gateway.example`
- wire contract: [sidecar-wire-api-v1.md](docs/architecture/app/sidecar-wire-api-v1.md)

Compatibility means all of these agree on:

- base path `v1`
- mandatory probes `/health`, `/ready`, `/metrics`
- cursor-based stream endpoints
- route identity `stocksharp->quik->finam`
- kill-switch semantics

## Readiness expectations

`WS-D` sidecar preflight is green only when all of these are true:

- live flags are explicitly enabled
- `TA3000_SIDECAR_TRANSPORT=http`
- `TA3000_SIDECAR_BASE_URL` is present
- live secrets for `StockSharp` and `Finam` are present
- `/health` is reachable and reports the frozen route
- `/ready` is green
- `/metrics` is reachable and non-empty
- rollout dry-run succeeds against the same base URL

## Landed preflight command

```bash
python scripts/run_phase9_sidecar_preflight.py --base-url http://127.0.0.1:18081
```

This command now produces:

- manifest identity
- env contract validation
- readiness probe results
- rollout dry-run summary

## Dry-run and canary path

The frozen sequencing is:

1. preflight only
2. rollout dry-run
3. canary stage
4. controlled batch

The dry-run path is required for reproducibility.
The canary path remains a `WS-F / 9B` acceptance concern.

## Explicit non-claims

`WS-D` does not claim:

- that a real `StockSharp` broker binary is now shipped in-repo
- that `Finam` canary is accepted
- that Phase 9B is ready to close

It only claims that the sidecar delivery surface is now frozen, machine-checkable, and reproducible.
