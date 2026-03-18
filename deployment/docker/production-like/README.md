# Phase 6 Production-like Profile

This profile provides a local operational hardening environment for live-path preflight:
- runtime operational profile endpoint (`/health`, `/ready`, `/metrics`),
- fail-closed secrets enforcement for live bridge,
- retry policy visibility through Prometheus metrics,
- local postgres and sidecar-stub services for production-like topology smoke.

## Services
- `runtime-profile` (`localhost:8088`) - exports operational health and metrics.
- `stocksharp-sidecar-stub` (`localhost:18080`) - local sidecar transport placeholder.
- `postgres` (`localhost:5432`) - runtime state store placeholder.
- `prometheus` (`localhost:9091`) - scrapes runtime operational metrics.

## Setup
1. Create secrets and environment values from template:
   - `deployment/docker/production-like/.env.production-like.example`
2. Export required variables in your shell (`TA3000_STOCKSHARP_API_KEY`, `TA3000_FINAM_API_TOKEN`, and DB credentials).

## Start
```bash
docker compose -f deployment/docker/production-like/docker-compose.production-like.yml up -d
```

## Validate
- Runtime health: `http://localhost:8088/health`
- Runtime readiness: `http://localhost:8088/ready`
- Runtime metrics: `http://localhost:8088/metrics`
- Prometheus: `http://localhost:9091`

If secrets are missing and `TA3000_ENFORCE_LIVE_SECRETS=1`, readiness stays `503` by design.

## Stop
```bash
docker compose -f deployment/docker/production-like/docker-compose.production-like.yml down
```
