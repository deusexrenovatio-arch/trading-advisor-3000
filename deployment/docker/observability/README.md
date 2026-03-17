# Phase 5 Observability Stack

This profile provisions local Prometheus, Loki, and Grafana for the Phase 5 review/analytics slice.

## Included services
- Prometheus (`localhost:9090`) for metrics scrape.
- Loki (`localhost:3100`) for structured log aggregation.
- Grafana (`localhost:3000`) with auto-provisioned datasources and dashboard.

## Runtime integration assumptions
- Runtime metrics endpoint is exposed at `http://host.docker.internal:9464/metrics`.
- Runtime logs are shipped to Loki by your local log pipeline.

## Start
```bash
docker compose -f deployment/docker/observability/docker-compose.observability.yml up -d
```

## Stop
```bash
docker compose -f deployment/docker/observability/docker-compose.observability.yml down
```

## Smoke checks
- Open Grafana and verify dashboard `TA3000 Phase5 Overview` is present.
- In Prometheus, confirm `ta3000_strategy_signals_total` appears in target metrics.
- In Grafana Explore (Loki), verify latency events with `status` labels can be queried.
