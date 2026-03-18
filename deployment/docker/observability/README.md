# Phase 5 Observability Stack

This profile provisions local Prometheus, Loki, and Grafana for the Phase 5 review/analytics slice.

## Included services
- `metrics-file-exporter` (`localhost:9464`) serves `runtime-artifacts/observability.prometheus.metrics.txt` as `/metrics`.
- Prometheus (`localhost:9090`) scrapes the file exporter.
- Promtail tails `runtime-artifacts/observability.loki.events.jsonl` and pushes labeled logs to Loki.
- Loki (`localhost:3100`) stores logs with labels including `job="trading-advisor-runtime"`.
- Grafana (`localhost:3000`) has auto-provisioned Prometheus/Loki datasources and dashboard.

## Data source files
- `deployment/docker/observability/runtime-artifacts/observability.prometheus.metrics.txt`
- `deployment/docker/observability/runtime-artifacts/observability.loki.events.jsonl`

These files are versioned with sample data so the stack is smoke-test ready immediately.
To validate with fresh replay output, replace these two files with Phase 5 replay artifacts.

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
- In Prometheus, query `ta3000_strategy_signals_total` and `ta3000_observability_file_bridge_up`.
- In Grafana Explore (Loki), query `{job="trading-advisor-runtime"}` and verify `stream="latency"` rows are present.
