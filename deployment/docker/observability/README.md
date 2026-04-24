# Review Observability Stack

This profile provisions local Prometheus, Loki, and Grafana for the review-and-observability slice.

## Included services
- `metrics-file-exporter` (`localhost:9464`) serves `runtime-artifacts/observability.prometheus.metrics.txt` as `/metrics`.
- Prometheus (`localhost:9090`) scrapes the file exporter.
- Promtail tails `runtime-artifacts/observability.loki.events.jsonl` and pushes labeled logs to Loki.
- Loki (`localhost:3100`) stores logs with labels including `job="trading-advisor-runtime"`.
- Grafana (`localhost:3300` by default) has auto-provisioned Prometheus/Loki datasources and dashboard.
- Promtail stores its positions file in a dedicated named volume so restart/recreate cycles do not depend on container-local `/tmp` state.

## Data source files
- `deployment/docker/observability/runtime-artifacts/observability.prometheus.metrics.txt`
- `deployment/docker/observability/runtime-artifacts/observability.loki.events.jsonl`

These files are versioned with sample data so the stack is smoke-test ready immediately.
To validate with fresh replay output, replace these two files with current review-observability artifacts.

## Start
```bash
docker compose -f deployment/docker/observability/docker-compose.observability.yml up -d
```

## Stop
```bash
docker compose -f deployment/docker/observability/docker-compose.observability.yml down
```

## Recovery
- If Prometheus fails with TSDB `meta.json` / `invalid magic number` errors, stop the profile and restart against a clean Prometheus volume. The compose profile already points to the clean `prometheus-data-v2` volume.
- If Promtail fails on `positions.yaml`, recreate the service; positions are stored in the dedicated `promtail-data` volume path `/var/lib/promtail/positions.yaml`.

## Smoke checks
- Open Grafana on `http://localhost:3300` and verify dashboard `TA3000 Observability Overview` is present.
- In Prometheus, query `ta3000_strategy_signals_total` and `ta3000_observability_file_bridge_up`.
- In Grafana Explore (Loki), query `{job="trading-advisor-runtime"}` and verify `stream="latency"` rows are present.
