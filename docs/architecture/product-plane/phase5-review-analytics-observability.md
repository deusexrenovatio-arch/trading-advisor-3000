# Phase 5 - Review, Analytics, Observability

## Goal
Close the review and observability slice on top of integrated replay:
- strategy/instrument performance dashboards from closed outcomes,
- latency visibility for signal lifecycle and execution boundaries,
- deterministic exports for Prometheus/Loki,
- local Grafana/Prometheus/Loki deployment profile for smoke validation.

## Deliverables
- `src/trading_advisor_3000/product_plane/runtime/analytics/review.py`
- `src/trading_advisor_3000/product_plane/runtime/analytics/system_replay.py` (extended exports and contracts)
- `deployment/docker/observability/docker-compose.observability.yml`
- `deployment/docker/observability/prometheus/prometheus.yml`
- `deployment/docker/observability/loki/loki-config.yml`
- `deployment/docker/observability/promtail/promtail-config.yml`
- `deployment/docker/observability/metrics_file_server.py`
- `deployment/docker/observability/runtime-artifacts/observability.prometheus.metrics.txt`
- `deployment/docker/observability/runtime-artifacts/observability.loki.events.jsonl`
- `deployment/docker/observability/grafana/provisioning/datasources/datasources.yml`
- `deployment/docker/observability/grafana/provisioning/dashboards/dashboards.yml`
- `deployment/docker/observability/grafana/dashboards/ta3000-phase5-overview.json`
- `tests/product-plane/unit/test_phase5_review_metrics.py`
- `tests/product-plane/unit/test_phase5_latency_metrics.py`
- `tests/product-plane/unit/test_phase5_observability_export.py`
- `tests/product-plane/unit/test_phase5_observability_deployment.py`
- `tests/product-plane/integration/test_phase5_review_observability.py`
- `docs/runbooks/app/phase5-observability-runbook.md`

## Design Decisions
1. Review metrics are built only from closed signals to avoid mixing open PnL and realized outcomes.
2. Dashboard rows are generated as deterministic daily aggregates (`strategy` and `instrument` views) with explicit win-rate and drawdown metrics.
3. Latency metrics are emitted per signal with explicit non-happy statuses (`missing_activation`, `missing_open_fill`, `missing_close_event`, `missing_close_fill`, `clock_skew`), so operational drift is visible even when execution did not fully complete.
4. Observability exports are file-contract based (`metrics.txt` for Prometheus and JSON lines for Loki) to keep acceptance reproducible in CI and replay runs.
5. Replay integration now emits Phase 5 artifacts and delta-manifest entries in one pass, so acceptance can validate full lineage from market bars to operational metrics.
6. Local observability stack is provisioned as a standalone compose profile with file bridges (`metrics-file-exporter` and `promtail`) so Phase 5 file artifacts can be validated end-to-end without external metrics or log pipelines.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_phase5_review_metrics.py -q`
- `python -m pytest tests/product-plane/unit/test_phase5_latency_metrics.py -q`
- `python -m pytest tests/product-plane/unit/test_phase5_observability_export.py -q`
- `python -m pytest tests/product-plane/unit/test_phase5_observability_deployment.py -q`
- `python -m pytest tests/product-plane/integration/test_phase5_review_observability.py -q`
- `python -m pytest tests/product-plane/integration/test_phase3_system_replay.py -q`
- `python -m pytest tests/product-plane -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`
- `python scripts/run_nightly_gate.py --from-git --git-ref HEAD`

## Out of Scope
- production-grade clustered log shipping topology,
- external alert routing and on-call escalation wiring,
- long-term retention and cost policies for observability storage.
