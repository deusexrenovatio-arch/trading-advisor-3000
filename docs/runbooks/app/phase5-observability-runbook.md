# Phase 5 Observability Runbook

## Purpose
Operate and validate Phase 5 review analytics and observability:
`signal events + fills + positions -> outcomes -> review metrics -> Prometheus/Loki exports -> Grafana views`.

## Preconditions
- Phase 5 tests are green.
- Replay fixtures are available (`tests/app/fixtures/research/canonical_bars_sample.jsonl`).
- Docker is running for local observability stack checks.

## Validation Procedure
1. Run Phase 5 integration tests:
   - `python -m pytest tests/app/integration/test_phase5_review_observability.py -q`
2. Run Phase 5 unit tests:
   - `python -m pytest tests/app/unit/test_phase5_review_metrics.py -q`
   - `python -m pytest tests/app/unit/test_phase5_latency_metrics.py -q`
   - `python -m pytest tests/app/unit/test_phase5_observability_export.py -q`
   - `python -m pytest tests/app/unit/test_phase5_observability_deployment.py -q`
3. Run full app regression:
   - `python -m pytest tests/app -q`

## Local Observability Smoke
1. Start the stack:
   - `docker compose -f deployment/docker/observability/docker-compose.observability.yml up -d`
2. Check availability:
   - Prometheus: `http://localhost:9090`
   - Grafana: `http://localhost:3000`
   - Loki API: `http://localhost:3100/ready`
3. In Grafana, confirm dashboard `TA3000 Phase5 Overview` is provisioned.
4. In Prometheus, query:
   - `ta3000_strategy_signals_total`
   - `ta3000_latency_status_total`
5. Stop stack after smoke:
   - `docker compose -f deployment/docker/observability/docker-compose.observability.yml down`

## Incident Triage Map
- `latency_status=missing_activation`
  - Meaning: signal was opened but never activated.
  - Action: inspect publish path and runtime decision gating.
- `latency_status=missing_open_fill`
  - Meaning: activation happened but no opening execution fill.
  - Action: inspect broker sync/bridge and fill ingestion path.
- `latency_status=missing_close_event`
  - Meaning: signal did not receive a terminal close/cancel/expire event.
  - Action: verify runtime closing logic and event dedup.
- `latency_status=missing_close_fill`
  - Meaning: close event exists but execution close fill is absent.
  - Action: inspect execution event role mapping (`open`/`close`) and fill correlation.
- `latency_status=clock_skew`
  - Meaning: event timestamps are out-of-order.
  - Action: verify time source consistency across runtime, broker bridge, and sidecar.

## Recovery Sequence
1. Freeze acceptance publication of new dashboards.
2. Re-run integrated replay and regenerate Phase 5 artifacts.
3. Compare `phase5_report.summary.latency_status_counts` against previous known-good baseline.
4. If drift persists, isolate by replaying one signal ID and tracing event chronology.
5. Re-run loop/pr gates before unfreezing.
