# Contract Surfaces

This document tracks release-blocking boundary contracts that are versioned in code and fixtures.

## Source Of Truth Rule
- Versioned JSON schemas: `src/trading_advisor_3000/product_plane/contracts/schemas/`
- Matching fixtures: `tests/product-plane/fixtures/contracts/`
- Matching contract tests: `tests/product-plane/contracts/`
- Release-blocking inventory + compatibility classes: `src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml`
- Contract change policy: `docs/architecture/product-plane/contract-change-policy.md`

## Release-Blocking Boundary Inventory

| Boundary | Contract set | Status | Notes |
| --- | --- | --- | --- |
| Core market/signal/execution DTOs | `canonical_bar.v1`, `signal_candidate.v1`, `runtime_signal.v1`, `signal_event.v1`, `decision_publication.v1`, `order_intent.v1`, `broker_order.v1`, `broker_fill.v1`, `broker_event.v1`, `position_snapshot.v1`, `risk_snapshot.v1` | implemented | Base product-plane contracts remain versioned with fixtures and regression tests. |
| Runtime API envelopes | `runtime_api_health_response.v1`, `runtime_api_ready_response.v1`, `runtime_api_replay_candidates_request.v1`, `runtime_api_replay_candidates_response.v1`, `runtime_api_close_signal_request.v1`, `runtime_api_close_signal_response.v1`, `runtime_api_cancel_signal_request.v1`, `runtime_api_cancel_signal_response.v1` | implemented | Release-blocking runtime API inventory is governed by decision `F1-C-RUNTIME-API-INVENTORY-SCOPE-V1`. |
| Research strategy governance | `gold_feature_snapshot.v1`, `strategy_candidate_projection.v1`, `strategy_scorecard.v1`, `strategy_promotion_decision.v1` | implemented | Canonical-to-strategy governed flow uses versioned projection, scorecard, and promotion decision contracts. |
| Telegram publication path | `telegram_operation.v1`, `decision_publication.v1` | implemented | Publication operation envelope is versioned alongside publication event contract. |
| Sidecar HTTP wire envelopes | `sidecar_submit_intent_request.v1`, `sidecar_submit_intent_response.v1`, `sidecar_cancel_intent_request.v1`, `sidecar_cancel_intent_response.v1`, `sidecar_replace_intent_request.v1`, `sidecar_replace_intent_response.v1`, `sidecar_updates_stream_response.v1`, `sidecar_fills_stream_response.v1`, `sidecar_health_response.v1`, `sidecar_ready_response.v1`, `sidecar_metrics_response.v1`, `sidecar_kill_switch_request.v1`, `sidecar_kill_switch_response.v1`, `sidecar_error_response.v1` | implemented | Wire envelopes are freeze-governed independent of real broker process status. |
| Runtime configuration envelope | `runtime_bootstrap_config.v1` | implemented | Runtime bootstrap profile/backend/channel envelope is versioned and tested. |
| Persistence + migration boundary | `runtime_signal_store_persistence_manifest.v1` + runtime signal/event/publication DTO schemas | implemented | Signal-store table and migration-tracking boundary are versioned and test-enforced. |
| External rollout/connectivity envelopes | `broker_staging_connector_profile.v1`, `staging_rollout_report.v1`, `runtime_operational_snapshot.v1`, `real_broker_process_report.v1` | implemented | Rollout profile, Finam session/secrets contract, operational snapshot, and governed broker proof report are versioned with compatibility class rules. |
## Runtime API Inventory Scope Decision
- Decision ID: `F1-C-RUNTIME-API-INVENTORY-SCOPE-V1`
- Source: `src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml`
- Exclusions from release-blocking runtime API inventory:
  - `GET /runtime/signal-events`: excluded because it is a read-only projection over `signal_event.v1`, which is already governed under the core DTO boundary.
  - `GET /runtime/strategy-registry`: excluded because it is an operator-facing inventory projection and not a release-blocking execution handshake envelope.

## Compatibility + Change Control
Compatibility classes and upgrade rules are defined in:

- `src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml`
- `docs/architecture/product-plane/contract-change-policy.md`

Any public payload change is invalid unless schema, fixture, and tests are updated together.

## Internal Product Plane Orchestration Contracts

The following contracts are versioned and test-covered, but they are not part of the release-blocking runtime boundary inventory:
- `research_campaign.v1`
- `research_run_summary.v1`

## DB Mapping Baseline
- `runtime_signal.v1` maps to `signal.active_signals`
- `signal_event.v1` maps to `signal.signal_events`
- `decision_publication.v1` maps to `signal.publications`
