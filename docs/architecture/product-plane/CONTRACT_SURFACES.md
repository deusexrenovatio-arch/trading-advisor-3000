# Contract Surfaces

This document tracks release-blocking boundary contracts that are versioned in code and fixtures.

## Source Of Truth Rule
- Versioned JSON schemas: `src/trading_advisor_3000/product_plane/contracts/schemas/`
- Matching fixtures: `tests/product-plane/fixtures/contracts/`
- Matching contract tests: `tests/product-plane/contracts/`
- Release-blocking inventory + compatibility classes: `src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml`
- Contract change policy: `docs/architecture/product-plane/contract-change-policy.md`

## Release-Blocking Boundary Inventory

Staged opt-in identity contracts: `canonical_bar.v3`,
`continuous_front_bars.v3`, `instrument_namespace.v1`, `temporal_mapping.v1`.
Shared authority: `market_identity.v3`; pure evaluator: `contracts/identity_v3.py`.
See [identity decision, keys and compatibility](docs/architecture/product-plane/market-identity.md).
These files prepare a new boundary; they do not replace the runtime contracts or
amend the release-blocking v1 inventory. Producer/consumer admission is pending.

Native snapshot metadata admission adds `snapshot_identity_selector.v1` and
`snapshot_identity_binding.v1`, referring to the same `market_identity.v3`
authority. Market Data verifies source inputs and resolves exact snapshot pins;
this mode establishes no temporal/PIT applicability. Native future expiry and
complete specification can remain unknown. The bounded opt-in read slice adds
`snapshot_read_request.v1` and `snapshot_read_projection.v1` under the same
authority: Arrow bars/L0/L1/L2, original schema/types/masks and explicit snapshot
pins. This does not admit full canonical Q, CF or historical/PIT resolution.
See the [read projection runbook](docs/runbooks/app/snapshot-read-projection.md).

| Boundary | Contract set | Status | Notes |
| --- | --- | --- | --- |
| Core market/signal/execution DTOs | `canonical_bar.v1`, `signal_candidate.v1`, `runtime_signal.v1`, `signal_event.v1`, `decision_publication.v1`, `order_intent.v1`, `broker_order.v1`, `broker_fill.v1`, `broker_event.v1`, `position_snapshot.v1`, `risk_snapshot.v1` | implemented | Base product-plane contracts remain versioned with fixtures and regression tests. |
| Runtime API envelopes | `runtime_api_health_response.v1`, `runtime_api_ready_response.v1`, `runtime_api_replay_candidates_request.v1`, `runtime_api_replay_candidates_response.v1`, `runtime_api_close_signal_request.v1`, `runtime_api_close_signal_response.v1`, `runtime_api_cancel_signal_request.v1`, `runtime_api_cancel_signal_response.v1` | implemented | Release-blocking runtime API inventory is governed by decision `F1-C-RUNTIME-API-INVENTORY-SCOPE-V1`. |
| Research strategy governance | `technical_indicator_snapshot.v1`, `strategy_candidate_projection.v1`, `strategy_evaluation_profile.v1`, `strategy_scorecard.v1`, `strategy_promotion_decision.v1` | implemented | Canonical-to-strategy governed flow uses indicator snapshots, versioned projection, strategy evaluation profile, scorecard, and promotion decision contracts. |
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
- `continuous_front_bars.v1`
- `continuous_front_roll_events.v1`
- `continuous_front_adjustment_ladder.v1`
- `continuous_front_qc_report.v1`
- `research_campaign.v1`
- `research_run_summary.v1`
- `data_scope_recovery.v1`: exact native Data segment proof, retained historical prefix,
  immutable bridge publication; [operational boundary](../../runbooks/app/data-research-snapshot-publication.md#explicit-scope-recovery).
- `data_economics_scope_transition.v1`: the native-proved zero-effect prepared to
  canonical economics transition, exact source/target pins and immutable metadata
  repair; [operator route](docs/runbooks/app/economics-canonical-cutover.md#complete-the-data-to-research-transition).
  Research consumes its validator through the public `data_plane.canonical` API.
- `research_refresh_admission.v1`: explicit daily/rebuild operation; native input and parent pins;
  NOOP/SCOPED/BLOCKED; original/effective windows and declared domain. One JSON schema,
  fixture and contract test. Existing Dagster config/checkpoints and Research manifest
  parameters carry it; [operational boundary](../../runbooks/app/indicator-refresh.md#daily-admission-boundary).

## DB Mapping Baseline
- `runtime_signal.v1` maps to `signal.active_signals`
- `signal_event.v1` maps to `signal.signal_events`
- `decision_publication.v1` maps to `signal.publications`

## Research Storage Retirement

Persisted `mtf_*` projections and legacy source hashes are retired. Native strategy timeframe APIs retain their contracts. Migration and rollback: [Continuous Front Indicator Storage](docs/architecture/product-plane/continuous-front-indicator-storage.md).

## Planned Trade Workspace boundaries

The accepted [Workspace contract design](docs/architecture/product-plane/trade-workspace/contracts.md)
proposes SourceSubmission/NormalizedTradeInput, CalculationBundle,
TradeWriteBundle/TradeSnapshot/CommitReceipt, DataContext, ReviewEnvelope and
lineage/read envelopes. These describe the full target; local Journal/Intake
candidates have narrower executable contracts and separate delivery evidence.

The full-target proposal alone adds no executable schemas, fixtures, SQL migrations or changes
to the release-blocking inventory. Implementation freezes each relevant boundary
with schema/fixture/compatibility and real producer-to-consumer proof. Existing
`signal.*` tables remain Runtime-owned; Journal uses its own versioned writer.

## Manual Journal v1 staged API

The first manual slice implements [Journal v1](docs/architecture/product-plane/trade-workspace/journal-v1.md).
`contracts/trade_workspace.py` is the executable DTO authority; its generated
`contracts/schemas/journal.v1.json`, versioned fixtures and schema/DTO parity
and negative tests travel together. The public boundary is
`/api/trade-workspace/v1`: create/change, immutable snapshots, timeline and receipts.
The sole writer is `PostgresJournal`; explicit migration001 owns `journal.*`.
This is a narrower installed contract than the full Workspace target above;
React, normalized Intake, Calculator/Review and data products remain separate.
