# Contract Surfaces

This document tracks the current boundary contracts that are versioned in code and fixtures.

## Source Of Truth Rule
- Versioned JSON schema under `src/trading_advisor_3000/app/contracts/schemas/`
- Matching fixtures under `tests/app/fixtures/contracts/`
- Matching round-trip or compatibility tests under `tests/app/contracts/`

## Current Boundary Inventory

| Surface | Contract set | Status | Notes |
| --- | --- | --- | --- |
| Market data | `canonical_bar.v1` | implemented | Baseline bar ingestion/research contract. |
| Signal candidate | `signal_candidate.v1` | implemented | Research -> runtime handoff contract. |
| Runtime signal snapshot | `runtime_signal.v1` | implemented | Durable current-state view for signal lifecycle. |
| Runtime signal event | `signal_event.v1` | implemented | Append-only event history for runtime actions/context/fills. |
| Decision publication | `decision_publication.v1` | implemented | Telegram/publication traceability contract. |
| Execution intent | `order_intent.v1` | implemented | Runtime -> execution boundary. |
| Broker feedback | `broker_order.v1`, `broker_fill.v1`, `broker_event.v1` | implemented | Live/paper execution feedback envelope set. |
| Position and risk | `position_snapshot.v1`, `risk_snapshot.v1` | implemented | Reconciliation and risk snapshots. |
| Sidecar wire API | `docs/architecture/app/sidecar-wire-api-v1.md` | partial | Wire-level HTTP contract exists, real .NET sidecar implementation does not. |

## DB Mapping Baseline
- `runtime_signal.v1` maps to `signal.active_signals`
- `signal_event.v1` maps to `signal.signal_events`
- `decision_publication.v1` maps to `signal.publications`

## Change Rule
Any public payload change must update schema, fixture, and test coverage together.
