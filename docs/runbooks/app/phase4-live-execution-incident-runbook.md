# Phase 4 Live Execution Incident Runbook

## Purpose
Operate the controlled-live execution loop:
`OrderIntent -> bridge submit -> broker sync (orders/fills/positions) -> reconciliation incidents`.

## Preconditions
- Feature flags for live route are explicitly configured.
- Controlled-live bridge health is `ok`.
- Reconciliation is part of the execution cycle (no blind submit-only mode).

## Preflight
1. Verify bridge health reports the expected path:
   - `stocksharp->quik->finam`
2. Confirm no preflight errors (`live_execution_disabled`, `quik_connector_disabled`, etc.).
3. Confirm sidecar readiness is `ready=true`.

## Validation Procedure
1. Run Phase 4 integration test:
   - `python -m pytest tests/app/integration/test_phase4_live_execution_controlled.py -q`
2. Run Phase 4 unit tests:
   - `python -m pytest tests/app/unit/test_phase4_live_bridge.py -q`
   - `python -m pytest tests/app/unit/test_phase4_broker_sync.py -q`
   - `python -m pytest tests/app/unit/test_phase4_reconciliation.py -q`
3. Run full app regression:
   - `python -m pytest tests/app -q`

## Incident Triage Map
- `missing_broker_order_ack`
  - Meaning: intent exists, broker order is not acknowledged.
  - Action: halt new submissions and replay broker order index from event log.
- `stale_order_state_vs_fill`
  - Meaning: fills are present but order state remains `new/submitted`.
  - Action: force state sync from broker snapshots, then re-run reconciliation.
- `filled_state_without_enough_fills`
  - Meaning: order state says `filled`, but cumulative fill qty is insufficient.
  - Action: freeze automatic lifecycle transitions and replay fill ingestion.
- `orphan_fill_without_order`
  - Meaning: fill references unknown broker order.
  - Action: quarantine fill event and rebuild order mapping before applying position updates.
- `quantity_mismatch` / `avg_price_mismatch` / `missing_position` / `unexpected_position`
  - Meaning: local expected positions diverge from observed broker positions.
  - Action: rebuild position ledger from fill log, then verify with broker snapshot.

## Recovery Sequence
1. Freeze new live submissions.
2. Re-ingest broker updates/fills from the last known clean cursor.
3. Recompute positions from fills.
4. Re-run full reconciliation (`orders + fills + positions`).
5. Unfreeze submissions only when reconciliation report is clean or accepted by operator decision.

## Escalation
- Escalate immediately if any high-severity incident repeats after one replay cycle.
- Keep incident evidence (order IDs, fill IDs, reconciliation snapshot) attached to the task note and PR evidence.
