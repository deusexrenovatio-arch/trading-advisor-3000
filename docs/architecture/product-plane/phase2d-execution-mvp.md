# Phase 2D - Execution MVP

## Goal
Deliver a paper-execution baseline:
- extended execution contracts (`BrokerOrder`, `BrokerFill`, `RiskSnapshot`, `BrokerEvent`),
- paper broker flow from `OrderIntent` to `PositionSnapshot`,
- StockSharp sidecar integration stub,
- broker event log and reconciliation skeleton.

## Deliverables
- `src/trading_advisor_3000/product_plane/contracts/execution.py`
- `src/trading_advisor_3000/product_plane/contracts/schemas/broker_order.v1.json`
- `src/trading_advisor_3000/product_plane/contracts/schemas/broker_fill.v1.json`
- `src/trading_advisor_3000/product_plane/contracts/schemas/risk_snapshot.v1.json`
- `src/trading_advisor_3000/product_plane/contracts/schemas/broker_event.v1.json`
- `src/trading_advisor_3000/product_plane/execution/intents/paper_broker.py`
- `src/trading_advisor_3000/product_plane/execution/adapters/stocksharp_sidecar_stub.py`
- `src/trading_advisor_3000/product_plane/execution/reconciliation/reconcile.py`
- `tests/product-plane/contracts/test_phase2d_execution_contracts.py`
- `tests/product-plane/integration/test_phase2d_execution.py`
- `tests/product-plane/unit/test_phase2d_reconciliation.py`

## Design Decisions
1. Execution contracts remain plain typed dataclasses with strict payload validation.
2. Paper execution is idempotent per `intent_id`.
3. Broker event log records intent/order/fill/position events in deterministic order.
4. Reconciliation compares expected vs observed snapshots by position key.
5. Sidecar stub keeps execution bridge behavior isolated from runtime strategy logic.

## Acceptance Commands
- `python -m pytest tests/product-plane/contracts/test_phase2d_execution_contracts.py -q`
- `python -m pytest tests/product-plane/integration/test_phase2d_execution.py -q`
- `python -m pytest tests/product-plane/unit/test_phase2d_reconciliation.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Out of Scope
- live broker network transport,
- real StockSharp process orchestration,
- automatic incident remediation.
