# Phase 4 - Live Execution Integration (Controlled)

## Goal
Close the controlled-live execution slice with explicit broker bridge controls:
- Python <-> StockSharp bridge contract hardened,
- QUIK/Finam route enabled only under feature flags,
- live broker sync for orders/fills/positions,
- reconciliation incidents surfaced with deterministic recovery actions,
- operational runbook for incident handling.

## Deliverables
- `src/trading_advisor_3000/product_plane/execution/adapters/live_bridge.py`
- `src/trading_advisor_3000/product_plane/execution/broker_sync/live_sync.py`
- `src/trading_advisor_3000/product_plane/execution/broker_sync/controlled_live.py`
- `src/trading_advisor_3000/product_plane/execution/reconciliation/reconcile.py` (extended live reconciliation)
- `tests/product-plane/unit/test_phase4_live_bridge.py`
- `tests/product-plane/unit/test_phase4_broker_sync.py`
- `tests/product-plane/unit/test_phase4_reconciliation.py`
- `tests/product-plane/integration/test_phase4_live_execution_controlled.py`
- `docs/runbooks/app/phase4-live-execution-incident-runbook.md`

## Design Decisions
1. Live path is fail-closed by default: all live feature flags must be enabled to allow submission in `mode=live`.
2. Bridge remains transport-neutral at contract level (`OrderIntent` / `BrokerOrder` / `BrokerFill` / `PositionSnapshot`), while physical path is represented as `stocksharp->quik->finam`.
3. Broker sync ingestion is idempotent and event-backed: duplicate fills are ignored, order/fill/position state is rebuilt from sync events, and incidents are emitted for unmapped/invalid states.
4. Reconciliation is upgraded from position-only checks to full live checks (`orders + fills + positions`) with explicit incident severity and recovery actions.
5. Controlled cycle orchestration (`submit -> sync -> reconcile`) is isolated from strategy/runtime logic to preserve plane boundaries.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_phase4_live_bridge.py -q`
- `python -m pytest tests/product-plane/unit/test_phase4_broker_sync.py -q`
- `python -m pytest tests/product-plane/unit/test_phase4_reconciliation.py -q`
- `python -m pytest tests/product-plane/integration/test_phase4_live_execution_controlled.py -q`
- `python -m pytest tests/product-plane -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Out of Scope
- direct network transport to real broker endpoints,
- autonomous incident remediation without operator confirmation,
- production scheduler rollout and infra topology hardening (Phase 5+).
