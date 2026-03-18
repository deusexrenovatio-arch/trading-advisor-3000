# Phase 4 Acceptance Checklist

Date: 2026-03-17

## Acceptance Disposition
- [x] Phase 4 controlled-live execution integration delivered
- [x] Feature-flag gated live route enforced (fail-closed)
- [x] Live sync/reconciliation incident surfacing delivered

## Deliverables
- [x] StockSharp <-> Python controlled bridge added
- [x] QUIK/Finam route represented under feature flags
- [x] Broker sync pipeline added (ack/update/fill -> orders/fills/positions)
- [x] Reconciliation upgraded to orders/fills/positions
- [x] Incident runbook added
- [x] Phase 4 unit/integration tests added

## Acceptance Criteria
- [x] Test/live-sim environment can connect via controlled bridge
- [x] Broker order/fill sync is proven by integration scenario
- [x] Reconciliation incidents are surfaced with recovery actions
- [x] PR gate green

## Evidence Commands
- [x] `python -m pytest tests/app/unit/test_phase4_live_bridge.py -q`
- [x] `python -m pytest tests/app/unit/test_phase4_broker_sync.py -q`
- [x] `python -m pytest tests/app/unit/test_phase4_reconciliation.py -q`
- [x] `python -m pytest tests/app/integration/test_phase4_live_execution_controlled.py -q`
- [x] `python -m pytest tests/app -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`
