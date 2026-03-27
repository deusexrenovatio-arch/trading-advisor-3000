# Phase 2D Acceptance Checklist

Date: 2026-03-16

## Acceptance Disposition (updated 2026-03-17)
- [x] Phase 2D accepted as skeleton/MVP
- [x] Full sidecar/reconciliation DoD closure is not accepted in the current truth source (`docs/architecture/app/STATUS.md`)

## Baseline Evidence Snapshot (captured 2026-03-17; not full closure)
- Sidecar now supports submit/cancel/replace operations.
- Reverse broker update/fill ingestion path is present in stub contract flow.
- Reconciliation upgraded from detector to incident model with explicit recovery actions and tests.

## Deliverables
- [x] Execution contracts (`BrokerOrder`, `BrokerFill`, `RiskSnapshot`, `BrokerEvent`) added
- [x] Paper broker mode added
- [x] StockSharp sidecar stub added
- [x] Broker event log added
- [x] Reconciliation skeleton added
- [x] Phase 2D contract/unit/integration tests added

## Acceptance Criteria
- [x] Paper mode works from `OrderIntent` to `PositionSnapshot`
- [x] Contract tests green
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/app/contracts/test_phase2d_execution_contracts.py -q`
- [x] `python -m pytest tests/app/integration/test_phase2d_execution.py -q`
- [x] `python -m pytest tests/app/unit/test_phase2d_reconciliation.py -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## F1 Re-Acceptance Snapshot (2026-03-27)
- [x] Phase 2D remains baseline accepted for execution contracts and paper/stub flow only.
- [x] Registry mapping stays explicit: `paper_execution_path` is implemented while real broker process remains `planned`.
- [x] Contract and reconciliation evidence remains valid without promoting this phase to real broker closure.
- [x] Checklist wording remains aligned with current truth-source constraints.
