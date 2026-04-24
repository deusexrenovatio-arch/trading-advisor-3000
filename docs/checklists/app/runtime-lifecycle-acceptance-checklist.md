# Runtime Lifecycle Acceptance Checklist

Date: 2026-03-16

## Acceptance Disposition (updated 2026-03-17)
- [x] Runtime Lifecycle accepted as MVP
- [x] Runtime Lifecycle accepted as full module DoD

## Baseline Resolution (closed 2026-03-17)
- Runtime now sets and enforces validity/expiry windows.
- Cooldown and blackout gates are implemented in replay acceptance path.
- Publishing lifecycle supports create/edit/close/cancel.
- Signal lifecycle is no longer overwritten by publication status; state machines are separated.

## Deliverables
- [x] Strategy registry added
- [x] Signal runtime replay engine added
- [x] Signal store and signal event log added
- [x] Telegram publish/edit/close adapter added
- [x] Runtime API facade added
- [x] Runtime Lifecycle unit/integration tests added

## Acceptance Criteria
- [x] Advisory signal lifecycle works end-to-end on replay
- [x] Runtime tests green
- [x] Idempotent publication confirmed
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/product-plane/unit/test_runtime_components.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_runtime_lifecycle.py -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
