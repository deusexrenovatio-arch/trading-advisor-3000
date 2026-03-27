# Phase 2C Acceptance Checklist

Date: 2026-03-16

## Acceptance Disposition (updated 2026-03-17)
- [x] Phase 2C accepted as MVP baseline slice
- [x] Full module DoD closure is not accepted in the current truth source (`docs/architecture/app/STATUS.md`)

## Baseline Evidence Snapshot (captured 2026-03-17; not full closure)
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
- [x] Phase 2C unit/integration tests added

## Acceptance Criteria
- [x] Advisory signal lifecycle works end-to-end on replay
- [x] Runtime tests green
- [x] Idempotent publication confirmed
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/app/unit/test_phase2c_runtime_components.py -q`
- [x] `python -m pytest tests/app/integration/test_phase2c_runtime.py -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## F1 Re-Acceptance Snapshot (2026-03-27)
- [x] Phase 2C remains baseline accepted for runtime lifecycle behavior and does not claim full product closure.
- [x] Registry mapping is explicit: this slice contributes to `runtime_signal_lifecycle` (`implemented`) and bounded parts of `durable_runtime_state` (`implemented` with phase-06 proof).
- [x] Restart/recovery and fallback-denial behavior are tracked in later phase evidence and remain part of re-acceptance review.
- [x] Checklist wording remains aligned with the restricted acceptance vocabulary.
