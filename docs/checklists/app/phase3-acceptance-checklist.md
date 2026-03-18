# Phase 3 Acceptance Checklist

Date: 2026-03-17

## Acceptance Disposition (updated 2026-03-17)
- [x] Phase 3 accepted as MVP shadow-forward slice
- [x] Phase 3 accepted as final target-architecture closure

## MVP-Positive Corrections Confirmed
- `candidate_id` is deterministic and consistent between research and forward flows.
- Integrated replay builds forward/outcomes only for runtime-accepted and published signals.
- These are validated in code and integration tests.

## Full DoD Resolution (closed 2026-03-17)
- `analytics.signal_outcomes` is now built from `signal.signal_events` + `execution.broker_fills` + `execution.positions`.
- Runtime -> forward hand-off uses explicit `candidate_id` transfer from runtime replay report, not only implicit recomputation.

## Deliverables
- [x] Shadow-forward engine added
- [x] Integrated replay scenario added (`market -> signal -> publication -> forward outcome`)
- [x] Analytics outcomes contract and builder added
- [x] Phase 3 unit/integration tests added
- [x] First system runbook added

## Acceptance Criteria
- [x] End-to-end replay green
- [x] Analytics outcome produced
- [x] PR gate green

## Evidence Commands
- [x] `python -m pytest tests/app/unit/test_phase3_forward_engine.py -q`
- [x] `python -m pytest tests/app/unit/test_phase3_analytics.py -q`
- [x] `python -m pytest tests/app/integration/test_phase3_system_replay.py -q`
- [x] `python -m pytest tests/app -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`
