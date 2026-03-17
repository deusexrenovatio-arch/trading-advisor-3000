# Phase 3 - Shadow-Forward and System Integration

## Goal
Deliver a full replayable system slice:
- shadow-forward engine for `research.forward_observations`,
- integrated replay flow `market data -> signal -> publication -> forward outcome`,
- analytics outcomes contract for `analytics.signal_outcomes`,
- first operational runbook for system replay.

## Deliverables
- `src/trading_advisor_3000/app/research/forward/engine.py`
- `src/trading_advisor_3000/app/runtime/analytics/outcomes.py`
- `src/trading_advisor_3000/app/runtime/analytics/system_replay.py`
- `tests/app/unit/test_phase3_forward_engine.py`
- `tests/app/unit/test_phase3_analytics.py`
- `tests/app/integration/test_phase3_system_replay.py`
- `docs/runbooks/app/phase3-system-replay-runbook.md`

## Design Decisions
1. Forward observations are computed from the same `DecisionCandidate` contracts used by runtime.
2. Forward metrics are normalized in R-units (`pnl_r`, `mfe_r`, `mae_r`) with deterministic risk scaling.
3. Analytics outcomes are generated from forward observations through `candidate_id` traceability.
4. Integrated replay reuses existing Phase 2B/2C modules instead of introducing a parallel runtime path.
5. Evidence artifacts are written as deterministic JSONL outputs for repeatable acceptance runs.

## Acceptance Commands
- `python -m pytest tests/app/unit/test_phase3_forward_engine.py -q`
- `python -m pytest tests/app/unit/test_phase3_analytics.py -q`
- `python -m pytest tests/app/integration/test_phase3_system_replay.py -q`
- `python -m pytest tests/app -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Out of Scope
- live broker execution integration and reconciliation incidents handling,
- production job scheduling and distributed orchestration,
- strategy performance dashboards beyond `analytics.signal_outcomes`.
