# Shadow Replay - Shadow-Forward and System Integration

## Goal
Deliver a full replayable system slice:
- shadow-forward engine for `research.forward_observations`,
- integrated replay flow `market data -> signal -> publication -> forward outcome`,
- analytics outcomes contract for `analytics.signal_outcomes`,
- first operational runbook for system replay.

## Deliverables
- `src/trading_advisor_3000/product_plane/research/forward/engine.py`
- `src/trading_advisor_3000/product_plane/runtime/analytics/outcomes.py`
- `src/trading_advisor_3000/product_plane/runtime/analytics/system_replay.py`
- `tests/product-plane/unit/test_shadow_forward_engine.py`
- `tests/product-plane/unit/test_shadow_replay_analytics.py`
- `tests/product-plane/integration/test_shadow_replay_system.py`
- `docs/runbooks/app/shadow-replay-runbook.md`

## Design Decisions
1. Forward observations are computed from the same `DecisionCandidate` contracts used by runtime.
2. Forward metrics are normalized in R-units (`pnl_r`, `mfe_r`, `mae_r`) with deterministic risk scaling.
3. Analytics outcomes are generated from forward observations through the same `candidate_id` formula as Research Plane research candidates.
4. Integrated replay builds forward/outcomes only from runtime-accepted and published signal IDs.
5. Evidence artifacts are written as deterministic JSONL outputs for repeatable acceptance runs.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_shadow_forward_engine.py -q`
- `python -m pytest tests/product-plane/unit/test_shadow_replay_analytics.py -q`
- `python -m pytest tests/product-plane/integration/test_shadow_replay_system.py -q`
- `python -m pytest tests/product-plane -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Out of Scope
- live broker execution integration and reconciliation incidents handling,
- production job scheduling and distributed orchestration,
- strategy performance dashboards beyond `analytics.signal_outcomes`.
