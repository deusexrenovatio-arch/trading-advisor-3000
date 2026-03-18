# Phase 2C - Runtime MVP

## Goal
Deliver an advisory runtime loop on replay:
- strategy registry with activation lifecycle,
- in-memory signal store and event history,
- Telegram publish/edit/close adapter with idempotent create,
- runtime replay engine and API facade.

## Deliverables
- `src/trading_advisor_3000/app/runtime/config/registry.py`
- `src/trading_advisor_3000/app/runtime/signal_store/store.py`
- `src/trading_advisor_3000/app/runtime/publishing/telegram.py`
- `src/trading_advisor_3000/app/runtime/decision/engine.py`
- `src/trading_advisor_3000/app/runtime/pipeline.py`
- `src/trading_advisor_3000/app/interfaces/api/runtime_api.py`
- `tests/app/unit/test_phase2c_runtime_components.py`
- `tests/app/integration/test_phase2c_runtime.py`

## Design Decisions
1. Runtime accepts only candidates allowed by active strategy versions.
2. Signal lifecycle is explicit in store state and mirrored by signal events.
3. Telegram publication create is idempotent by `signal_id`.
4. Replay order is deterministic by `(ts_decision, signal_id)`.
5. Runtime API returns replay report plus current state snapshots for acceptance evidence.

## Acceptance Commands
- `python -m pytest tests/app/unit/test_phase2c_runtime_components.py -q`
- `python -m pytest tests/app/integration/test_phase2c_runtime.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Out of Scope
- external Telegram API network calls,
- persistent PostgreSQL signal store,
- live trading mode orchestration.
