# Runtime Lifecycle - Runtime MVP

## Goal
Deliver an advisory runtime loop on replay:
- strategy registry with activation lifecycle,
- in-memory signal store and event history,
- Telegram publish/edit/close adapter with idempotent create,
- runtime replay engine and API facade.

## Deliverables
- `src/trading_advisor_3000/product_plane/runtime/config/registry.py`
- `src/trading_advisor_3000/product_plane/runtime/signal_store/store.py`
- `src/trading_advisor_3000/product_plane/runtime/publishing/telegram.py`
- `src/trading_advisor_3000/product_plane/runtime/decision/engine.py`
- `src/trading_advisor_3000/product_plane/runtime/pipeline.py`
- `src/trading_advisor_3000/product_plane/interfaces/api/runtime_api.py`
- `tests/product-plane/unit/test_runtime_components.py`
- `tests/product-plane/integration/test_runtime_lifecycle.py`

## Design Decisions
1. Runtime accepts only candidates allowed by active strategy versions.
2. Signal lifecycle is explicit in store state and mirrored by signal events.
3. Telegram publication create is idempotent by `signal_id`.
4. Replay order is deterministic by `(ts_decision, signal_id)`.
5. Runtime API returns replay report plus current state snapshots for acceptance evidence.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_runtime_components.py -q`
- `python -m pytest tests/product-plane/integration/test_runtime_lifecycle.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Out of Scope
- external Telegram API network calls,
- persistent PostgreSQL signal store,
- live trading mode orchestration.
