# Runtime Lifecycle

This document retains the original replay MVP and its lifecycle decisions.
Current implementation status is owned by
[Product Plane Status](docs/architecture/product-plane/STATUS.md); environment
and publication prerequisites are in the
[bootstrap runbook](docs/runbooks/app/bootstrap.md).

## Current successor

- `runtime/bootstrap.py` requires `PostgresSignalStore` for `staging` and
  `production`; missing DSN or an in-memory backend fails closed. Direct
  programmatic/test stacks may still use the in-memory store.
- `runtime/publishing/telegram.py` implements Bot API publish/edit/close/cancel
  when a bot token is supplied. Without a token it uses memory transport;
  channel binding alone is not external Telegram delivery evidence.
- `interfaces/asgi.py` exposes the replay/lifecycle API through the same
  bootstrap. Its `/ready` response confirms an initialized bootstrap context;
  it does not probe ongoing database, Telegram, broker or live-feed readiness.
- Prepared-candidate replay, durable-state tests and external delivery/broker
  acceptance are distinct evidence. The real broker process remains planned
  in Product Plane Status.

Current owners: `runtime/bootstrap.py`, `runtime/signal_store/postgres.py`,
`runtime/publishing/telegram.py`, and `interfaces/asgi.py` under
`src/trading_advisor_3000/product_plane/`.

## Historical replay MVP goal
The original MVP delivered an advisory runtime loop on replay:
- strategy registry with activation lifecycle,
- in-memory signal store and event history,
- Telegram publish/edit/close adapter with idempotent create,
- runtime replay engine and API facade.

## Original MVP deliverables
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

## Focused verification commands
- `python -m pytest tests/product-plane/unit/test_runtime_components.py -q`
- `python -m pytest tests/product-plane/integration/test_runtime_lifecycle.py -q`
- `python -m pytest tests/product-plane/unit/test_durable_runtime_bootstrap.py -q`
- `python -m pytest tests/product-plane/unit/test_runtime_api_smoke.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

These commands exercise scoped implementation contracts. A passing local test
or `/ready` response alone does not establish operational publication readiness.

## Historical MVP exclusions
The original MVP excluded:
- external Telegram API network calls,
- persistent PostgreSQL signal store,
- live trading mode orchestration.

The current successor implements the first two surfaces; their historical
exclusion is not a current prohibition or a claim of missing code. Real broker
lifecycle and production readiness retain the limits in Product Plane Status.
