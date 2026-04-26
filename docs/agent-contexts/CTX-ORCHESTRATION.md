# CTX-ORCHESTRATION

## Scope
Runtime wiring, execution flow orchestration, and cross-subsystem coordination.

## Inside This Context
- Runtime lifecycle, execution coordination, product-plane config/common helpers, Dagster wiring, and process-level flow composition.
- This context connects subsystems but should not absorb their inner domain logic.
- Typical questions: how is a product route started, what coordinates execution, which runtime profile or Dagster asset wires the flow?
- Not inside: data-plane internals, research semantics, contracts, or interfaces when those narrower contexts own the changed files.

## Owned Paths
- `src/trading_advisor_3000/__main__.py`
- `src/trading_advisor_3000/app_metadata.py`
- `src/trading_advisor_3000/product_plane/runtime/`
- `src/trading_advisor_3000/product_plane/execution/`
- `src/trading_advisor_3000/product_plane/common/`
- `src/trading_advisor_3000/product_plane/config/`
- `src/trading_advisor_3000/dagster_defs/`
- `tests/product-plane/integration/test_runtime_lifecycle.py`
- `tests/product-plane/integration/test_execution_flow.py`
- `tests/product-plane/integration/test_shadow_replay_system.py`
- `tests/product-plane/integration/test_review_observability.py`
- `tests/product-plane/integration/test_operational_hardening.py`

## Guarded Paths
- `src/trading_advisor_3000/product_plane/contracts/`
- `src/trading_advisor_3000/product_plane/interfaces/`
- `src/trading_advisor_3000/product_plane/research/`

## Navigation Facets
- runtime
- execution-flow
- config
- dagster-wiring

## Search Seeds
- `src/trading_advisor_3000/product_plane/runtime/`
- `src/trading_advisor_3000/product_plane/execution/`
- `src/trading_advisor_3000/product_plane/config/`
- `tests/product-plane/integration/test_runtime_lifecycle.py`

## Navigation Notes
- Do not use this context as a catch-all for `product_plane/`.
- Data, research, interfaces, and contracts should route to their narrower cards first.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/integration/test_runtime_lifecycle.py -q`
