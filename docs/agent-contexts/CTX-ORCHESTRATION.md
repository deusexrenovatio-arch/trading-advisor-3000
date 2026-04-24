# CTX-ORCHESTRATION

## Scope
Runtime wiring, execution flow orchestration, and cross-subsystem coordination.

## Owned Paths
- `src/trading_advisor_3000/__main__.py`
- `src/trading_advisor_3000/app_metadata.py`
- `src/trading_advisor_3000/product_plane/`
- `src/trading_advisor_3000/product_plane/runtime/`
- `src/trading_advisor_3000/product_plane/execution/`
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

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/integration/test_runtime_lifecycle.py -q`
