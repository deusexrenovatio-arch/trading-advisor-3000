# CTX-API-UI

## Scope
Delivery interfaces, API surfaces, and operator-facing integration boundaries.

## Owned Paths
- `src/trading_advisor_3000/product_plane/interfaces/`
- `tests/product-plane/integration/test_phase4_live_execution_controlled.py`
- `tests/product-plane/unit/test_phase4_live_bridge.py`
- `tests/product-plane/unit/test_phase5_observability_export.py`

## Guarded Paths
- `src/trading_advisor_3000/product_plane/contracts/`
- `src/trading_advisor_3000/product_plane/domain/`
- `src/trading_advisor_3000/product_plane/data_plane/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/integration/test_phase4_live_execution_controlled.py -q`
