# CTX-API-UI

## Scope
Delivery interfaces, API surfaces, and operator-facing integration boundaries.

## Inside This Context
- Operator-facing endpoints, live bridge behavior, observability exports, and interface integration tests.
- This context owns how users and operational surfaces consume product-plane behavior.
- Typical questions: which endpoint exposes this capability, how does live execution bridge data, what does the operator see?
- Not inside: internal contracts, domain rules, data production, or research materialization unless interface behavior changes with them.

## Owned Paths
- `src/trading_advisor_3000/product_plane/interfaces/`
- `tests/product-plane/integration/test_controlled_live_execution.py`
- `tests/product-plane/unit/test_live_bridge.py`
- `tests/product-plane/unit/test_observability_export.py`

## Guarded Paths
- `src/trading_advisor_3000/product_plane/contracts/`
- `src/trading_advisor_3000/product_plane/domain/`
- `src/trading_advisor_3000/product_plane/data_plane/`

## Navigation Facets
- operator-api
- live-bridge
- observability-export

## Search Seeds
- `src/trading_advisor_3000/product_plane/interfaces/`
- `tests/product-plane/integration/test_controlled_live_execution.py`
- `tests/product-plane/unit/test_live_bridge.py`

## Navigation Notes
- Start here for operator-facing behavior, API contracts as consumed by operators, and live bridge behavior.
- Route contract ownership back to `CTX-CONTRACTS` when schemas or public payloads change.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/integration/test_controlled_live_execution.py -q`
