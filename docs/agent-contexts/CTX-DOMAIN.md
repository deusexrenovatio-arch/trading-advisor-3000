# CTX-DOMAIN

## Scope
Residual app-plane internals and package metadata not covered by data, research, orchestration, or interface contexts.

## Inside This Context
- Residual domain/package metadata and app-plane behavior that has no narrower data, research, runtime, contract, or interface owner.
- This context is intentionally small and should shrink when a more specific product-plane owner appears.
- Typical questions: is this truly residual domain behavior, or should it route to a narrower context?
- Not inside: broad product-plane changes by default.

## Owned Paths
- `src/trading_advisor_3000/product_plane/domain/`
- `tests/product-plane/test_app_plane_metadata.py`

## Guarded Paths
- `docs/architecture/`
- `scripts/`

## Input/Output Contract
- Input: process-shell validated configuration and contracts.
- Output: residual app-plane behavior or package metadata changes that do not cross shell boundaries.

## Navigation Facets
- residual-domain
- app-metadata

## Search Seeds
- `src/trading_advisor_3000/product_plane/domain/`
- `tests/product-plane/test_app_plane_metadata.py`

## Navigation Notes
- This is a residual context, not a default product-plane bucket.
- Prefer data, research, orchestration, interface, or contract contexts whenever a narrower owner exists.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/test_app_plane_metadata.py -q`
