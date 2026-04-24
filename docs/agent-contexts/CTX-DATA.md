# CTX-DATA

## Scope
Data ingestion, normalization, and canonical data-plane contracts.

## Owned Paths
- `src/trading_advisor_3000/product_plane/data_plane/`
- `src/trading_advisor_3000/migrations/`
- `tests/product-plane/integration/test_historical_data_plane.py`
- `tests/product-plane/unit/test_historical_data_builder.py`
- `tests/product-plane/unit/test_historical_data_manifests.py`
- `tests/product-plane/unit/test_historical_data_quality.py`
- `tests/product-plane/fixtures/data_plane/`

## Guarded Paths
- `src/trading_advisor_3000/product_plane/research/`
- `src/trading_advisor_3000/product_plane/runtime/`
- `src/trading_advisor_3000/product_plane/interfaces/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/integration/test_historical_data_plane.py -q`
