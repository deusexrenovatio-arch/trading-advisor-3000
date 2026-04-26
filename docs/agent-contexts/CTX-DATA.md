# CTX-DATA

## Scope
Data ingestion, normalization, and canonical data-plane contracts.

## Inside This Context
- Provider ingestion, canonical storage, Delta runtime helpers, manifests, migrations, and data quality checks.
- This is the first context for authoritative data roots, table materialization, canonical bars, and storage verification.
- Typical questions: where does canonical data live, which table/path is authoritative, what quality gate proves the data?
- Not inside: research feature semantics, runtime execution orchestration, or operator API behavior unless they consume data outputs.

## Owned Paths
- `src/trading_advisor_3000/product_plane/data_plane/`
- `src/trading_advisor_3000/migrations/`
- `tests/product-plane/integration/test_historical_data_plane.py`
- `tests/product-plane/unit/test_historical_data_builder.py`
- `tests/product-plane/unit/test_historical_data_manifests.py`
- `tests/product-plane/unit/test_historical_data_quality.py`
- `tests/product-plane/unit/test_delta_*.py`
- `tests/product-plane/fixtures/data_plane/`

## Guarded Paths
- `src/trading_advisor_3000/product_plane/research/`
- `src/trading_advisor_3000/product_plane/runtime/`
- `src/trading_advisor_3000/product_plane/interfaces/`

## Navigation Facets
- ingestion
- canonical-storage
- quality
- delta-runtime

## Search Seeds
- `src/trading_advisor_3000/product_plane/data_plane/`
- `src/trading_advisor_3000/product_plane/data_plane/delta_runtime.py`
- `tests/product-plane/unit/test_historical_data_*.py`
- `tests/product-plane/unit/test_delta_*.py`

## Navigation Notes
- Start here for storage, canonical data, Delta runtime, provider normalization, and data quality tasks.
- Open research or runtime context only when changed files cross those boundaries.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/integration/test_historical_data_plane.py -q`
