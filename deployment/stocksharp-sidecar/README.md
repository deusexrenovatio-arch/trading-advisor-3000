# StockSharp Sidecar Stub

Phase 2D provides a contract-level sidecar stub for local execution flow validation.

## Current stub behavior
- Accepts `OrderIntent` payload envelopes.
- Returns deterministic acknowledgement with `external_order_id`.
- Exposes in-memory queue via adapter API for integration tests.

## Scope boundary
- No live broker transport.
- No external process management.
- No credentials/secrets in repository.
