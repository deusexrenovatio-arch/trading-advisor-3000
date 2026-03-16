# Architecture As Docs

This package defines shell architecture boundaries and governance contracts.

## Core documents
- `docs/architecture/layers.md`
- `docs/architecture/modules.md`
- `docs/architecture/architecture-map.md`
- `docs/architecture/glossary.md`
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/entities-v2.md`
- `docs/architecture/architecture-map-v2.md`

## ADRs
- `docs/architecture/adr/README.md`
- `docs/architecture/adr/0001-shell-boundaries.md`

## Sync flow
- source docs:
  - `docs/architecture/layers-v2.md`
  - `docs/architecture/entities-v2.md`
- generated map:
  - `docs/architecture/architecture-map-v2.md`
- command:
  - `python scripts/sync_architecture_map.py`

## Validation
- `python scripts/validate_architecture_policy.py`
- `python -m pytest tests/architecture -q`
