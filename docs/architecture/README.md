# Architecture As Docs

This package documents architecture for both repository surfaces:
- Delivery Shell architecture (governance/control-plane concerns)
- Product Plane architecture (application/runtime concerns)

## Start map
- `docs/architecture/repository-surfaces.md`
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/modules.md`

## Delivery Shell architecture index
- `docs/architecture/layers.md`
- `docs/architecture/architecture-map.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/entities-v2.md`
- `docs/architecture/architecture-map-v2.md`
- `docs/architecture/governed-pipeline-hardening-technical-specification.md`

## Product Plane architecture index
- `docs/architecture/app/README.md`
- `docs/architecture/app/STATUS.md`
- `docs/architecture/app/CONTRACT_SURFACES.md`
- `docs/architecture/app/product-plane-spec-v2/README.md`

## Shared references
- `docs/architecture/glossary.md`
- `docs/architecture/trading-advisor-3000.md`

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
