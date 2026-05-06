# Architecture As Docs

This package documents architecture for both repository surfaces:
- Delivery Shell architecture (governance/control-plane concerns)
- Product Plane architecture (application/runtime concerns)

Use `docs/architecture/trading-advisor-3000.md` as the single entry map when you
need a coherent whole-system view before reading the more specific documents.

## Start map
- `docs/project-map/current-truth-map-2026-05-05.md`
- `docs/project-map/documentation-currentness-map-2026-05-06.md`
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/repository-surfaces.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/modules.md`

## Delivery Shell architecture index
- `docs/architecture/layers.md`
- `docs/architecture/architecture-map.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/entities-v2.md`
- `docs/architecture/architecture-map-v2.md`
- `docs/architecture/governed-pipeline-hardening-technical-specification.md`
- `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md`

## Product Plane architecture index
- `docs/architecture/product-plane/README.md`
- `docs/architecture/product-plane/STATUS.md`
- `docs/architecture/product-plane/CONTRACT_SURFACES.md`
- `docs/architecture/product-plane/research-plane-platform.md`
- `docs/architecture/product-plane/stack-conformance-baseline.md`

## Retired historical docs
- Retired legacy app-path docs are archived at
  `docs/archive/legacy-app-docs/2026-05-06/README.md`. Use
  `docs/architecture/product-plane/**` for current product-plane truth.
- The old product-plane spec v2 package is archived at
  `docs/archive/product-plane-spec-v2/2026-05-06/README.md`; do not use it as a
  current implementation source.

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
