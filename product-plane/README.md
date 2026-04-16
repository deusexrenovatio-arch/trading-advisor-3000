# Product Plane Hub

Use this hub when the task is about application/runtime capabilities.

## Primary paths
- `src/trading_advisor_3000/*`
- `tests/product-plane/*`
- `docs/architecture/product-plane/*`
- `docs/runbooks/app/*`
- `docs/checklists/app/*`
- `deployment/*`

## First docs to open
1. `docs/architecture/product-plane/STATUS.md`
2. `docs/architecture/product-plane/CONTRACT_SURFACES.md`
3. `docs/architecture/product-plane/research-plane-platform.md`
4. `docs/runbooks/app/research-plane-operations.md`
5. `docs/architecture/repository-surfaces.md`

## Research Plane
The stable research path is now the materialized `phase2b` contour:

`canonical data -> research materialization -> vectorbt backtests -> ranking -> candidate projection -> runtime`

Official operational route:

`python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>`

Use these documents when the task touches research:
- `docs/architecture/product-plane/research-plane-platform.md`
- `docs/runbooks/app/research-campaign-route.md`
- `docs/runbooks/app/research-plane-operations.md`
- `docs/checklists/app/phase2b-acceptance-checklist.md`

Historical note:
- The old snapshot-centric `run_research_from_bars(...)` design is no longer the primary implementation.
- The Spark SQL candidate bridge remains compatibility-only and is not the accepted primary route.
- Low-level bootstrap/backtest/projection jobs remain internal/debug-only and are not the supported front door.

## Boundary reminder
Product-plane changes must not weaken shell governance contracts.
