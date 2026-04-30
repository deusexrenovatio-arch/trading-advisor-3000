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
The stable research path is now the materialized research contour:

`canonical data -> research materialization -> vectorbt backtests -> ranking -> candidate projection -> runtime`

Backtest input reads are Delta-native: the campaign route filters Delta tables and projects only required strategy columns before Python builds vectorbt matrices. Python row-object reloaders are not a supported fallback for the active research/backtest contour.

Official operational route:

`python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>`

Use these documents when the task touches research:
- `docs/architecture/product-plane/research-plane-platform.md`
- `docs/runbooks/app/research-campaign-route.md`
- `docs/runbooks/app/research-plane-operations.md`
- `docs/checklists/app/phase2b-acceptance-checklist.md`

Historical note:
- `run_research_from_bars(...)` remains only as a thin programmatic adapter over the materialized research route.
- Operator-facing execution stays on `run_campaign`, while scheduled freshness remains Dagster-owned through `research_data_prep_job`.

## Boundary reminder
Product-plane changes must not weaken shell governance contracts.
