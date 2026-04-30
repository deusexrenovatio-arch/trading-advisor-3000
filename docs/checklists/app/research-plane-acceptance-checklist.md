# Research Plane Acceptance Checklist

Date: 2026-03-16

## Current Addendum (updated 2026-04-14)
- [x] Public `run_research_from_bars(...)` now routes through the materialized research path
- [x] Legacy Spark candidate SQL path has been removed from the active branch and is no longer part of acceptance truth
- [x] Derived feature layer materializes helper labels and ATR reference outputs
- [x] Canonical operational route is `python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>`
- [x] Campaign runner is the only operator-facing research route; scheduled freshness stays Dagster-owned
- [x] Active backtest inputs are read through native Delta/Arrow predicates and strategy-column projection before Python/vectorbt matrix construction; Python row-object Delta reloaders are not accepted as a fallback for the battle contour
- [x] Benchmark artifacts now exist as committed JSON, markdown, and cache-marker evidence
- Stable references:
  `docs/architecture/product-plane/research-plane-platform.md`
  `docs/runbooks/app/research-campaign-route.md`
  `docs/runbooks/app/research-plane-operations.md`

## Acceptance Disposition (updated 2026-03-17)
- [x] Research Plane accepted as MVP
- [x] Research Plane accepted as full module DoD

## Baseline Resolution (closed 2026-03-17)
- Backtest now supports walk-forward windows.
- Commissions, slippage, and session filters are integrated in research execution.
- Strategy metrics are persisted as explicit research outputs and reflected in manifest/tests.

## Deliverables
- [x] Feature engine added
- [x] Feature/research Delta contract manifest added
- [x] Backtest engine added
- [x] Reference strategy implementations added
- [x] Candidate outputs persisted in committed research artifacts
- [x] Phase 2B unit/integration tests added

## Acceptance Criteria
- [x] Backtest reproducible
- [x] Point-in-time tests green
- [x] Research outputs written to Delta-compatible artifacts
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/product-plane/unit/test_research_dagster_manifests.py -q`
- [x] `python -m pytest tests/product-plane/unit/test_research_feature_layer.py -q`
- [x] `python -m pytest tests/product-plane/unit/test_research_benchmark_artifacts.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_materialized_research_plane.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_research_dagster_jobs.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_research_campaign_route.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_research_benchmark_job.py -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
