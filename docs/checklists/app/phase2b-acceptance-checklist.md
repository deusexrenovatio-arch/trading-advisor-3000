# Phase 2B Acceptance Checklist

Date: 2026-03-16

## Current Addendum (updated 2026-04-14)
- [x] Public `run_research_from_bars(...)` now routes through the materialized phase2b path
- [x] Legacy Spark candidate SQL path is compatibility-only and no longer part of acceptance truth
- [x] Derived feature layer materializes helper labels and ATR reference outputs
- [x] Separate CLI jobs exist for bootstrap, backtest, projection, and benchmark
- [x] Benchmark artifacts now exist as committed JSON, markdown, and cache-marker evidence
- Stable references:
  `docs/architecture/product-plane/research-plane-platform.md`
  `docs/runbooks/app/research-plane-operations.md`

## Acceptance Disposition (updated 2026-03-17)
- [x] Phase 2B accepted as MVP
- [x] Phase 2B accepted as full module DoD

## Baseline Resolution (closed 2026-03-17)
- Backtest now supports walk-forward windows.
- Commissions, slippage, and session filters are integrated in research execution.
- Strategy metrics are persisted as explicit research outputs and reflected in manifest/tests.

## Deliverables
- [x] Feature engine added
- [x] Feature/research Delta contract manifest added
- [x] Backtest engine added
- [x] Sample strategy implementations added
- [x] Candidate outputs persisted in sample research artifacts
- [x] Phase 2B unit/integration tests added

## Acceptance Criteria
- [x] Backtest reproducible
- [x] Point-in-time tests green
- [x] Research outputs written to Delta-compatible artifacts
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/product-plane/unit/test_phase2b_manifests.py -q`
- [x] `python -m pytest tests/product-plane/unit/test_research_feature_layer.py -q`
- [x] `python -m pytest tests/product-plane/unit/test_research_benchmark_artifacts.py -q`
- [x] `python -m pytest tests/product-plane/unit/test_research_legacy_deprecation.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_phase2b_dagster_bootstrap.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_research_jobs_cli.py -q`
- [x] `python -m pytest tests/product-plane/integration/test_research_benchmark_job.py -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
