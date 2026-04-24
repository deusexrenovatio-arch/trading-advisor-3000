# CTX-RESEARCH

## Scope
Research pipelines, feature generation, and analysis-oriented execution.

## Owned Paths
- `src/trading_advisor_3000/product_plane/research/`
- `src/trading_advisor_3000/spark_jobs/`
- `src/trading_advisor_3000/dagster_defs/research_assets.py`
- `tests/product-plane/integration/test_materialized_research_plane.py`
- `tests/product-plane/integration/test_research_campaign_route.py`
- `tests/product-plane/integration/test_research_dagster_jobs.py`
- `tests/product-plane/integration/test_research_benchmark_job.py`
- `tests/product-plane/unit/test_research_feature_layer.py`
- `tests/product-plane/unit/test_research_benchmark_artifacts.py`
- `tests/product-plane/unit/test_research_dagster_manifests.py`
- `docs/architecture/product-plane/research-plane-platform.md`
- `docs/runbooks/app/research-plane-operations.md`
- `docs/checklists/app/phase2b-acceptance-checklist.md`

## Guarded Paths
- `src/trading_advisor_3000/product_plane/runtime/`
- `src/trading_advisor_3000/product_plane/interfaces/`
- `src/trading_advisor_3000/product_plane/contracts/`

## Primary Review Route
Use this order when reviewing or accepting research-plane changes:
1. confirm the stable architecture map in `docs/architecture/product-plane/research-plane-platform.md`
2. confirm the acceptance chain in `docs/checklists/app/phase2b-acceptance-checklist.md`
3. use `test_research_dagster_jobs.py` as the main orchestration proof
4. use `test_research_campaign_route.py` for operator-route proof
5. use `test_research_benchmark_job.py` plus `test_research_benchmark_artifacts.py` for benchmark evidence

Important:
- do not use the old MVP-only proof chain as the default acceptance route
- do not treat a table merely existing as sufficient Stage 7 proof
- use the materialized path as the primary route and treat legacy research bridges as compatibility-only

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python -m pytest tests/product-plane/integration/test_materialized_research_plane.py -q`
- `python -m pytest tests/product-plane/integration/test_research_dagster_jobs.py -q`
- `python -m pytest tests/product-plane/integration/test_research_campaign_route.py -q`
