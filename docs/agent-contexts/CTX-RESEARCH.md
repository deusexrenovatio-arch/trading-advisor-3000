# CTX-RESEARCH

## Scope
Research pipelines, feature generation, and analysis-oriented execution.

## Owned Paths
- `src/trading_advisor_3000/app/research/`
- `src/trading_advisor_3000/spark_jobs/`
- `src/trading_advisor_3000/dagster_defs/phase2b_assets.py`
- `tests/app/integration/test_phase2b_research_plane.py`
- `tests/app/unit/test_phase2b_features.py`
- `tests/app/unit/test_phase2b_manifests.py`

## Guarded Paths
- `src/trading_advisor_3000/app/runtime/`
- `src/trading_advisor_3000/app/interfaces/`
- `src/trading_advisor_3000/app/contracts/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/app/integration/test_phase2b_research_plane.py -q`
