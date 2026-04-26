# Research Plane - Research Plane MVP

Historical note:
- This page captures the original MVP closure context.
- The current stable research-plane map lives in `docs/architecture/product-plane/research-plane-platform.md`.
- The operational execution path lives in `docs/runbooks/app/research-plane-operations.md`.

## Goal
Deliver a reproducible research baseline:
- indicator and derived indicator engine with point-in-time guarantees,
- indicator/research Delta contracts,
- reference strategy set,
- backtest engine with deterministic run IDs,
- candidate outputs persisted to research artifacts.

## Deliverables
- `src/trading_advisor_3000/product_plane/research/indicators/store.py`
- `src/trading_advisor_3000/product_plane/research/derived_indicators/materialize.py`
- `src/trading_advisor_3000/product_plane/research/derived_indicators/store.py`
- `src/trading_advisor_3000/product_plane/research/strategies/reference.py`
- `src/trading_advisor_3000/product_plane/research/backtests/engine.py`
- `src/trading_advisor_3000/product_plane/research/pipeline.py`
- `tests/product-plane/fixtures/research/canonical_bars_sample.jsonl`
- `tests/product-plane/unit/test_research_derived_indicator_layer.py`
- `tests/product-plane/unit/test_research_dagster_manifests.py`
- `tests/product-plane/integration/test_materialized_research_plane.py`

## Design Decisions
1. Point-in-time calculations use only rolling history up to the current bar.
2. Indicator and derived indicator frames carry deterministic dataset, profile, and source-hash lineage.
3. Backtest outputs are deterministic and reproducible through stable hashing.
4. Candidate outputs include contract-safe `DecisionCandidate` payloads.
5. Research artifacts are written as Delta-compatible acceptance replay outputs.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_research_derived_indicator_layer.py -q`
- `python -m pytest tests/product-plane/unit/test_research_dagster_manifests.py -q`
- `python -m pytest tests/product-plane/integration/test_materialized_research_plane.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Out of Scope
- slippage/commission microstructure modeling,
- distributed Delta/Spark runtime orchestration,
- multi-dataset walk-forward scheduling.
