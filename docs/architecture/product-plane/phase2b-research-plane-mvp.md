# Phase 2B - Research Plane MVP

## Goal
Deliver a reproducible research baseline:
- feature engine with point-in-time guarantees,
- feature/research Delta contracts,
- sample strategy catalog,
- backtest engine with deterministic run IDs,
- candidate outputs persisted to research artifacts.

## Deliverables
- `src/trading_advisor_3000/product_plane/research/features/snapshot.py`
- `src/trading_advisor_3000/product_plane/research/features/engine.py`
- `src/trading_advisor_3000/product_plane/research/features/store.py`
- `src/trading_advisor_3000/product_plane/research/strategies/sample.py`
- `src/trading_advisor_3000/product_plane/research/backtest/engine.py`
- `src/trading_advisor_3000/product_plane/research/pipeline.py`
- `tests/product-plane/fixtures/research/canonical_bars_sample.jsonl`
- `tests/product-plane/unit/test_phase2b_features.py`
- `tests/product-plane/unit/test_phase2b_manifests.py`
- `tests/product-plane/integration/test_phase2b_research_plane.py`

## Design Decisions
1. Point-in-time calculations use only rolling history up to the current bar.
2. Feature snapshots include deterministic `snapshot_id` based on contract/time/timeframe/version.
3. Backtest outputs are deterministic and reproducible through stable hashing.
4. Candidate outputs include contract-safe `DecisionCandidate` payloads.
5. Research artifacts are written as Delta-compatible JSONL sample outputs for acceptance replay.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_phase2b_features.py -q`
- `python -m pytest tests/product-plane/unit/test_phase2b_manifests.py -q`
- `python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Out of Scope
- slippage/commission microstructure modeling,
- distributed Delta/Spark runtime orchestration,
- multi-dataset walk-forward scheduling.
