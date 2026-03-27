# Phase 2B - Research Plane MVP

## Goal
Deliver a reproducible research baseline:
- feature engine with point-in-time guarantees,
- feature/research Delta contracts,
- sample strategy catalog,
- backtest engine with deterministic run IDs,
- candidate outputs persisted as physical Delta tables.

## Deliverables
- `src/trading_advisor_3000/app/research/features/snapshot.py`
- `src/trading_advisor_3000/app/research/features/engine.py`
- `src/trading_advisor_3000/app/research/features/store.py`
- `src/trading_advisor_3000/app/research/strategies/sample.py`
- `src/trading_advisor_3000/app/research/backtest/engine.py`
- `src/trading_advisor_3000/app/research/pipeline.py`
- `tests/app/fixtures/research/canonical_bars_sample.jsonl`
- `tests/app/unit/test_phase2b_features.py`
- `tests/app/unit/test_phase2b_manifests.py`
- `tests/app/integration/test_phase2b_research_plane.py`

## Design Decisions
1. Point-in-time calculations use only rolling history up to the current bar.
2. Feature snapshots include deterministic `snapshot_id` based on contract/time/timeframe/version.
3. Backtest outputs are deterministic and reproducible through stable hashing.
4. Candidate outputs include contract-safe `DecisionCandidate` payloads.
5. Research outputs are materialized as local Delta directories (`*.delta`) with `_delta_log`.
6. Integration coverage reads Delta outputs back through runtime APIs, not manifest-only proofs.

## Acceptance Commands
- `python -m pytest tests/app/unit/test_phase2b_features.py -q`
- `python -m pytest tests/app/unit/test_phase2b_manifests.py -q`
- `python -m pytest tests/app/integration/test_phase2b_research_plane.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Out of Scope
- slippage/commission microstructure modeling,
- distributed Delta/Spark runtime orchestration,
- multi-dataset walk-forward scheduling.
