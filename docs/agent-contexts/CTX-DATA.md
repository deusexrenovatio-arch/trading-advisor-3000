# CTX-DATA

## Scope
Data ingestion, normalization, and canonical data-plane contracts.

## Owned Paths
- `src/trading_advisor_3000/app/data_plane/`
- `src/trading_advisor_3000/migrations/`
- `tests/app/integration/test_phase2a_data_plane.py`
- `tests/app/unit/test_phase2a_builder.py`
- `tests/app/unit/test_phase2a_manifests.py`
- `tests/app/unit/test_phase2a_quality.py`
- `tests/app/fixtures/data_plane/`

## Guarded Paths
- `src/trading_advisor_3000/app/research/`
- `src/trading_advisor_3000/app/runtime/`
- `src/trading_advisor_3000/app/interfaces/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/app/integration/test_phase2a_data_plane.py -q`
