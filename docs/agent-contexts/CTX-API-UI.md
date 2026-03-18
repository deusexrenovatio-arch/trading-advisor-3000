# CTX-API-UI

## Scope
Delivery interfaces, API surfaces, and operator-facing integration boundaries.

## Owned Paths
- `src/trading_advisor_3000/app/interfaces/`
- `tests/app/integration/test_phase4_live_execution_controlled.py`
- `tests/app/unit/test_phase4_live_bridge.py`
- `tests/app/unit/test_phase5_observability_export.py`

## Guarded Paths
- `src/trading_advisor_3000/app/contracts/`
- `src/trading_advisor_3000/app/domain/`
- `src/trading_advisor_3000/app/data_plane/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/app/integration/test_phase4_live_execution_controlled.py -q`
