# CTX-ORCHESTRATION

## Scope
Runtime wiring, execution flow orchestration, and cross-subsystem coordination.

## Owned Paths
- `src/trading_advisor_3000/__main__.py`
- `src/trading_advisor_3000/app_metadata.py`
- `src/trading_advisor_3000/app/runtime/`
- `src/trading_advisor_3000/app/execution/`
- `src/trading_advisor_3000/dagster_defs/`
- `tests/app/integration/test_phase2c_runtime.py`
- `tests/app/integration/test_phase2d_execution.py`
- `tests/app/integration/test_phase3_system_replay.py`
- `tests/app/integration/test_phase5_review_observability.py`
- `tests/app/integration/test_phase6_operational_hardening.py`

## Guarded Paths
- `src/trading_advisor_3000/app/contracts/`
- `src/trading_advisor_3000/app/interfaces/`
- `src/trading_advisor_3000/app/research/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/app/integration/test_phase2c_runtime.py -q`
