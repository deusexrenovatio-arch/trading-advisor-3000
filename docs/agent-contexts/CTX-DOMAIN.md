# CTX-DOMAIN

## Scope
Residual app-plane internals and package metadata not covered by data, research, orchestration, or interface contexts.

## Owned Paths
- `src/trading_advisor_3000/app/domain/`
- `tests/app/test_app_plane_metadata.py`

## Guarded Paths
- `docs/architecture/`
- `scripts/`

## Input/Output Contract
- Input: process-shell validated configuration and contracts.
- Output: residual app-plane behavior or package metadata changes that do not cross shell boundaries.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/app/test_app_plane_metadata.py -q`
