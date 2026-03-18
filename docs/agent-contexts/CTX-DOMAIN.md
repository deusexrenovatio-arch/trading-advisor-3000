# CTX-DOMAIN

## Scope
Domain-only package internals that are not data/research/orchestration/interface layers.

## Owned Paths
- `src/trading_advisor_3000/app/domain/`
- `tests/app/contracts/`
- `tests/app/test_app_placeholder.py`

## Guarded Paths
- `docs/architecture/`
- `scripts/`

## Input/Output Contract
- Input: process-shell validated configuration and contracts.
- Output: minimal placeholder app behavior without trading logic.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/app/contracts -q`
