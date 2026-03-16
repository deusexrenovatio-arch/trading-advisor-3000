# CTX-ARCHITECTURE

## Scope
Architecture-as-docs package, ADRs, and boundary validation tests.

## Owned Paths
- `docs/architecture/`
- `tests/architecture/`

## Guarded Paths
- `src/trading_advisor_3000/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/architecture -q`
