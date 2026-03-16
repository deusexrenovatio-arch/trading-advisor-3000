# CTX-OPS

## Scope
Governance scripts, lifecycle contracts, and gate orchestration.

## Owned Paths
- `AGENTS.md`
- `docs/agent/`
- `docs/workflows/`
- `docs/runbooks/`
- `docs/tasks/`
- `docs/session_handoff.md`
- `scripts/`
- `tests/process/`
- `.githooks/`
- `.github/workflows/`

## Guarded Paths
- `src/trading_advisor_3000/`

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
