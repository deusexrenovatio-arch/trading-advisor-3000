# CTX-OPS

## Scope
Governance scripts, lifecycle contracts, and gate orchestration.

## Inside This Context
- Agent startup rules, ordinary chat governance, task lifecycle, and handoff pointers.
- Gate runners, validation harnesses, process tests, Git/CI workflow glue, and operator runbooks.
- Typical questions: which route should run, which gate proves this patch, why did lifecycle or policy fail?
- Not inside: product trading behavior, research semantics, canonical data logic, or operator API behavior except as guarded cross-context effects.

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

## Navigation Facets
- lifecycle
- gates
- handoff
- process-tests

## Search Seeds
- `scripts/context_router.py`
- `scripts/run_loop_gate.py`
- `scripts/run_pr_gate.py`
- `scripts/task_session.py`
- `tests/process/test_context_router.py`

## Navigation Notes
- Treat `docs/session_handoff.md` and `docs/tasks/**` as bookkeeping when product-plane files are also changed.
- Use product-plane context cards first when they appear before OPS in `navigation_order`.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
