# CTX-OPS

## Scope
Governance scripts, gate routing, and CI/PR delivery policy.

## Inside This Context
- Agent startup rules, ordinary chat governance, gate routing, and PR evidence policy.
- Gate runners, validation harnesses, process tests, Git/CI workflow glue, and operator runbooks.
- Typical questions: which route should run, which gate proves this patch, why did policy fail?
- Not inside: product trading behavior, research semantics, canonical data logic, or operator API behavior except as guarded cross-context effects.

## Owned Paths
- `AGENTS.md`
- `docs/agent/`
- `docs/workflows/`
- `docs/runbooks/`
- `scripts/`
- `tests/process/`
- `.githooks/`
- `.github/workflows/`

## Guarded Paths
- `src/trading_advisor_3000/`

## Navigation Facets
- gates
- process-tests

## Search Seeds
- `scripts/context_router.py`
- `scripts/run_loop_gate.py`
- `scripts/run_pr_gate.py`
- `tests/process/test_context_router.py`

## Navigation Notes
- Use product-plane context cards first when they appear before OPS in `navigation_order`.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_task_request_contract.py`
