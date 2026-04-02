# Layers

## Layer 1: Governance Contract
- `AGENTS.md`
- `docs/agent/*`
- `.githooks/pre-push`

## Layer 2: Process Runtime
- `scripts/task_session.py`
- `scripts/context_router.py`
- `scripts/run_loop_gate.py`
- `scripts/run_pr_gate.py`
- `scripts/run_nightly_gate.py`

## Layer 3: Durable State
- `plans/items/*` (canonical)
- `plans/PLANS.yaml` (compatibility)
- `memory/{decisions,incidents,patterns}/*` (canonical)
- `memory/agent_memory.yaml` and `memory/task_outcomes.yaml` (compatibility)

## Layer 4: Application Plane
- `src/trading_advisor_3000/*`
- `tests/product-plane/*`

## Layer 5: Verification
- `tests/process/*`
- `tests/architecture/*`
- `.github/workflows/*`
