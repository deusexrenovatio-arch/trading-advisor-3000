# CTX-CONTRACTS

## Scope
High-risk contract and durable-state surfaces for plans, memory, and validation policies.

## Inside This Context
- Durable process state, phase contracts, critical contours, task outcomes, and validation policy.
- The context decides whether a change is allowed to proceed, not how product code should implement the behavior.
- Typical questions: is the task request complete, is solution intent required, did a critical contour close honestly?
- Not inside: app implementation details except when they are contractually constrained by configs, plans, or validators.

## Owned Paths
- `configs/`
- `plans/`
- `memory/`
- `docs/checklists/`
- `scripts/critical_contours.py`
- `scripts/validate_task_request_contract.py`
- `scripts/validate_solution_intent.py`
- `scripts/validate_critical_contour_closure.py`
- `scripts/validate_plans.py`
- `scripts/validate_agent_memory.py`
- `scripts/validate_task_outcomes.py`
- `scripts/sync_task_outcomes.py`
- `scripts/sync_state_layout.py`

## Guarded Paths
- `src/trading_advisor_3000/`
- `docs/architecture/`

## Navigation Facets
- contracts
- plans
- memory-state
- critical-contours

## Search Seeds
- `scripts/validate_task_request_contract.py`
- `scripts/validate_solution_intent.py`
- `scripts/validate_critical_contour_closure.py`
- `configs/critical_contours.yaml`
- `plans/items/index.yaml`

## Escalation Rules
1. Split mixed patches in order: contracts -> code -> docs.
2. Stop after two repeated failures on the same path and run remediation first.
3. Do not close task lifecycle if contract validators are red.

## Minimum Checks
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_solution_intent.py --from-git --git-ref HEAD`
- `python scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD`
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_task_outcomes.py`
