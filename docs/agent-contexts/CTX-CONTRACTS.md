# CTX-CONTRACTS

## Scope
High-risk contract and durable-state surfaces for plans, memory, and validation policies.

## Owned Paths
- `configs/`
- `plans/`
- `memory/`
- `docs/checklists/`
- `scripts/validate_task_request_contract.py`
- `scripts/validate_plans.py`
- `scripts/validate_agent_memory.py`
- `scripts/validate_task_outcomes.py`
- `scripts/sync_task_outcomes.py`
- `scripts/sync_state_layout.py`

## Guarded Paths
- `src/trading_advisor_3000/`
- `docs/architecture/`

## Escalation Rules
1. Split mixed patches in order: contracts -> code -> docs.
2. Stop after two repeated failures on the same path and run remediation first.
3. Do not close task lifecycle if contract validators are red.

## Minimum Checks
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_task_outcomes.py`

