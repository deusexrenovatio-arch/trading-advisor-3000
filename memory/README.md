# Memory Registry

Canonical source:
- `memory/decisions/*.yaml`
- `memory/incidents/*.yaml`
- `memory/patterns/*.yaml`
- section indexes under each folder

Compatibility outputs:
- `memory/agent_memory.yaml`
- `memory/task_outcomes.yaml`

Maintenance commands:
- `python scripts/sync_state_layout.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_task_outcomes.py`
