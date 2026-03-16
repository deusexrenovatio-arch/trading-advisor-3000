# Context Budget Workflow

## Purpose
- Keep session context focused and deterministic.
- Move durable continuity into repository artifacts instead of long chat history.

## Contract
1. Keep `docs/session_handoff.md` as pointer-shim.
2. Keep active task note delta concise and actionable.
3. Keep canonical state in:
   - `plans/items/`
   - `memory/{decisions,incidents,patterns}/`
4. Regenerate compatibility outputs via `python scripts/sync_state_layout.py`.

## Validation
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_task_request_contract.py`
