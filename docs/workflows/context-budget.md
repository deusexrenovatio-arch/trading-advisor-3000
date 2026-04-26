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
5. Use `context_router.py` to produce a context footprint before loading broad context.
6. For code tasks, start from the primary card's search seeds and use Serena for exact symbols and references.
7. Treat generated artifacts, local tool config, archives, plans, and memory as cold unless explicitly needed.
8. When cold or broad context is needed, log a Context Expansion Reason before opening it.

## Context Footprint
Record or summarize these fields when a task or PR spans more than one context:
- primary context
- navigation order
- secondary contexts
- unmapped files
- critical contours

## Context Expansion Reason
Use this only when the agent expands beyond the primary context route into
memory, current diff, logs, generated artifacts, live process state, Graphify,
web docs, or broad file reads.

Keep each entry short:
- `reason`: the uncertainty or evidence question being resolved
- `source`: the tool/source being consulted
- `insufficiency`: why the current context is not enough
- `stop_condition`: what answer lets the agent stop expanding

This is operator traceability, not a new heavyweight gate.

## Validation
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_task_request_contract.py`
