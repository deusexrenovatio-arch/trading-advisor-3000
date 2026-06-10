# Context Budget Workflow

## Purpose
- Keep session context focused and deterministic.
- Move durable continuity into repository artifacts instead of long chat history.

## Contract
1. Keep context route output concise and actionable.
2. Keep canonical state in:
   - `plans/items/`
   - `memory/{decisions,incidents,patterns}/`
3. Regenerate compatibility outputs via `python scripts/sync_state_layout.py`.
4. Use `context_router.py` to produce a context footprint before loading broad context.
5. For code tasks, start from the primary card's search seeds and use Serena for exact symbols and references.
6. Treat generated artifacts, local tool config, archives, plans, and memory as cold unless explicitly needed.
7. When cold or broad context is needed, log a Context Expansion Reason before opening it.

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
- `python scripts/validate_task_request_contract.py`
