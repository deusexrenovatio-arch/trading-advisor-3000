# First-Time-Right Gate Checklist

## Purpose
- Improve first-pass quality and reduce rework loops.
- Force explicit scenario coverage before implementation.

## Required Before Coding
1. Task request contract is complete and validated.
2. Goal contract is measurable and testable.
3. User-case coverage includes success, edge, and failure flows.
4. Budget/stop-replan controls are explicit.
5. Context integrity is preserved (no unrelated side-work).
6. When tradeoffs exist, the chosen path and at least one rejected viable alternative are explicit.
7. When the task simplifies or consolidates flows, the intended source of truth and duplicate-layer treatment are explicit.

## Required Output Block
Keep this under `## First-Time-Right Report` in the active task note:
1. Confirmed coverage.
   Include the chosen path and the target shape being preserved.
2. Missing or risky scenarios.
   Include shortcut or downgrade risks being deliberately avoided.
3. Resource/time risks and chosen controls.
4. Highest-priority fixes or follow-ups.
   Make the next unlock step or next discriminating probe obvious.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
