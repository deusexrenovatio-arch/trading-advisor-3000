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

## Required Output Block
Keep this under `## First-Time-Right Report` in the active task note:
1. Confirmed coverage.
2. Missing or risky scenarios.
3. Resource/time risks and chosen controls.
4. Highest-priority fixes or follow-ups.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
