# Task Request Contract Checklist

## Purpose
- Raise request quality before implementation starts.
- Reduce hidden scope drift and repeated same-path patching.

## Mandatory Contract
- Objective: one concrete outcome.
- In Scope: what can change now.
- Out of Scope: what is intentionally deferred.
- Constraints: risk, policy, runtime, and time limits.
- Done Evidence: exact commands and artifacts that prove completion.
- Priority Rule: tie-breaker when requirements conflict.

## Critical Contour Extension
Only when a task hits a configured critical contour, add:
- Solution Class: `target|staged|fallback`
- Critical Contour: configured contour id
- Forbidden Shortcuts: explicit shortcut ids that must not be used
- Closure Evidence: the exact evidence that proves the claimed contour closure
- Shortcut Waiver: `none` or one-line reason

Also add one short design checkpoint that states:
- chosen path,
- why it is not a shortcut,
- what target shape is preserved.

Use the snippet in [docs/templates/critical_contour_task_note_block.md](docs/templates/critical_contour_task_note_block.md).

## Mandatory Repetition Control
- Max Same-Path Attempts: cap before forced strategy reset.
- Stop Trigger: explicit signal that current path is exhausted.
- Reset Action: concrete reset step.
- New Search Space: at least two alternative approaches.
- Next Probe: smallest command/test to distinguish alternatives.

## Rejection Rules
- Missing measurable objective.
- Scope/out-of-scope contradiction.
- No testable completion evidence.
- No priority rule when tradeoffs exist.
- Repetition control missing on repeated failures.

## Enforcement
- Keep sections in active task note (via `docs/session_handoff.md` pointer):
  - `## Task Request Contract`
  - `## First-Time-Right Report`
  - `## Repetition Control`
- For configured critical contours, also add the critical contour block from `docs/templates/critical_contour_task_note_block.md`.
- Validate with:
  - `python scripts/validate_task_request_contract.py`
  - `python scripts/validate_session_handoff.py`
