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

## Critical Contour Addendum
When the diff matches `configs/critical_contours.yaml`, add `## Solution Intent` and record:
- Solution Class: `target`, `staged`, or `fallback`.
- Critical Contour: the matching pilot contour id.
- Forbidden Shortcuts: the shortcut patterns that must stay blocked.
- Closure Evidence: the contour-specific evidence being used.
- Shortcut Waiver: `none` or one explicit reason for an intentional fallback.

Keep the design checkpoint inline:
- chosen path;
- why it is not a shortcut;
- what future shape stays preserved.

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
- For package/module planning, also satisfy `docs/checklists/planning-gate-contract.md`.
- Validate with:
  - `python scripts/validate_task_request_contract.py`
  - `python scripts/validate_phase_planning_contract.py`
  - `python scripts/validate_session_handoff.py`
