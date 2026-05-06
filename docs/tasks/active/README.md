# Active Tasks

Canonical active task notes live here only for a live governed task session.

Important: this directory is no longer a reliable source of current TA3000
product reality by itself. Many older notes can describe stale intent,
superseded work, or historical process state. For current project state, read
`docs/project-map/current-truth-map-2026-05-05.md` first, then use task notes only
as supporting evidence.

Lifecycle:
1. Start session: `python scripts/task_session.py begin --request "<request>"`
2. Work in one active note pointed by `docs/session_handoff.md`.
3. Close session: `python scripts/task_session.py end` (moves note to archive).

Index:
- `docs/tasks/active/index.yaml` is the machine-readable active-task registry.
- Empty `items: []` is valid when there is no live governed task session.

Historical task notes moved out of this directory on 2026-05-06:

- `docs/archive/historical-task-notes/2026-05-06/`
