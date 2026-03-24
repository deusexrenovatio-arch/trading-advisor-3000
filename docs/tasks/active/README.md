# Active Tasks

Canonical active task notes live here.

Lifecycle:
1. Start session: `python scripts/task_session.py begin --request "<request>"`
2. Work in one active note pointed by `docs/session_handoff.md`.
3. Close session: `python scripts/task_session.py end` (moves note to archive).

Critical contour note:
- When a task hits a configured critical contour, insert the block from `docs/templates/critical_contour_task_note_block.md` into the active task note.

Index:
- `docs/tasks/active/index.yaml` is the machine-readable active-task registry.
