# Harness Guideline

## Purpose
Define how governance principles map to enforceable checks.

| Principle | Enforcement path | Owner surface | Lane |
| --- | --- | --- | --- |
| PR-only main | `.githooks/pre-push` | root governance | loop/pre-push |
| Narrow hot context | `.cursorignore` + hot docs | docs/agent | loop |
| Canonical runtime entrypoints | `docs/agent/runtime.md` + scripts | scripts | loop/pr |
| No legacy gate names | policy docs + tests | docs + tests | loop/pr |
| Small patch sets | workflow docs + review discipline | docs/workflow | PR |
| Durable state compatibility | `scripts/sync_state_layout.py` | plans + memory | nightly |
| Process regression monitoring | `scripts/validate_process_regressions.py` | memory/task_outcomes | nightly |

## Phase note
This file defines control-plane contract boundaries for the shell baseline.
