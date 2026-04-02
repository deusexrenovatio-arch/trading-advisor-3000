---
name: parallel-worktree-flow
description: Coordinate parallel worktrees with deterministic isolation, sync rules, and merge order.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: parallel worktree operations and branch isolation
routing_triggers:
  - "worktree"
  - "parallel streams"
  - "branch isolation"
  - "integration branch"
---

# Parallel Worktree Flow

## Purpose
Coordinate parallel worktrees with deterministic isolation, sync rules, and merge order.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`

## Boundaries

This skill should NOT:
- [Add constraints and limitations]
- [Specify what the agent should never do]

