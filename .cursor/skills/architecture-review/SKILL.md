---
name: architecture-review
description: Review module boundaries, dependency direction, and architecture contracts for regressions.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-ARCHITECTURE
scope: architecture boundary review and dependency control
routing_triggers:
  - "architecture"
  - "boundaries"
  - "dependencies"
  - "module review"
---

# Architecture Review

## Purpose
Review module boundaries, dependency direction, and architecture contracts for regressions.

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

