---
name: qa-test-engineer
description: Design quality verification plans and regression coverage for delivery gates.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: quality planning and end-to-end verification
routing_triggers:
  - "qa"
  - "test plan"
  - "regression"
  - "validation"
---

# Qa Test Engineer

## Purpose
Design quality verification plans and regression coverage for delivery gates.

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

