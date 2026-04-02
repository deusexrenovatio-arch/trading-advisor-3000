---
name: golden-tests-and-fixtures
description: Create deterministic fixture-based regression tests for high-change workflows.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: golden fixtures and deterministic regression protection
routing_triggers:
  - "golden tests"
  - "fixtures"
  - "regression protection"
  - "deterministic tests"
---

# Golden Tests And Fixtures

## Purpose
Create deterministic fixture-based regression tests for high-change workflows.

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

