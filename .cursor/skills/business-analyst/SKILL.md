---
name: business-analyst
description: Decompose requests into measurable scope, acceptance criteria, and traceable outcomes.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: requirements framing and acceptance decomposition
routing_triggers:
  - "requirements"
  - "scope"
  - "acceptance"
  - "traceability"
  - "stakeholder"
---

# Business Analyst

## Purpose
Decompose requests into measurable scope, acceptance criteria, and traceable outcomes.

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

