---
name: product-owner
description: Prioritize outcome value and define delivery sequencing for planned changes.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: value-based prioritization and roadmap sequencing
routing_triggers:
  - "value"
  - "priorities"
  - "roadmap"
  - "mvp"
---

# Product Owner

## Purpose
Prioritize outcome value and define delivery sequencing for planned changes.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
