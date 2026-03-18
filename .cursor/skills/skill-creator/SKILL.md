---
name: skill-creator
description: Create or revise skills with clear triggers, compact instructions, and governance alignment.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-SKILLS
scope: skill authoring and lifecycle maintenance
routing_triggers:
  - "create skill"
  - "update skill"
  - "skill design"
  - "skill authoring"
---

# Skill Creator

## Purpose
Create or revise skills with clear triggers, compact instructions, and governance alignment.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
