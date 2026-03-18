---
name: skill-installer
description: Install and activate approved skills into local runtime catalogs.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-SKILLS
scope: skill onboarding and local catalog installation
routing_triggers:
  - "install skill"
  - "catalog install"
  - "skill onboarding"
  - "skill source"
---

# Skill Installer

## Purpose
Install and activate approved skills into local runtime catalogs.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
