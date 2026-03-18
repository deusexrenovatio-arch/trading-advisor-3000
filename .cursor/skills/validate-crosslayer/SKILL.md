---
name: validate-crosslayer
description: Validate cross-layer consistency and prevent boundary drift between subsystems.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-ARCHITECTURE
scope: cross-layer consistency and boundary validation
routing_triggers:
  - "crosslayer"
  - "boundary validation"
  - "consistency checks"
  - "layer contract"
---

# Validate Crosslayer

## Purpose
Validate cross-layer consistency and prevent boundary drift between subsystems.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
