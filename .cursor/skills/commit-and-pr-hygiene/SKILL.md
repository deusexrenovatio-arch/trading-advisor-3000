---
name: commit-and-pr-hygiene
description: Keep commit series and pull requests atomic, reviewable, and policy-compliant.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-OPS
scope: commit hygiene and pull request structure
routing_triggers:
  - "commit hygiene"
  - "pr hygiene"
  - "atomic changes"
  - "reviewability"
---

# Commit And Pr Hygiene

## Purpose
Keep commit series and pull requests atomic, reviewable, and policy-compliant.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
