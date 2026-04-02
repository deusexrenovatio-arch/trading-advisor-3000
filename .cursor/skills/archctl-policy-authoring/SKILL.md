---
name: archctl-policy-authoring
description: Author and tighten architecture policy gates and fitness rules.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-CONTRACTS
scope: policy gate authoring and fitness rule design
routing_triggers:
  - "policy gate"
  - "fitness rule"
  - "architecture policy"
  - "ci blocking"
---

# Archctl Policy Authoring

## Purpose
Author and tighten architecture policy gates and fitness rules.

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

