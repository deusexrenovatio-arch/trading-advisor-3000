---
name: repeated-issue-review
description: Perform deep repeated-issue analysis with explicit root-cause and prevention actions.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: repeat-failure analysis and prevention strategy
routing_triggers:
  - "repeated issue"
  - "root cause"
  - "stability"
  - "full review"
---

# Repeated Issue Review

## Purpose
Perform deep repeated-issue analysis with explicit root-cause and prevention actions.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
