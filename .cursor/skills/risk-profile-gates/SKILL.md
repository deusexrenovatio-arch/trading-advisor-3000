---
name: risk-profile-gates
description: Define deterministic risk-profile gates for process changes and release readiness.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-CONTRACTS
scope: risk classification and gating rules
routing_triggers:
  - "risk profile"
  - "risk gate"
  - "release gate"
  - "policy threshold"
---

# Risk Profile Gates

## Purpose
Define deterministic risk-profile gates for process changes and release readiness.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
