---
name: incident-runbook
description: Define incident response and remediation paths with durable evidence.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: incident handling and remediation flow
routing_triggers:
  - "incident"
  - "runbook"
  - "postmortem"
  - "remediation"
---

# Incident Runbook

## Purpose
Define incident response and remediation paths with durable evidence.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
