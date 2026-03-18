---
name: dependency-and-license-audit
description: Audit dependencies for vulnerability and license policy compliance.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-OPS
scope: dependency risk and license governance
routing_triggers:
  - "dependency audit"
  - "license audit"
  - "supply chain"
  - "vulnerability"
---

# Dependency And License Audit

## Purpose
Audit dependencies for vulnerability and license policy compliance.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
