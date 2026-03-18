---
name: testing-suite
description: Maintain unit, integration, contract, and process test suites for governance confidence.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: test suite strategy and maintenance
routing_triggers:
  - "tests"
  - "coverage"
  - "integration"
  - "contract tests"
---

# Testing Suite

## Purpose
Maintain unit, integration, contract, and process test suites for governance confidence.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
