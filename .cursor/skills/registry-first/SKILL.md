---
name: registry-first
description: Apply registry-first change discipline for contracts, schemas, and ownership records.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-CONTRACTS
scope: registry-first governance for contract evolution
routing_triggers:
  - "registry"
  - "schema"
  - "contract"
  - "catalog"
---

# Registry First

## Purpose
Apply registry-first change discipline for contracts, schemas, and ownership records.

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

