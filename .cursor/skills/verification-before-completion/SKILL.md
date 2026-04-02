---
name: verification-before-completion
description: Enforce completion claims only when executable evidence and explicit checks are present.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: completion verification and evidence quality control
routing_triggers:
  - "verification before completion"
  - "done criteria"
  - "evidence gap"
  - "proof before closeout"
---

# Verification Before Completion

## Purpose
Prevent premature closure by requiring explicit, executable evidence for every completion claim.

## Trigger Patterns
- "task is done"
- "ready to close"
- "acceptance pass"
- "looks good without rerun"
- "no time for full verification"

## Capabilities
- Translate completion claims into a concrete evidence checklist per change surface.
- Detect missing proof classes (for example, docs-only evidence where integration proof is required).
- Block closure when checks are listed but not actually executed.
- Produce a minimal rerun command set required for a valid closeout.

## Workflow
1. Capture claimed outcome and affected surfaces.
2. Map each claim to required evidence class and executable checks.
3. Validate that evidence is current, reproducible, and tied to the changed boundary.
4. Record explicit PASS/BLOCKED decision with missing evidence items.
5. Re-run only the required commands after remediation and re-evaluate.

## Integration
- Co-load with `phase-acceptance-governor` for governed phase acceptance.
- Co-load with `testing-suite` when verification depends on test evidence quality.
- Co-load with `docs-sync` when operator flow and architecture docs are part of done criteria.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_skills.py --strict`

## Boundaries
This skill should NOT:
- accept narrative "confidence" as a substitute for executable evidence.
- downgrade required proof class silently to speed up closeout.
