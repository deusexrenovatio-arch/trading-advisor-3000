---
name: phase-acceptance-governor
description: Enforce hard phase acceptance so no silent fallbacks, skipped checks, unresolved assumptions, or deferred critical work can pass as done.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: hard phase acceptance policy and evidence-based unblock rules
routing_triggers:
  - "phase acceptance"
  - "acceptance gate"
  - "acceptor"
  - "fallback"
  - "skip checks"
  - "verification before completion"
---

# Phase Acceptance Governor

## Purpose
Keep phase acceptance hard and explicit.
No phase may unlock the next phase if it still depends on silent assumptions, skipped checks, hidden fallbacks, deferred critical work, or missing evidence.

## Trigger Patterns
- "phase acceptance"
- "acceptance gate"
- "acceptor"
- "fallback"
- "skip checks"
- "unresolved assumptions"
- "deferred critical work"

## Capabilities
- Detect hidden quality downgrades and policy-breaching shortcuts.
- Block phase completion when evidence or required checks are missing.
- Produce explicit remediation requirements before re-running acceptance.
- Enforce completion verification so "done" is evidence-backed, not assertion-backed.

## Mandatory Blockers
Treat the phase as `BLOCKED` when any of the following are present:

1. A fallback changes the intended path, backend, or quality bar without an explicit phase-contract decision.
2. A required check was skipped, downgraded, or replaced with weaker proof.
3. An assumption still materially shapes implementation or acceptance.
4. Critical work was deferred behind wording like `later`, `follow-up`, `future phase`, or `out of scope` without explicit contract permission.
5. The implementation is a local patch-up that does not fit the intended architecture.
6. Tests exist only on paper or were not actually executed.
7. Docs are stale or incomplete for operator flow, user/test cases, or architecture.

## Acceptance Rubric
The acceptor must evaluate every phase on all of these dimensions:

1. Route integrity: the worker stayed inside the phase and did not bypass the required flow.
2. Implementation integrity: no shortcut or hidden downgrade of the target solution shape.
3. Test evidence: required checks exist, are relevant, and were actually run.
4. Documentation coverage: docs reflect implementation and operator behavior.
5. Architecture fit: boundaries, dependency direction, and future scale remain sound.
6. Traceability: the phase objective and done evidence are fully closed, not "mostly done".
7. Completion verification: every closure claim maps to executable proof, not narrative only.

## Required Review Lenses
When available, combine this skill with:

- `.cursor/skills/architecture-review/SKILL.md`
- `.cursor/skills/testing-suite/SKILL.md`
- `.cursor/skills/docs-sync/SKILL.md`
- `.cursor/skills/verification-before-completion/SKILL.md`

## Output Contract
The acceptor should return:

1. A clear `PASS` or `BLOCKED` verdict.
2. Concrete blockers with why and remediation.
3. The rerun checks needed after remediation.
4. Explicit evidence gaps and prohibited findings, if any.
5. A route signal showing that the governed phase route was used.

## Validation
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/validate_skills.py --strict`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
