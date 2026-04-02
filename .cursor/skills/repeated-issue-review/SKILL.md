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
  - "incident keeps returning"
---

# Repeated Issue Review

## Purpose
Perform deep repeated-issue analysis with explicit root-cause and prevention actions.

## Trigger Patterns
- "fixed but failed again"
- "same class of bug returned"
- "cannot reproduce consistently"
- "need full root cause tracing"

## Capabilities
- Build timeline-level incident narratives across attempts, environments, and patch versions.
- Separate symptom, trigger, and root cause to avoid patching only visible effects.
- Identify recurrence vectors: missing test coverage, weak gate signal, unsafe fallback, or stale docs/runbook.
- Produce concrete prevention actions with ownership and verification gates.

## Workflow
1. Reconstruct failure chronology (first report, attempted fixes, latest recurrence).
2. Define reproducible probes and collect evidence per hypothesis.
3. Eliminate false leads; keep only causal chain backed by logs/tests/contracts.
4. Design remediation that removes root cause and closes recurrence path.
5. Add regression checks and an operational guardrail that would have caught it earlier.

## Integration
- Pair with `incident-runbook` for operational containment and postmortem hygiene.
- Pair with `testing-suite` to convert root-cause findings into regression protections.
- Pair with `phase-acceptance-governor` when recurrence impacts phase acceptance decisions.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`

## Boundaries

This skill should NOT:
- conclude root cause from a single log line without competing hypothesis elimination.
- close recurrence incidents without a prevention check that is actually executable.
