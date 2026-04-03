---
name: code-reviewer
description: Review code changes for correctness, security, maintainability, performance, and test adequacy with prioritized actionable findings.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: code review quality and actionable risk triage
routing_triggers:
  - "code review"
  - "review pr"
  - "review feedback"
  - "security review"
  - "performance review"
---

# Code Reviewer

## Purpose
Provide clear, actionable code review feedback that improves code quality and reduces rework.

## Review Priorities
1. Correctness
2. Security
3. Maintainability
4. Performance
5. Test adequacy

Style-only comments are secondary unless they hide a real defect risk.

## Findings Contract
Report findings first, ordered by severity:
- `P0`: release-blocking defect or exploit path
- `P1`: high regression or reliability risk
- `P2`: non-blocking maintainability debt

Each finding must include:
1. exact location and affected behavior;
2. why the issue matters in production;
3. minimal remediation direction.

## Review Workflow
1. Understand change intent and changed boundaries before judging details.
2. Inspect high-risk paths first:
   - input validation and trust boundaries;
   - state mutation and error handling;
   - dependency changes and external calls;
   - test coverage for changed behavior.
3. Classify findings by severity and confidence.
4. Propose smallest safe remediation for each P0/P1 item.
5. Summarize residual risks and missing evidence.

## Quality Checklist

### Correctness
- Behavior matches requested outcome.
- Edge and failure paths are explicit.
- No silent fallback that changes semantics.

### Security
- User-controlled input is validated and safely handled.
- No new secret exposure path.
- Access checks stay explicit at boundary points.

### Maintainability
- Changed logic is readable and decomposed by responsibility.
- Cross-layer reach-through is avoided when contracts should be used.
- Duplication is controlled where it increases defect risk.

### Performance
- No obvious hot-path regressions on changed code.
- No avoidable repeated heavy work in loops.
- Query and I/O patterns are bounded for expected load.

### Tests
- Changed behavior includes primary success and failure-path coverage.
- Tests are deterministic and targeted to changed risk.
- Assertions validate behavior, not only implementation internals.

## Skill Traces (Conditional Co-Use)
Use only when the condition is true:
1. Architecture boundary risk is present:
   - `architecture-review`
2. Dependency add or upgrade is part of the patch:
   - `dependency-and-license-audit`
3. Security-sensitive config or secret paths changed:
   - `secrets-and-config-hardening`
4. Test evidence is weak for changed behavior:
   - `testing-suite`
5. The same defect pattern repeats:
   - `repeated-issue-review`

## Boundaries
This skill should NOT:
- block progress with style-only or subjective preferences;
- require full refactors when targeted fixes close risk;
- replace architecture review for boundary-level decisions.

