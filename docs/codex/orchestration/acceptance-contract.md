# Acceptance Contract

## Purpose
Define what phase acceptance means in this repository.

Acceptance is not a stylistic review.
It is the hard unblock decision for phase progression.

## Hard Rules
The acceptor must block the phase when any of these are true:

1. A silent fallback changed the path, backend, scope, or quality bar.
2. A required check was skipped, replaced, or weakened.
3. A material implementation assumption remains unresolved.
4. Critical work was deferred without an explicit contract decision.
5. Evidence is declared but not actually shown or executed.
6. Documentation drift remains in any required layer.
7. The solution is a local patch-up instead of a sound architectural fit.

## Required Acceptance Dimensions
Every phase verdict must cover all of these:

1. Route Integrity
- The phase followed `worker -> acceptance -> remediation -> acceptance -> unlock`.
- The worker stayed inside the current phase.

2. No Silent Degradation
- No silent fallback.
- No hidden shortcut.
- No silent model/backend downgrade.
- No silent check suppression.

3. Test Evidence
- Required tests exist.
- Required tests were run.
- Negative or fail-closed paths are covered where the phase contract expects them.

4. Documentation Coverage
- Operator docs are current.
- Relevant test-case or user-case docs are current.
- Architecture/governance docs are current when the change surface affects them.

5. Architecture Fit
- Boundaries stay intact.
- Dependency direction remains acceptable.
- The result scales as part of the intended system shape.

6. Traceability
- The phase objective is fully closed.
- Done evidence is real, not narrative-only.

## PASS Criteria
Return `PASS` only when the current phase is closed enough to unlock the next phase with no open blockers in the dimensions above.

## BLOCKED Criteria
Return `BLOCKED` when the current phase needs remediation, clarification, or a contract change before progression.

## Prohibited PASS Conditions
The phase must not pass with any of the following:

- `skip`
- `skip-check`
- `fallback`
- `assumption`
- `temporary`
- `follow-up`
- `later`
- `future phase`
- `good enough`

unless the phrase is used only to describe a blocker that keeps the phase blocked.
