# Acceptance Contract

## Purpose
Define what phase acceptance means in this repository.

Acceptance is not a stylistic review.
It is the hard unblock decision for phase progression.

## Evidence Contract
- Use the bounded worker evidence contract from `docs/checklists/phase-evidence-contract.md`.
- Acceptance is not allowed to rely only on prose, summaries, or route telemetry when the phase claims progress on one or more owned surfaces.
- The worker evidence contract is intentionally small:
  - `surfaces`
  - `proof_class`
  - `artifact_paths`
  - `checks`
  - `real_bindings`

## Quality Split
- `result quality` is the acceptor-owned evaluation of the phase output itself.
- `orchestration quality` is the Python-owned evaluation of how the governed route behaved.

The acceptor must score result quality independently of retry friction.
Required result-quality dimensions:
1. `requirements_alignment`
2. `documentation_quality`
3. `implementation_quality`
4. `testing_quality`

Retry count, remediation count, blocker recurrence, and route friction belong only to orchestration quality.

## Hard Rules
The acceptor must block the phase when any of these are true:

1. A silent fallback changed the path, backend, scope, or quality bar.
2. A required check was skipped, replaced, or weakened.
3. A material implementation assumption remains unresolved.
4. Critical work was deferred without an explicit contract decision.
5. Evidence is declared but not actually shown or executed.
6. The bounded worker evidence contract is missing, incomplete, or weaker than the phase brief requires.
7. Documentation drift remains in any required layer.
8. The solution is a local patch-up instead of a sound architectural fit.

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

4. Evidence Contract Integrity
- Owned surfaces are named explicitly in worker evidence.
- Proof class is explicit and not weaker than the phase brief requires.
- Artifact paths and executed checks are present.
- Real bindings are explicit when the phase requires real systems or channels.

5. Documentation Coverage
- Operator docs are current.
- Relevant test-case or user-case docs are current.
- Architecture/governance docs are current when the change surface affects them.
- If docs were changed in remediation, the report must include full source + materialized documentation context and explicit goal/acceptance-preservation evidence.

6. Architecture Fit
- Boundaries stay intact.
- Dependency direction remains acceptable.
- The result scales as part of the intended system shape.

7. Traceability
- The phase objective is fully closed.
- Done evidence is real, not narrative-only.

## PASS Criteria
Return `PASS` only when the current phase is closed enough to unlock the next phase with no open blockers in the dimensions above.

Phase `PASS` does not automatically mean release readiness.
The acceptor must respect the phase brief's accepted-state label:
- `prep_closed` = local prerequisite closure only;
- `real_contour_closed` = real contour closure for the owned surface only;
- `release_decision` = only this class may emit the final `ALLOW_RELEASE_READINESS` or `DENY`.

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
