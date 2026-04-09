# Phase Evidence Contract

## Purpose
- Keep phase acceptance evidence explicit, small, and fail-closed.
- Avoid narrative-only proof while also avoiding a bloated evidence schema.

## Mandatory Worker Evidence Contract
Every governed worker or remediation payload must include one `evidence_contract` object with exactly these review-significant fields:

- `surfaces`: release or phase surfaces this attempt claims to move.
- `proof_class`: one of `doc`, `schema`, `unit`, `integration`, `staging-real`, `live-real`.
- `artifact_paths`: concrete artifact paths produced or relied on for this phase proof.
- `checks`: exact commands or tests that were executed for this proof.
- `real_bindings`: real systems/channels/environments used by the proof, or an empty list only when the phase does not require real bindings.

## Hard Rules
- Evidence must be phase-scoped, not a generic branch summary.
- `proof_class` must not be weaker than the phase brief's delivered proof class.
- `surfaces` must cover every surface owned by the current phase.
- `artifact_paths` and `checks` must both be non-empty for any phase that claims progress.
- `checks` entries must be exact executed commands; placeholder tokens like `<path>` or `<phase-scoped-files>` are invalid evidence.
- If the phase requires real bindings, `real_bindings` must be non-empty.
- If remediation edits documentation files, payload must include `documentation_context` with:
  - `source_documents`,
  - `materialized_documents`,
  - `preserved_goals`,
  - `preserved_acceptance_criteria`.

## Automatic Block Conditions
Acceptance must block when any of these are true:

1. `evidence_contract` is missing for a phase that owns one or more surfaces.
2. One or more owned surfaces are absent from `surfaces`.
3. `proof_class` is weaker than the phase's delivered proof class.
4. `artifact_paths` is empty.
5. `checks` is empty.
6. The phase requires real bindings and `real_bindings` is empty.

## Non-Goals
- This contract does not try to encode every artifact subtype or every runtime envelope.
- This contract does not replace human acceptance review for architecture fit, truthfulness, or operator guidance.
- This contract does not infer whether a listed binding is truly correct; it only fail-closes on missing or obviously weak proof.

## Enforcement
- Worker and remediation prompts must require `evidence_contract`.
- The orchestrator acceptance policy must block on the automatic block conditions above.
- Human acceptance still reviews the evidence for correctness, recurrence risk, and unresolved operational exceptions, but no longer carries the whole burden of checking that evidence exists in the first place.
