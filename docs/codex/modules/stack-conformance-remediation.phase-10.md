# Module Phase Brief

Updated: 2026-03-27 10:18 UTC

## Parent

- Parent Brief: docs/codex/modules/stack-conformance-remediation.parent.md
- Execution Contract: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Phase

- Name: F1 - Full Re-Acceptance and Release-Readiness Decision Proof
- Status: blocked

## Objective

- Re-close the affected product phases under the repaired evidence model and produce an explicit release-readiness decision pack (`ALLOW_RELEASE_READINESS` or `DENY_RELEASE_READINESS`) with no overclaiming language.

## Contract Basis

- Source package baseline for F1 keeps the strong readiness bar (all architecture-critical implemented; replaceable implemented or ADR-removed) for any `ALLOW_RELEASE_READINESS`.
- This phase executes under execution-contract amendment `F1-2026-03-27-release-readiness-decision-contract`.
- Under that amendment:
  - `DENY_RELEASE_READINESS` is a valid F1 decision outcome when blockers are explicitly mapped to truth-source state.
  - `DENY_RELEASE_READINESS` does not unlock progression and does not downgrade the strong bar for later `ALLOW_RELEASE_READINESS`.

## In Scope

- regenerated phase checklists
- acceptance evidence pack
- final stack-conformance report
- red-team review result
- explicit release-readiness decision with blocker mapping to truth-source state

## Out Of Scope

- new foundational technology implementation
- reopening already accepted governance rules unless required by evidence
- production rollout beyond release-readiness decision proof

## Change Surfaces

- PROCESS-STATE
- GOV-DOCS
- ARCH-DOCS

## Constraints

- This phase starts only after all prerequisite closure and ADR decisions are complete.
- Every accepted phase must map to registry state and generated evidence.
- Red-team review is mandatory; green unit tests alone are insufficient.
- A release-readiness `ALLOW` decision is forbidden while any architecture-critical surface remains `partial`, `planned`, or `not accepted`, or while any replaceable surface is neither implemented nor ADR-removed.

## Acceptance Gate

- Every accepted phase maps to registry state and executable evidence.
- Final report, regenerated checklists, evidence pack, and red-team result stay mutually consistent.
- Red-team review returns an explicit release-readiness decision:
  - `ALLOW_RELEASE_READINESS` only when every architecture-critical surface is implemented and every replaceable surface is implemented or removed by ADR.
  - `DENY_RELEASE_READINESS` when any prerequisite remains open, with blockers explicitly traced to truth-source state.
- Final report and regenerated checklists contain no overclaiming language.

## Disprover

- Any release-readiness `ALLOW` decision with unresolved `partial`/`planned`/`not accepted` truth-source state.
- Any accepted phase lacking black-box proof or negative-test evidence.
- Any decision package that omits explicit blocker mapping when verdict is `DENY_RELEASE_READINESS`.

## Done Evidence

- Acceptance evidence pack exists.
- Final stack-conformance report exists.
- Red-team review result exists and contains an explicit release-readiness decision.
- Decision package states whether unlock is allowed or denied and why.

## Rollback Note

- Revert any `ALLOW_RELEASE_READINESS` claim immediately if prerequisite evidence is missing or if overclaiming language returns.
