# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-F - Operational Readiness and Final Release Decision
- Status: planned

## Objective

- Produce the final governed release decision package only after every upstream F1 closure prerequisite is satisfied and reproducibly evidenced.

## In Scope

- canonical runbooks
- environment inventory
- release checklist
- aggregated immutable artifacts
- final red-team review
- explicit `ALLOW_RELEASE_READINESS` or `DENY_RELEASE_READINESS` decision package

## Out Of Scope

- foundational technology implementation that belongs to earlier F1 phases
- bypassing blockers through narrative downgrades
- production rollout beyond the release-decision boundary

## Change Surfaces

- PROCESS-STATE
- GOV-DOCS
- ARCH-DOCS

## Constraints

- `ALLOW_RELEASE_READINESS` is legal only when every prerequisite F1 phase is terminal and evidenced.
- Any contradiction across STATUS, registry, spec, ADR, code, tests, or immutable artifacts forces `DENY_RELEASE_READINESS`.
- Red-team review and release-decision validation are mandatory.

## Acceptance Gate

- `G0/G1` contradictions are closed.
- `R2/S1` ghost states are closed.
- `E1` evidence is hardened.
- `contracts_freeze`, `real_broker_process`, and `production_readiness` are terminal in truth sources.
- Release-gate validation and red-team both pass for the emitted decision.

## Disprover

- Remove one required immutable artifact, restore one ghost chosen technology, or mutate one public payload without contract updates and confirm the final F1 decision fails.

## Done Evidence

- The release-decision package contains runbooks, environment inventory, release checklist, immutable artifacts, and red-team output.
- The package emits an explicit `ALLOW_RELEASE_READINESS` or `DENY_RELEASE_READINESS` verdict with no overclaiming language.
- The final decision is reproducible from the truth-source and artifact bundle.

## Rollback Note

- Revoke any `ALLOW_RELEASE_READINESS` outcome immediately if one prerequisite surface falls back to `planned`, `partial`, `not accepted`, or unsupported evidence.
