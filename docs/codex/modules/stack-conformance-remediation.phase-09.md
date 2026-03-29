# Module Phase Brief

Updated: 2026-03-25 15:26 UTC

## Parent

- Parent Brief: docs/codex/modules/stack-conformance-remediation.parent.md
- Execution Contract: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Phase

- Name: S1 - Replaceable Stack Decisions
- Status: completed

## Objective

- Resolve all remaining replaceable technology mismatches so no declared stack item stays in a ghost `chosen but not real` state.

## In Scope

- `vectorbt`
- `Alembic`
- `OpenTelemetry`
- `Polars`
- `DuckDB`
- any remaining replaceable decision from earlier phases that still lacks implementation or ADR-backed removal

## Out Of Scope

- re-closing already handled architecture-critical phases
- release-readiness signoff
- governance-only vocabulary work already covered by earlier phases

## Change Surfaces

- CTX-RESEARCH
- CTX-ORCHESTRATION
- ARCH-DOCS

## Constraints

- Every surface needs one of two outcomes only: executable proof or ADR-backed removal or replacement.
- No ambiguous `chosen` state can remain after this phase.
- Split sub-patches by surface if one combined patch becomes too risky.

## Acceptance Gate

- No replaceable target-stack item remains in an ambiguous state.
- Implementation or ADR evidence exists for every in-scope surface.
- Registry and docs match the chosen outcome for each surface.

## Disprover

- Leave any replaceable technology as declared-in-spec with no runtime proof or ADR and confirm the phase fails.

## Done Evidence

- Each replaceable surface has an explicit end state.
- Related docs/spec/registry/runtime evidence are aligned.

## Rollback Note

- Revert any individual surface decision that cannot maintain alignment across docs, registry, and executable proof.
