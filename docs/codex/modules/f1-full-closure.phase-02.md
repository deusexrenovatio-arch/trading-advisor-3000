# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-B - Telegram and Replaceable Stack Closure
- Status: completed

## Objective

- Remove every ghost chosen state from the replaceable stack set and close the Telegram adapter mismatch with one honest terminal outcome per technology.

## In Scope

- `aiogram`
- `Polars`
- `DuckDB`
- `vectorbt`
- `Alembic`
- `OpenTelemetry`
- stack spec, registry, ADR set, runtime code, and tests required to make each technology terminal

## Out Of Scope

- final release-readiness decision
- contracts-freeze expansion outside the replaceable stack and Telegram boundary
- broader runtime or broker-path work unrelated to the replaceable set

## Change Surfaces

- ARCH-DOCS
- CTX-CONTRACTS
- CTX-ORCHESTRATION
- CTX-API-UI

## Constraints

- Each replaceable technology must end in exactly one honest state: `implemented`, `removed`, or `replaced_by:<tech>`.
- `chosen but planned` is forbidden at the end of this phase for the in-scope replaceable set.
- For Telegram, the only acceptable outcomes are a real aiogram implementation or an ADR-backed switch to the custom Bot API engine with aligned docs and registry state.

## Acceptance Gate

- Stack spec, registry, ADR set, runtime code, and tests agree for every in-scope technology.
- No replaceable technology remains ambiguous after the phase.
- Negative tests cover at least one ghost chosen-state regression path.

## Disprover

- Leave one technology such as `OpenTelemetry` marked as chosen without executable proof or ADR-backed removal and confirm the phase fails.

## Done Evidence

- Every in-scope replaceable technology is terminal and non-ambiguous.
- Telegram closure no longer contradicts runtime reality, spec, registry, or route history.
- Negative tests prove that ghost chosen states are rejected.

## Release Gate Impact

- Surface Transition: `R2/S1` ghost-state model `ambiguous -> terminal`
- Minimum Proof Class: live-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: replaceable_stack_alignment, publication_chat_contour
- Delivered Proof Class: live-real
- Required Real Bindings: configured real publication chat/channel, real publication credentials, and replayable publication evidence
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Live contour closure: this phase does not prove a real configured production chat/channel or a real broker rollout contour.

## Rollback Note

- Revert any terminal-state claim that lacks aligned runtime, registry, doc, and ADR evidence.
