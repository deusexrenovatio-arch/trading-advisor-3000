# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-B - Telegram and Replaceable Stack Closure
- Status: completed

## Objective

- убрать все replaceable ghost technologies и закрыть Telegram mismatch.

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

- Stack spec, registry, ADR set, runtime code и tests совпадают по каждой технологии.
- Ни одна replaceable technology не остаётся ambiguous.
- Publication chat contour is proven with a configured real publication chat/channel, real publication credentials, and replayable publication evidence.
- Negative tests покрывают случай ghost chosen state.

## Disprover

- Оставить `OpenTelemetry` как chosen в stack spec без runtime proof и без ADR, и убедиться, что phase fail-ится.

## Done Evidence

- Every in-scope replaceable technology is terminal and non-ambiguous.
- Telegram/publication closure is proven on a configured real publication chat/channel and no longer contradicts runtime reality, spec, registry, or route history.
- Negative tests prove that ghost chosen states are rejected.

## Release Gate Impact

- Surface Transition: `R2/S1` ghost-state model `ambiguous -> terminal`; `publication_chat_contour` `unconfigured -> real_configured_chat`
- Minimum Proof Class: live-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: replaceable_stack_alignment, publication_chat_contour
- Delivered Proof Class: live-real
- Required Real Bindings: configured real publication chat/channel, real publication credentials, and replayable publication evidence
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Live contour closure beyond owned surfaces: this phase does not prove a real broker rollout contour or the integrated `production_live_readiness` bundle.

## Rollback Note

- Revert any terminal-state claim that lacks aligned runtime, registry, doc, and ADR evidence.
