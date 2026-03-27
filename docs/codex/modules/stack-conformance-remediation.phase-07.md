# Module Phase Brief

Updated: 2026-03-25 15:26 UTC

## Parent

- Parent Brief: docs/codex/modules/stack-conformance-remediation.parent.md
- Execution Contract: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Phase

- Name: R2 - Telegram Adapter Closure
- Status: completed

## Objective

- Resolve the Telegram-stack mismatch honestly by either implementing aiogram with proof or removing it through ADR and aligned stack updates.

## In Scope

- Telegram publication adapter decision and implementation or ADR
- runtime/docs/registry alignment for the chosen path
- targeted proof with a mocked Telegram API if aiogram is retained

## Out Of Scope

- durable runtime default
- sidecar implementation
- unrelated observability or research decisions

## Change Surfaces

- CTX-ORCHESTRATION
- GOV-DOCS
- ARCH-DOCS

## Constraints

- A custom in-memory publication engine is not aiogram proof.
- Registry, spec/docs, and runtime implementation must say the same thing after the phase.
- Keep the proof path explicit and black-box enough to disprove stub-only closure.

## Acceptance Gate

- The chosen Telegram path matches the declared stack state.
- No doc claims aiogram unless actual aiogram runtime exists.
- Tests or ADR evidence prove the chosen direction.

## Disprover

- Keep aiogram declared as chosen while using only the current custom in-memory adapter and confirm the phase fails.

## Done Evidence

- aiogram is either implemented with runtime proof or removed through ADR-backed updates.
- Telegram-related docs/spec/registry claims are aligned.

## Rollback Note

- Revert the adapter decision if docs, registry, and runtime paths drift apart again.
