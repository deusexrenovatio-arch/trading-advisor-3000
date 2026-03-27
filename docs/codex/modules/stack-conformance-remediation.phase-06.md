# Module Phase Brief

Updated: 2026-03-25 15:26 UTC

## Parent

- Parent Brief: docs/codex/modules/stack-conformance-remediation.parent.md
- Execution Contract: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Phase

- Name: R1 - Durable Runtime Default and Service Closure
- Status: completed

## Objective

- Make the default non-test runtime path durable and resolve the declared service or API surface honestly by either implementing it or formally de-scoping it.

## In Scope

- default Postgres-backed runtime entrypoint outside explicit test or dev paths
- restart and recovery proof for durable runtime state
- service or API closure decision:
  - implement FastAPI if retained, or
  - remove FastAPI through ADR and aligned docs/spec/registry changes

## Out Of Scope

- Telegram adapter closure
- sidecar implementation
- research-only replaceable-stack decisions

## Change Surfaces

- CTX-ORCHESTRATION
- CTX-API-UI
- ARCH-DOCS

## Constraints

- Staging or prod-style profiles must not silently fall back to in-memory state.
- A plain Python class must not be represented as a service runtime.
- Keep the phase black-box testable from the outside.

## Acceptance Gate

- Runtime boot with durable state works from the agreed profile.
- Restart preserves state.
- The API or service surface is either booted and smoke-tested or formally removed from the target stack through ADR-backed updates.

## Disprover

- Attempt to start a staging-style profile without Postgres and confirm there is no silent in-memory fallback.

## Done Evidence

- Durable runtime default is explicit.
- Restart and recovery proof exists.
- FastAPI is either truly implemented with smoke proof or formally removed from the target stack.

## Rollback Note

- Revert the runtime default change if it creates hidden fallback paths or mixes durable and in-memory semantics again.
