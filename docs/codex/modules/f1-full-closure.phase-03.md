# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-C - Contracts Freeze Closure
- Status: planned

## Objective

- Move `contracts_freeze` from partial to fully governed by versioning every release-blocking public boundary with schemas, fixtures, tests, and compatibility rules.

## In Scope

- runtime API request/response envelopes
- Telegram publication contract if the Telegram path remains active
- sidecar HTTP wire envelopes
- runtime configuration envelope
- persistence and migration boundary
- external rollout and connectivity envelopes
- compatibility matrix and contract change policy

## Out Of Scope

- real broker connector delivery
- release-runbook and operational-readiness packaging
- replaceable-stack decisions already assigned to `F1-B`

## Change Surfaces

- ARCH-DOCS
- CTX-CONTRACTS
- CTX-API-UI
- CTX-ORCHESTRATION

## Constraints

- A public payload change is invalid without the corresponding schema, fixture, and test delta.
- The contract set must be versioned and compatibility-aware rather than prose-only.
- Do not mark `contracts_freeze` implemented while any release-blocking envelope remains unversioned.

## Acceptance Gate

- Every release-blocking public boundary is represented by versioned schema and fixture artifacts with tests.
- Contract tests cover the release-blocking envelopes named by the phase.
- `contracts_freeze` can move honestly from `partial` to `implemented`.

## Disprover

- Mutate one public payload without a matching schema, fixture, and test update and confirm the phase fails.

## Done Evidence

- `CONTRACT_SURFACES` and its companion schemas, fixtures, and tests cover all release-blocking envelopes.
- Compatibility rules are explicit and enforced by tests.
- The truth-source bundle no longer describes `contracts_freeze` as partial.

## Rollback Note

- Revert any new contract claim that cannot be backed by schema, fixture, and regression tests together.
