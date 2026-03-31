# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-C - Contracts Freeze Closure
- Status: completed

## Objective

- перевести `contracts_freeze` из `partial` в fully governed implemented release surface.

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

- Нельзя изменить публичный boundary без schema + fixture + test delta.
- Contract tests cover all release-blocking envelopes.
- `contracts_freeze` can honestly move from `partial` to `implemented`.

## Disprover

- Изменить любой public payload без schema/fixture/test update и убедиться, что CI падает.

## Done Evidence

- `CONTRACT_SURFACES` and its companion schemas, fixtures, and tests cover all release-blocking envelopes.
- Compatibility rules are explicit and enforced by tests.
- The truth-source bundle no longer describes `contracts_freeze` as partial.

## Release Gate Impact

- Surface Transition: `contracts_freeze` `partial -> implemented`
- Minimum Proof Class: integration
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: contracts_freeze
- Delivered Proof Class: integration
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Live contour closure: this phase does not prove that every release-blocking boundary has been exercised against a real live contour.

## Rollback Note

- Revert any new contract claim that cannot be backed by schema, fixture, and regression tests together.
