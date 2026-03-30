# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-E - Real Broker Process Closure
- Status: planned

## Objective

- Move `real_broker_process` from planned to implemented by proving a real agreed broker connector contour inside the in-repo sidecar process.

## In Scope

- in-repo sidecar connector path for the agreed broker surface
- versioned secrets and staging connector configuration contract
- staging-first proof profile for boot, readiness, kill-switch, submit, replace, cancel, updates, and reconciliation
- explicit failure-mode handling and tests for connector miswire and runtime recovery

## Out Of Scope

- broad production rollout claims
- final operational-readiness sign-off
- unrelated shell governance changes outside the broker contour

## Change Surfaces

- CTX-ORCHESTRATION
- CTX-CONTRACTS
- GOV-DOCS
- ARCH-DOCS

## Constraints

- HTTP stubs, wire docs, or mock-only gateways do not close this phase.
- The broker connector must be real for the agreed staging contour.
- Secrets, readiness semantics, failure modes, and recovery behavior must be explicit and versioned.

## Acceptance Gate

- `real_broker_process` can move honestly from `planned` to `implemented`.
- STATUS, registry, runbooks, runtime code, and tests describe the same broker contour.
- Negative tests cover missing credentials, connector unavailability, session-not-ready, kill-switch active, rejected order, and timeout or retry behavior.

## Disprover

- Run the staging profile with a miswired or unavailable connector and confirm the phase fails rather than overclaiming readiness.

## Done Evidence

- A real broker connector contour exists in the sidecar and is exercised by governed proof.
- The config and secrets contract is versioned and documented.
- Recovery and failure semantics are executable rather than narrative-only.

## Release Gate Impact

- Surface Transition: `real_broker_process` `planned -> implemented`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: broker_execution_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: real broker connector contour, real connector configuration and secrets, and replayable staging-real broker proof
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Production-wide closure: this phase does not prove broad production rollout beyond the agreed broker contour.

## Rollback Note

- Revert any broker readiness claim immediately if the proof contour depends on stubs, hidden credentials, or unreplayed local state.
