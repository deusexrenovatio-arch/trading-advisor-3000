# Module Phase Brief

Updated: 2026-03-31 19:12 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-E - Real Broker Process Closure
- Status: blocked

## Objective

- перевести `StockSharp/QUIK/Finam real broker process` из `planned` в implemented release-blocking surface.
- Finam-native session and transport proof remain mandatory for the staging-real closure contour.

## In Scope

- in-repo sidecar execution path for the agreed broker surface
- versioned secrets and staging Finam session configuration contract
- Finam-native preflight proof via session details (`/v1/sessions/details`) with real JWT binding
- staging-first proof profile for boot, readiness, kill-switch, submit, replace, cancel, updates, and reconciliation
- explicit failure-mode handling and tests for transport miswire and runtime recovery

## Out Of Scope

- broad production rollout claims
- final operational-readiness sign-off
- unrelated shell governance changes outside the broker contour

## Change Surfaces

- CTX-ORCHESTRATION
- CTX-CONTRACTS
- GOV-RUNTIME
- GOV-DOCS
- ARCH-DOCS

## Constraints

- HTTP stubs, wire docs, or mock-only gateways do not close this phase.
- Finam session proof must be real for the agreed staging contour.
- Vendor API root cannot be treated as gateway `/health`; proof is bound to Finam-native session contract.
- Secrets, readiness semantics, failure modes, and recovery behavior must be explicit and versioned.

## Acceptance Gate

- `real_broker_process` can move from `planned` to `implemented`.
- STATUS, registry, docs and runbooks reflect the same scope.
- No doc uses words like `production ready` unless release readiness phase also passes.

## Disprover

- Run the staging profile with unreachable sidecar transport and confirm fail-closed behavior.
- Run Finam session preflight with invalid or expired JWT and confirm the phase fails rather than overclaiming readiness.

## Done Evidence

- Real Finam session contour is proved in governed artifacts (`/v1/sessions/details` + required fields).
- Sidecar staging-real lifecycle (boot/readiness/kill-switch/submit/replace/cancel/updates/reconciliation) is exercised by governed proof.
- The config and secrets contract is versioned and documented.
- Recovery and failure semantics are executable rather than narrative-only.

## Release Gate Impact

- Surface Transition: `real_broker_process` `planned -> implemented`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: broker_execution_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: real Finam API contour (session JWT), real connector configuration and secrets, and replayable staging-real broker proof
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Production-wide closure: this phase does not prove broad production rollout beyond the agreed broker contour.

## Rollback Note

- Revert any broker readiness claim immediately if the proof contour depends on stubs, hidden credentials, or unreplayed local state.
