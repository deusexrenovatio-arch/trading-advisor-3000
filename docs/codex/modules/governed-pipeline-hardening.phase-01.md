# Module Phase Brief

Updated: 2026-04-01 11:20 UTC

## Parent

- Parent Brief: docs/codex/modules/governed-pipeline-hardening.parent.md
- Execution Contract: docs/codex/contracts/governed-pipeline-hardening.execution-contract.md

## Phase

- Name: H0 - Contract Introduction
- Status: completed

## Objective

- Introduce route/snapshot/profile schemas and document the ephemeral session concept without switching runtime behavior yet.

## In Scope

- Route/snapshot/profile contract definitions for later validators and gates.
- Session-lifecycle documentation for ephemeral begin and tracked promotion semantics.
- Governance docs updates needed to anchor the new contract vocabulary.

## Out Of Scope

- Runtime behavior switch in launcher, gates, or session writers.
- CI profile execution changes.
- Multi-module route behavior changes.
- Git mutation serialization enforcement.

## Change Surfaces

- GOV-DOCS
- PROCESS-STATE

## Constraints

- Keep this phase documentation-contract only; no implicit enforcement activation.
- Keep canonical gate names unchanged.
- Keep `docs/session_handoff.md` a lightweight pointer shim.

## Acceptance Gate

- Route, snapshot, and profile contract terms are explicitly defined and reusable by later phases.
- Ephemeral session concept is documented with clear promotion boundary.
- No command or validator behavior is switched in this phase.

## Disprover

- If this phase introduces behavior-changing gate or session logic, or leaves snapshot/profile terms implicit, phase acceptance fails.

## Release Gate Impact

- Surface Transition: contract schema introduction `missing -> declared`
- Minimum Proof Class: doc
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: contract_schema_introduction
- Delivered Proof Class: doc
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Real contour closure: this phase does not prove real snapshot/profile execution behavior.

## Rollback Note

- Revert contract-vocabulary additions together if they introduce ambiguity without improving later-phase enforceability.
