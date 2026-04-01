# Module Phase Brief

Updated: 2026-04-01 11:20 UTC

## Parent

- Parent Brief: docs/codex/modules/governed-pipeline-hardening.parent.md
- Execution Contract: docs/codex/contracts/governed-pipeline-hardening.execution-contract.md

## Phase

- Name: H3 - Recomposition and Multi-Module Enablement
- Status: planned

## Objective

- Enable stacked follow-up continuation, machine-readable multi-module resolution, and tooling-assisted truth recomposition after split-branch merges.

## In Scope

- `stacked-followup` route support with predecessor merge context and new base contract.
- Multi-module route resolution contract for explicit slug choice, priority rules, and ambiguity reports.
- Truth recomposition helper and validator support for temporary split-branch downgrade restoration.

## Out Of Scope

- Enforcement deprecation decisions.
- Final release decision package generation.
- CI profile refactor beyond H2 ownership.
- Product-plane domain feature work.

## Change Surfaces

- GOV-RUNTIME
- PROCESS-STATE
- GOV-DOCS

## Constraints

- Follow-up continuation must not require manual branch archaeology.
- Ambiguity must produce machine-readable contracts before failing.
- Recomposition must restore already merged truth-source state before applying remaining contour deltas.

## Acceptance Gate

- After predecessor contour merge, follow-up route can rebuild remaining contour against updated `main` with explicit continuation contract.
- Multi-module ambiguity yields machine-readable resolution output instead of opaque hard failure.
- Truth recomposition flow restores merged truth and keeps only remaining contour deltas.

## Disprover

- If contour-B continuation still needs manual branch surgery after contour-A merge, or ambiguity lacks a resolution contract, this phase fails acceptance.

## Release Gate Impact

- Surface Transition: stacked follow-up and recomposition contour `manual -> staging-real governed`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: stacked_followup_recomposition_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: merged predecessor context, follow-up rebuild contract, ambiguity report artifact, recomposition validator evidence
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Enforcement closure: this phase does not prove final enforcement upgrade or live-real git serialization guarantees.

## Rollback Note

- Revert stacked-followup and recomposition changes together if they produce unresolved ambiguity or regress already merged truth-source surfaces.
