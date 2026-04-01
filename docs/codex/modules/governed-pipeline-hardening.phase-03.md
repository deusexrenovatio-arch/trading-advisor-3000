# Module Phase Brief

Updated: 2026-04-01 11:20 UTC

## Parent

- Parent Brief: docs/codex/modules/governed-pipeline-hardening.parent.md
- Execution Contract: docs/codex/contracts/governed-pipeline-hardening.execution-contract.md

## Phase

- Name: H2 - CI and Proof Refactor
- Status: blocked

## Objective

- Split dependency and CI execution by surface profile and enforce a portable proof-runner contract across local and hosted environments.

## In Scope

- Dependency profile model separation (base, runtime/API, data-proof, proof-docker, dev/test umbrella).
- Surface-aware PR matrix selection and visibility.
- Shared proof-runner runtime contract with path and ownership normalization.
- Negative-path proof coverage for unwritable output, missing runtime root, and cross-platform path normalization.

## Out Of Scope

- Stacked follow-up route logic.
- Multi-module ambiguity resolution.
- Truth recomposition helper logic.
- Final enforcement/deprecation decisions.

## Change Surfaces

- GOV-RUNTIME
- GOV-DOCS
- PROCESS-STATE

## Constraints

- Hosted CI remains fail-closed while avoiding unrelated heavy proofs for narrow contours.
- Proof outputs must carry explicit profile markers.
- Linux hosted and Windows local proof semantics must stay reproducible.

## Acceptance Gate

- Runtime-only contour validation can execute with runtime-relevant dependency profile and matrix selection.
- Surface-aware CI selection is visible in CI logs and summary artifacts.
- Proof-runner contract behaves deterministically across hosted Ubuntu and local Windows profiles for governed proof paths.

## Disprover

- If a runtime-only contour still forces unrelated heavy data-proof setup, or hosted/local proof artifacts diverge silently, this phase fails acceptance.

## Release Gate Impact

- Surface Transition: snapshot and CI profile contour `declared -> staging-real`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: snapshot_ci_profile_contour
- Delivered Proof Class: staging-real
- Required Real Bindings: hosted-ubuntu ci evidence, local-windows replay evidence, profile-tagged proof artifacts
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Route continuity: this phase does not prove stacked follow-up, multi-module resolution, or truth recomposition closure.

## Rollback Note

- Revert profile matrix and proof portability changes together if they introduce non-deterministic cross-platform behavior.
