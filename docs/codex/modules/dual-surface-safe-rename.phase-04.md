# Module Phase Brief

Updated: 2026-04-02 14:10 UTC

## Parent

- Parent Brief: docs/codex/modules/dual-surface-safe-rename.parent.md
- Execution Contract: docs/codex/contracts/dual-surface-safe-rename.execution-contract.md

## Phase

- Name: P4 - Runtime and Test Namespace Cutover
- Status: planned

## Objective

- Execute high-risk physical path cutover for product runtime and tests, with strict import, selector, and regression verification.

## In Scope

- Physical rename src/trading_advisor_3000/app -> src/trading_advisor_3000/product_plane.
- Physical rename tests/app -> tests/product-plane.
- Import, fixture, selector, and script updates required by the new namespace.

## Out Of Scope

- Final removal of compatibility bridge controls.
- Governance selector finalization for CODEOWNERS and CI branch protections.
- Migration closure report publication.

## Change Surfaces

- CTX-ORCHESTRATION
- GOV-RUNTIME
- ARCH-DOCS

## Constraints

- Runtime imports must resolve only through target namespace after cutover.
- Product-plane and architecture test subsets must pass.
- Compatibility bridge fallback remains available until phase 05 acceptance.

## Acceptance Gate

- Runtime namespace imports are resolved and tested on target paths.
- Product-plane tests execute from tests/product-plane selectors.
- Loop and PR gates stay green with no unresolved import regressions.

## Disprover

- Reintroduce one legacy runtime import path and verify tests or validators fail before acceptance.

## Done Evidence

- Runtime and tests namespace cutover patch with selector updates.
- Regression bundle for process, architecture, and product-plane tests.
- Verified rollback diff that can restore legacy namespaces if needed.

## Release Gate Impact

- Surface Transition: runtime and tests namespace cutover `bridge_mode -> target_namespace`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: runtime_test_namespace_cutover
- Delivered Proof Class: staging-real
- Required Real Bindings: full runtime import graph, product-plane test selectors, fixture path rewrites, and gate evidence on target namespace
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove ALLOW_RELEASE_READINESS.
- Final governance lock: this phase does not prove CI and ownership selectors are target-only.

## Rollback Note

- Revert runtime and test physical renames as one isolated rollback patch and re-enable bridge mode.
