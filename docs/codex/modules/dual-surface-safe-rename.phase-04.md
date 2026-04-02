# Module Phase Brief

Updated: 2026-04-02 14:00 UTC

## Parent

- Parent Brief: docs/codex/modules/dual-surface-safe-rename.parent.md
- Execution Contract: docs/codex/contracts/dual-surface-safe-rename.execution-contract.md

## Phase

- Name: P4 - Runtime and Test Namespace Cutover
- Status: completed

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

- Physical runtime cutover completed: `src/trading_advisor_3000/app/` -> `src/trading_advisor_3000/product_plane/`.
- Physical test cutover completed: `tests/app/` -> `tests/product-plane/`.
- Legacy import compatibility preserved via bridge at `src/trading_advisor_3000/app/__init__.py`.
- Runtime/test selector updates landed across governance and validation surfaces:
  - `configs/change_surface_mapping.yaml`
  - `configs/critical_contours.yaml`
  - `scripts/context_router.py`
  - `scripts/run_surface_pr_matrix.py`
  - `registry/stack_conformance.yaml`
- Critical contour validation now supports explicit multi-contour intent for coordinated high-risk namespace cutovers:
  - `scripts/validate_solution_intent.py`
  - `scripts/validate_critical_contour_closure.py`
  - `tests/process/test_critical_contours.py`
- Regression evidence:
  - `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
  - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`

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
