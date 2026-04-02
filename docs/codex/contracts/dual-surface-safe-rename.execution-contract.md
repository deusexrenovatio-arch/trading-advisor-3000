# Execution Contract

Updated: 2026-04-02 14:00 UTC

## Source Package

- Package Zip: D:/trading advisor 3000/artifacts/spec-packages/dual-surface-safe-rename-migration-spec-2026-04-02.zip
- Package Manifest: artifacts/codex/package-intake/20260402T110810Z-dual-surface-safe-rename-migration-spec-2026-04-/manifest.md
- Source Title: Dual-Surface Safe Rename Migration Technical Specification

## Primary Source Decision

- Selected Primary Document: docs/architecture/dual-surface-safe-rename-migration-technical-specification.md
- Selection Rule: prefer the explicit migration specification that defines phase objectives, entry criteria, deliverables, and rollback rules over supporting index references.
- Supporting Documents:
  - docs/architecture/repository-surfaces.md for current namespace map and deferred rename candidates.
  - docs/architecture/README.md for architecture index linkage.
- Conflict Status: no material contradiction found between selected primary and supporting architecture references.

## Prompt / Spec Quality

- Verdict: READY
- Why: the source explicitly defines phased migration goals, risk controls, acceptance gates, and validation matrix for a governed rollout.

## Objective

- Execute dual-surface physical path migration in isolated governed phases so navigation clarity improves without runtime, CI, or governance regression.

## Release Target Contract

- Target Decision: DENY_RELEASE_READINESS
- Target Environment: governed dual-surface migration route where each phase must prove compatibility and rollback safety before advancing
- Forbidden Proof Substitutes: docs-only, checklist-only, narrative-only, smoke-only, route-report-only, unstamped-manual-proof
- Release-Ready Proof Class: live-real

## Mandatory Real Contours

- governed_migration_acceptance: final migration decision must include reproducible full-gate evidence and explicit go or no-go release decision package.

## Release Surface Matrix

- Surface: inventory_traceability | Owner Phase: P1 | Required Proof Class: doc | Must Reach: full_legacy_reference_inventory
- Surface: compatibility_bridge_controls | Owner Phase: P2 | Required Proof Class: integration | Must Reach: dual_path_bridge_with_block_new_legacy_refs
- Surface: docs_namespace_cutover | Owner Phase: P3 | Required Proof Class: integration | Must Reach: docs_product_plane_namespace_without_link_regression
- Surface: runtime_test_namespace_cutover | Owner Phase: P4 | Required Proof Class: staging-real | Must Reach: runtime_and_tests_on_target_namespace
- Surface: governance_selector_cutover | Owner Phase: P5 | Required Proof Class: integration | Must Reach: ci_and_codeowners_target_namespace_only
- Surface: governed_migration_acceptance | Owner Phase: P6 | Required Proof Class: live-real | Must Reach: final_governed_migration_decision_package

## In Scope

- Canonical execution contract and module phase briefs under docs/codex for rename continuation.
- Phase-scoped inventory artifacts for legacy path references with risk and wave labels.
- Strict gate-driven promotion between compatibility bridge, docs rename, runtime/test rename, and governance cutover phases.

## Out Of Scope

- Trading or strategy logic changes.
- Cosmetic rewrites of archived historical artifacts.
- One-shot bulk rename outside governed phase order.

## Constraints

- Package intake and continuation must use scripts/codex_governed_entry.py.
- Mainline integration remains PR-only with one phase per PR.
- Keep shell control-plane paths domain free.
- Respect migration order from the technical specification.

## Done Evidence

- docs/codex/contracts/dual-surface-safe-rename.execution-contract.md exists.
- docs/codex/modules/dual-surface-safe-rename.parent.md exists.
- docs/codex/modules/dual-surface-safe-rename.phase-01.md through docs/codex/modules/dual-surface-safe-rename.phase-06.md exist.
- python scripts/validate_phase_planning_contract.py
- python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none
- python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none

## Primary Change Surfaces

- PROCESS-STATE
- GOV-DOCS
- GOV-RUNTIME
- ARCH-DOCS

## Routing

- Path: module
- Rationale: the source is explicitly phased, and the migration is high-risk enough to require governed continuation rather than one package-sized implementation patch.

## Mode Hint

- continue

## Next Allowed Unit Of Work
- Execute P5 governance/CI selector cutover after runtime and test namespace cutover evidence and green loop/pr gates for phase-04.

## Suggested Branch / PR

- Branch: codex/dual-surface-rename-phase4
- PR Title: Phase 4 runtime and test namespace cutover for dual-surface safe rename migration
