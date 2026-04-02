# Module Parent Brief

Updated: 2026-04-02 15:05 UTC

## Source

- Package Zip: D:/trading advisor 3000/artifacts/spec-packages/dual-surface-safe-rename-migration-spec-2026-04-02.zip
- Execution Contract: docs/codex/contracts/dual-surface-safe-rename.execution-contract.md

## Module Objective

- Deliver safe dual-surface physical path migration in strict phase order, with compatibility-first controls and deterministic acceptance evidence for each PR slice.

## Why This Is Module Path

- The migration spans docs, runtime imports, tests, scripts, validators, CI selectors, and ownership routing.
- The specification explicitly demands phased rollout, rollback notes, and no giant PR.
- High-risk namespace changes require governed continuation and gate-backed promotion.

## Phase Order

1. Phase 01 - P1 Full Dependency and Reference Inventory
2. Phase 02 - P2 Compatibility Bridge Layer
3. Phase 03 - P3 Low-Risk Physical Rename for Docs
4. Phase 04 - P4 Runtime and Test Namespace Cutover
5. Phase 05 - P5 Governance and CI Selector Cutover
6. Phase 06 - P6 Legacy Cleanup and Migration Closure

## Phase State Reconstruction

- Specification and architecture linkage are already landed from preparatory PRs.
- This module starts active execution at Phase 01 with explicit inventory deliverables before any physical rename.

## Global Constraints

- Keep each phase in an isolated PR.
- Do not mix compatibility bridge, runtime cutover, and final governance selector closure in one patch.
- Keep shell control-plane surfaces free of product business logic.
- Stop expansion and remediate process if the same validation failure repeats twice.

## Global Done Evidence

- Every phase brief has explicit acceptance, disprover, release-impact ownership, and rollback notes.
- Parent pointer advances only after gate evidence is green for the current phase.
- Final phase emits explicit governed go or no-go migration closeout package.

## Open Risks

- Hidden legacy references in validators or CI selectors can create delayed regressions.
- Runtime and test namespace cutover can fail silently if compatibility bridge coverage is incomplete.
- Governance selector cutover can misroute ownership if CODEOWNERS and workflow scopes drift.

## Next Phase To Execute
- docs/codex/modules/dual-surface-safe-rename.phase-03.md
