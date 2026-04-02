# Dual-Surface Safe Rename Migration Technical Specification

Status: draft for implementation
Date: 2026-04-02
Audience: repository maintainers, architecture owners, governance owners, platform owners
Scope: safe physical renaming of repository paths to strengthen dual-surface clarity (`shell` + `product-plane`) without regressions in runtime, CI, validation, or navigation

## 1. Purpose

Define a fail-safe migration program for physical path renaming in a dual-surface repository.

The goal is not cosmetic refactoring.
The goal is structural clarity with preserved reliability:
- clear navigation from first repository glance;
- explicit and enforceable boundary between `shell` and `product-plane`;
- zero silent breakage in imports, scripts, tests, CI, docs links, and governance gates.

## 2. Problem Statement

Current repository navigation already uses dual-surface terminology, but core filesystem paths still contain legacy names (`app`) that can be ambiguous for new contributors.

Immediate mass rename is risky because path references are spread across:
- Python imports and runtime wiring;
- tests and fixtures;
- scripts and validators;
- docs and cross-links;
- CODEOWNERS and CI workflows;
- historical artifacts and operational runbooks.

Without a phased migration contract, rename work can create hidden regressions:
1. partial path rewrites pass local checks but fail in CI;
2. doc links stay stale and erode trust;
3. governance checks route changes to wrong owners;
4. rollback becomes unclear because many unrelated changes are mixed.

## 3. Objectives

### O1. Preserve runtime and governance stability
Renaming must not degrade shell gates, validation lanes, or product-plane testability.

### O2. Make repository surfaces explicit in filesystem terms
Target paths should align with canonical vocabulary:
- `shell`
- `product-plane`
- `mixed` (change classification, not path namespace)

### O3. Enforce phased, auditable migration
Each phase must have:
- entry criteria;
- concrete deliverables;
- Definition of Done;
- rollback strategy.

### O4. Minimize irreversible blast radius
Use compatibility bridges and staged cutover before deleting legacy paths.

### O5. Keep shell boundaries intact
No trading or product business logic may be moved into shell control-plane surfaces during rename.

## 4. Non-Goals

This specification does not aim to:
- redesign business capabilities or product architecture;
- rewrite historical archived artifacts for cosmetic consistency;
- force one giant rename PR;
- bypass existing loop/pr/nightly validation contracts;
- relax PR-only policy for `main`.

## 5. Canonical Rename Scope

## 5.1 In-scope rename candidates

Primary candidates (subject to phase gates):
1. `docs/architecture/app/` -> `docs/architecture/product-plane/`
2. `tests/app/` -> `tests/product-plane/`
3. `src/trading_advisor_3000/app/` -> `src/trading_advisor_3000/product_plane/`

Secondary alignment candidates:
4. app-scoped references in docs/checklists/runbooks/workflows indexes.
5. CI and CODEOWNERS patterns that route product-plane changes.

## 5.2 Out of scope

- Root package name `src/trading_advisor_3000/`.
- Historical task archives and frozen generated packages.
- Non-rename feature work.

## 6. Migration Strategy (Phase Model)

## Phase 0 - Contract Freeze And Baseline Capture

Purpose:
Lock migration contract and baseline current behavior before any physical rename.

Entry criteria:
- Dual-surface terminology already present in top-level docs.
- Critical validators and gates are green on current baseline.

Deliverables:
1. Approved technical specification (this document).
2. Explicit migration backlog by wave and owner.
3. Baseline evidence bundle:
   - docs link validation;
   - loop gate summary;
   - PR gate summary;
   - product-plane test subset summary.

Definition of Done:
- Baseline artifacts are recorded and reviewable.
- No physical path rename yet.

Rollback:
- Not applicable (documentation-only phase).

## Phase 1 - Full Dependency And Reference Inventory

Purpose:
Build complete impact map so no path reference is missed later.

Entry criteria:
- Phase 0 DoD complete.

Deliverables:
1. Machine-readable inventory of old-path references grouped by type:
   - imports/code;
   - test paths/fixtures;
   - scripts/validators;
   - docs links;
   - CI and CODEOWNERS;
   - packaging and runbook references.
2. Risk labeling per reference:
   - low (pure doc links),
   - medium (test and script references),
   - high (runtime imports, CI selectors, validators).
3. Rename wave assignment per reference cluster.

Definition of Done:
- No unknown path class remains.
- Every high-risk reference has an owning wave.

Rollback:
- Revert inventory artifacts only (no runtime impact).

## Phase 2 - Compatibility Bridge Layer

Purpose:
Introduce temporary compatibility so old and new paths can coexist during migration.

Entry criteria:
- Phase 1 inventory approved.

Deliverables:
1. Transitional import/path adapters where needed.
2. Transitional docs redirects or pointer pages for renamed doc roots.
3. Validation rule: block new references to legacy names in newly changed files.
4. CI checks updated to accept dual-path mode during migration window.

Definition of Done:
- Repository works with compatibility enabled.
- Both legacy and target namespaces are test-covered where applicable.

Rollback:
- Disable bridge rules and revert to pre-bridge baseline in one patch set.

## Phase 3 - Low-Risk Physical Renames (Docs-First)

Purpose:
Execute low-risk path changes first to validate migration mechanics.

Entry criteria:
- Compatibility bridge active.
- Dual-path CI mode proven.

Deliverables:
1. Physical rename of documentation subtree candidate:
   - `docs/architecture/app/` -> `docs/architecture/product-plane/`.
2. Update all internal links and indexes.
3. Update governance docs and navigation hubs.

Definition of Done:
- `validate_docs_links.py` passes.
- No unresolved links to old doc root in tracked docs.
- Reviewers confirm navigation clarity improved.

Rollback:
- Restore old doc path and reverse link rewrites.

## Phase 4 - Runtime/Test Path Renames (High-Risk Core)

Purpose:
Perform product-plane code and test namespace rename with strict safety gates.

Entry criteria:
- Phase 3 stable for at least one PR cycle.
- Full impact set for runtime/test references is approved.

Deliverables:
1. Physical code path rename candidate:
   - `src/trading_advisor_3000/app/` -> `src/trading_advisor_3000/product_plane/`.
2. Physical test path rename candidate:
   - `tests/app/` -> `tests/product-plane/`.
3. Bulk update of imports, fixtures, script selectors, and validator patterns.
4. Extended regression run:
   - process tests,
   - architecture tests,
   - product-plane tests,
   - selected integration smoke tests.

Definition of Done:
- All runtime imports resolve on target namespace.
- CI lanes pass without legacy-path fallback for renamed code/test surfaces.
- No feature behavior regression is detected in acceptance subset.

Rollback:
- Revert this phase as isolated patch series.
- Re-enable compatibility bridge for prior namespace.

## Phase 5 - Governance/CI/Ownership Cutover

Purpose:
Switch control-plane automation fully to target names.

Entry criteria:
- Phase 4 stable and green.

Deliverables:
1. Finalize CODEOWNERS patterns for target namespaces only.
2. Finalize CI and validator selectors to target paths.
3. Update PR templates/checklists to remove legacy namespace hints.
4. Add guard checks failing on new legacy-path references.

Definition of Done:
- Governance routing and CI operate only on target names.
- Legacy-path additions are blocked by default.

Rollback:
- Restore previous CODEOWNERS/CI selectors and reopen compatibility mode.

## Phase 6 - Legacy Cleanup And Closure

Purpose:
Remove temporary bridges and close migration with auditable acceptance.

Entry criteria:
- One full cycle of green pipelines after Phase 5.

Deliverables:
1. Remove compatibility adapters and temporary redirects.
2. Purge residual legacy references in active docs/scripts/tests.
3. Publish migration closeout report:
   - completed waves,
   - incidents,
   - resolved risks,
   - residual debt.

Definition of Done:
- No active path dependency on legacy names.
- All required gates and tests are green in final mode.
- Migration report approved by owners.

Rollback:
- Reintroduce compatibility bridge from tagged pre-closure baseline.

## 7. Cross-Phase Acceptance Criteria

Global acceptance requires all of the following:
1. `shell` and `product-plane` boundaries remain explicit and unchanged in intent.
2. No business logic appears in shell control-plane surfaces.
3. Docs navigation remains valid and first-glance clear.
4. Governance gates (`loop`, `pr`) remain fail-closed and deterministic.
5. CODEOWNERS and CI routing are correct for target namespaces.
6. Rollback path is proven for each high-risk phase before advancing.

## 8. NFR Requirements

### NFR-01. Reliability first
Migration quality has higher priority than migration speed.

### NFR-02. Atomicity
Each phase must be deliverable as isolated, reviewable patch series.

### NFR-03. Reversibility
Every high-risk phase must have a documented one-step rollback strategy.

### NFR-04. Traceability
Phase evidence must be archived under `artifacts/` with clear timestamps.

### NFR-05. Operator clarity
At any point, contributors must understand which namespace is canonical.

## 9. Risk Register (Initial)

| Risk | Impact | Likelihood | Mitigation | Phase Owner |
| --- | --- | --- | --- | --- |
| Hidden import/runtime references missed | high | medium | Phase 1 full inventory + Phase 4 gated rollout | platform + app-core |
| Broken docs links after doc rename | medium | medium | Phase 3 docs-link validation and index updates | architecture |
| CI selects wrong paths | high | medium | Phase 5 selector cutover with dual-path test period | platform |
| Ownership misrouting in CODEOWNERS | medium | medium | parallel CODEOWNERS audit in Phase 5 | process + architecture |
| Rollback complexity after mixed patches | high | medium | enforce one-phase patch boundaries | all phase leads |

## 10. Governance And Decision Gates

Promotion rule between phases:
1. DoD of current phase complete.
2. Mandatory validation bundle attached.
3. Explicit go/no-go decision recorded.
4. For repeated failure signatures: stop expansion and remediate before continuing.

Mandatory go/no-go checks:
- docs link validation;
- loop gate;
- PR gate;
- targeted product-plane tests;
- ownership and CI selector sanity checks (from Phase 5 onward).

## 11. Validation Matrix

Minimum command set by closeout phases:
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python -m pytest tests/process -q`
- `python -m pytest tests/architecture -q`
- `python -m pytest tests/app -q` (until test namespace is renamed)

After test path cutover:
- replace `tests/app` references with the new `tests/product-plane` equivalents in all commands and CI selectors.

## 12. Implementation Backlog (Phase-Aligned)

### B0 (Phase 0)
- Approve this specification and create migration checklist artifact.

### B1 (Phase 1)
- Generate reference inventory and classify by risk.
- Produce per-wave patch map.

### B2 (Phase 2)
- Add compatibility adapters and anti-regression checks.
- Enable dual-path CI mode.

### B3 (Phase 3)
- Rename docs subtree and update all links/indexes.

### B4 (Phase 4)
- Rename runtime and test namespaces.
- Update imports/tests/scripts and run full regression.

### B5 (Phase 5)
- Finalize CODEOWNERS, CI, validators, and template routing.

### B6 (Phase 6)
- Remove compatibility layer.
- Publish migration closure report.

## 13. Definition of Done (Program Level)

Program is complete only when:
1. Canonical filesystem naming is aligned with dual-surface terms.
2. Legacy namespace usage is removed from active surfaces.
3. CI and validators are target-namespace native.
4. Documentation and navigation are stable and verified.
5. Rollback strategy is documented and proven for high-risk waves.
6. No shell boundary violations were introduced during migration.

## 14. Open Questions

1. Should doc root rename (`app` -> `product-plane`) be completed before or after runtime namespace rename if CI capacity is constrained?
2. Do we maintain a long-lived alias layer for external tools, or hard-cut after one release cycle?
3. Which exact test subsets are mandatory for Phase 4 go/no-go in resource-constrained runs?
4. Do we need a temporary contributor guideline for both old and new names during migration window?
5. Should generated historical packages be left untouched (recommended) or optionally rebuilt under new names?
