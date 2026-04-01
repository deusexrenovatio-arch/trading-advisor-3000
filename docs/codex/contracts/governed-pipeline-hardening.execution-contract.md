# Execution Contract

Updated: 2026-04-01 11:20 UTC

## Source Package

- Package Zip: D:/trading advisor 3000/artifacts/spec-packages/governed-pipeline-hardening-technical-specification-2026-03-30.zip
- Package Manifest: D:/trading advisor 3000-wt-governed-20260401/artifacts/codex/package-intake/20260401T104845Z-governed-pipeline-hardening-technical-specificat/manifest.md
- Manifest Suggested Primary Document: docs/architecture/governed-pipeline-hardening-technical-specification.md
- Suggested Phase Compiler Artifact: NONE
- Source Title: Governed Pipeline Hardening Technical Specification

## Primary Source Decision

- Selected Primary Document: docs/architecture/governed-pipeline-hardening-technical-specification.md
- Selection Rule: the package contains one substantive supported specification document and the manifest already ranked it first, so it is selected directly as the primary source without tie-break.
- Supporting Documents:
  - `D:/trading advisor 3000-wt-governed-20260401/artifacts/codex/package-intake/20260401T104845Z-governed-pipeline-hardening-technical-specificat/manifest.md` for package provenance and ranking rationale.
  - no additional source documents were present in the extracted package.
- Conflict Status: no material contradiction was detected because the package is single-document.

## Prompt / Spec Quality

- Verdict: READY
- Why: the primary source defines objectives, functional and non-functional requirements, acceptance matrix, phased rollout, and definition of done with enough precision for governed phase planning.

## Normalization Note

- This intake run performs module-path normalization and phase planning only.
- The source declares an explicit rollout (`H0` through `H4`), so this patch materializes one execution contract, one module parent brief, and one phase brief per declared phase.
- `Suggested phase compiler artifact` is `NONE`; therefore phase ids and objectives are preserved from source rollout headings, while per-phase acceptance/disprover clauses are bound from source FR/acceptance clauses without reordering or collapsing phases.
- No implementation of hardening workstreams is performed in this intake patch.

## Objective

- Convert the governed pipeline hardening package into canonical governed execution artifacts so continuation can proceed phase by phase with explicit release-surface ownership.

## Release Target Contract

- Target Decision: ALLOW_RELEASE_READINESS
- Target Environment: governed pipeline running against real repository state with explicit snapshot/profile evidence across local Windows and hosted Ubuntu execution profiles
- Forbidden Proof Substitutes: docs-only, schema-only, fixture-only, mock-only, stub-only, smoke-only, route-report-only
- Release-Ready Proof Class: live-real

## Mandatory Real Contours

- snapshot_ci_profile_contour: snapshot-class and CI profile behavior must be real and reproducible beyond documentation-only declarations.
- stacked_followup_recomposition_contour: stacked continuation, multi-module ambiguity handling, and truth recomposition must operate on real merged history.
- enforcement_serialization_contour: explicit release enforcement and git mutation serialization must be proven with live-real behavior.

## Release Surface Matrix

- Surface: contract_schema_introduction | Owner Phase: H0 | Required Proof Class: doc | Must Reach: route_snapshot_profile_contracts_declared
- Surface: dual_mode_transition | Owner Phase: H1 | Required Proof Class: integration | Must Reach: backward_compatible_dual_mode_operation
- Surface: snapshot_ci_profile_contour | Owner Phase: H2 | Required Proof Class: staging-real | Must Reach: profile_aware_ci_and_proof_contract_operational
- Surface: stacked_followup_recomposition_contour | Owner Phase: H3 | Required Proof Class: staging-real | Must Reach: stacked_followup_and_recomposition_operational
- Surface: enforcement_serialization_contour | Owner Phase: H4 | Required Proof Class: live-real | Must Reach: explicit_release_enforcement_decision

## In Scope

- One canonical execution contract under `docs/codex/contracts/`.
- One module parent brief under `docs/codex/modules/`.
- Five ordered phase briefs preserving source rollout ids (`H0`..`H4`).
- Explicit release-gate ownership and accepted-state labels per phase.

## Out Of Scope

- Implementing FR-01 through FR-11 in this intake patch.
- Editing `docs/session_handoff.md` beyond its pointer-shim role.
- Re-running or replacing the governed launcher from inside this runtime prompt.
- Any product-plane business or trading logic changes.

## Constraints

- Treat the zip archive as one source package, not as pre-cleaned implementation instructions.
- Keep canonical gate names only (`run_loop_gate.py`, `run_pr_gate.py`, `run_nightly_gate.py`).
- Preserve source phase order and ids; no phase merge, rename, or softening.
- Do not represent docs-only or schema-only closure as release readiness.
- Avoid introducing a second active governed module pointer during intake normalization.

## Done Evidence

- `docs/codex/contracts/governed-pipeline-hardening.execution-contract.md` exists.
- `docs/codex/modules/governed-pipeline-hardening.parent.md` exists.
- `docs/codex/modules/governed-pipeline-hardening.phase-01.md` through `docs/codex/modules/governed-pipeline-hardening.phase-05.md` exist.
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_phase_planning_contract.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Primary Change Surfaces

- PROCESS-STATE
- GOV-DOCS

## Routing

- Path: module
- Rationale: the source is explicitly phase-driven and requires governed continuation by ordered module phases rather than one package-wide implementation patch.

## Mode Hint

- continue

## Next Allowed Unit Of Work
- Execute H4 - Enforcement Upgrade only: Deprecate implicit legacy behavior, require explicit snapshot/profile markers for policy-critical decisions, and enforce serialized git mutations for governed writes.
## Suggested Branch / PR

- Branch: codex/governed-pipeline-hardening-phase-plan
- PR Title: Normalize governed pipeline hardening module and phase plan
