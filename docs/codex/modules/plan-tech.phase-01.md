# Module Phase Brief

Updated: 2026-03-25 13:55 UTC

## Parent

- Parent Brief: docs/codex/modules/plan-tech.parent.md
- Execution Contract: docs/codex/contracts/plan-tech.execution-contract.md

## Phase

- Name: Policy and Critical Contour Foundations
- Status: completed

## Objective

- Establish the minimum policy and contract surfaces needed to express `target`, `staged`, and `fallback` for critical contours.

## In Scope

- One short anti-shortcut policy document for the shell layer.
- Task-note contract extensions for critical contours.
- A machine-readable critical contour config with the two pilot contours.
- Docs/checklist updates required to explain the new fields and pilot-only scope.

## Out Of Scope

- New validators.
- Gate wiring.
- Acceptance passports.
- Repo-wide contour expansion.

## Change Surfaces

- GOV-DOCS
- CTX-CONTRACTS

## Constraints

- Keep the policy lightweight and machine-checkable.
- Avoid new approval flow or ADR/waiver registry.
- Only critical contours get the extra required fields.

## Done Evidence

- A short policy document exists and names the three solution classes plus forbidden shortcut patterns.
- A critical contour config exists with the two pilot contours and required evidence markers.
- Task-contract docs explain the new critical-contour-only fields.

## Rollback Note

- Remove the new policy/config/docs surfaces and revert the task-note contract wording if phase 01 introduces confusion or breaks low-risk flows.
