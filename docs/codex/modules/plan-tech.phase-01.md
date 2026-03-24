# Module Phase Brief

Updated: 2026-03-24 15:08 UTC

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

## Commands

- python scripts/validate_docs_links.py --roots AGENTS.md docs
- python scripts/validate_task_request_contract.py
- python scripts/validate_session_handoff.py
- python scripts/run_loop_gate.py --from-git --git-ref HEAD

## Done Evidence

- A short policy document exists and names the three solution classes plus forbidden shortcut patterns.
- A critical contour config exists with the two pilot contours and required evidence markers.
- Task-contract docs explain the new critical-contour-only fields.

## Rollback Note

- Remove the new policy/config/docs surfaces and revert the task-note contract wording if phase 01 introduces confusion or breaks low-risk flows.

## Next Allowed Step

- Start phase 02 only after the phase-01 policy/config/task-note foundations are green in the loop gate.
