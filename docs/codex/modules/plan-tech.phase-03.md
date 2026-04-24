# Module Phase Brief

Updated: 2026-03-25 13:55 UTC

## Parent

- Parent Brief: docs/codex/modules/plan-tech.parent.md
- Execution Contract: docs/codex/contracts/plan-tech.execution-contract.md

## Phase

- Name: Routing and Pilot Passports
- Status: completed

## Objective

- Make critical contours pull in the right architecture and QA context and define the two pilot acceptance passports.

## In Scope

- Routing-policy updates for critical contours.
- Context and skills guidance that requires declaring `target`, `staged`, or `fallback` before implementation.
- Two short acceptance passports: data integration closure and runtime publication closure.

## Out Of Scope

- Repo-wide rollout to additional contours.
- Heavyweight architecture comparison requirements for all tasks.
- Full analytics program.

## Change Surfaces

- GOV-DOCS
- ARCH-DOCS
- PROCESS-STATE

## Constraints

- Keep the passports short and directly tied to real contour evidence.
- Make fallback declarations explicit when used.
- Only require architecture comparison for target-critical contour work.

## Release Gate Impact

- Surface transition: pilot_passport_routing_contour from enforced validators to documented routing and acceptance passports.
- Minimum proof class: doc
- Accepted state label: prep_closed

## Release Surface Ownership

- Owned surfaces: pilot_passport_routing_contour
- Delivered proof class: doc
- Required real bindings: none
- Target downgrade is forbidden: yes

## What This Phase Does Not Prove

- This phase does not prove release readiness; it defines routing and passport contracts for the pilot contours only.

## Done Evidence

- Routing docs describe the critical contour behavior.
- Two pilot passports exist and define target/staged meaning, forbidden green paths, required evidence, and re-acceptance triggers.
- Critical contour tasks can be routed without inventing a new approval process.

## Rollback Note

- Revert routing/passport docs together if the critical-contour behavior creates ambiguity or conflicts with existing context ownership.
