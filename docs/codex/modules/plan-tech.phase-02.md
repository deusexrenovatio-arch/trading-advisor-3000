# Module Phase Brief

Updated: 2026-03-25 13:55 UTC

## Parent

- Parent Brief: docs/codex/modules/plan-tech.parent.md
- Execution Contract: docs/codex/contracts/plan-tech.execution-contract.md

## Phase

- Name: Validator and Gate Enforcement
- Status: completed

## Objective

- Add the two fail-closed validators and wire them into the existing local, loop, PR, and nightly checks.

## In Scope

- `validate_solution_intent.py`
- `validate_critical_contour_closure.py`
- Gate/checks matrix updates for local, loop, PR, and nightly.
- Focused unit and process tests for the new behavior.

## Out Of Scope

- New gate lanes.
- Pilot acceptance passports.
- Broad static analysis beyond the pilot contours.

## Change Surfaces

- GOV-RUNTIME
- CTX-CONTRACTS
- GOV-DOCS

## Constraints

- Fail closed when required contour evidence is missing.
- Do not duplicate existing proving flow.
- Keep tests focused on shortcut-pattern detection, not generic architecture theory.

## Release Gate Impact

- Surface transition: critical_contour_gate_enforcement from policy-defined to enforced in loop/pr validation.
- Minimum proof class: staging-real
- Accepted state label: real_contour_closed

## Release Surface Ownership

- Owned surfaces: critical_contour_gate_enforcement
- Delivered proof class: staging-real
- Required real bindings: changed-file loop/pr gate execution against configured pilot contour paths
- Target downgrade is forbidden: yes

## What This Phase Does Not Prove

- This phase does not prove release readiness; it proves only pilot critical-contour gate enforcement for the shell governance route.

## Done Evidence

- Both validators exist and are called by the existing checks and gates.
- Tests prove that `target` without proper closure evidence fails, and valid `staged` wording passes.
- Docs-only and low-risk shell diffs still avoid the extra critical-contour burden.

## Rollback Note

- Remove gate wiring and validator scripts together if enforcement proves too noisy or blocks non-critical work incorrectly.
