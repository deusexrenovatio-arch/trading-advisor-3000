# Module Phase Brief

Updated: 2026-03-24 15:00 UTC

## Parent

- Parent Brief: docs/codex/modules/plan-tech.parent.md
- Execution Contract: docs/codex/contracts/plan-tech.execution-contract.md

## Phase

- Name: Validator and Gate Enforcement
- Status: planned

## Objective

- Add the two fail-closed validators and wire them into the existing local, loop, PR, and nightly checks.

## In Scope

- validate_solution_intent.py
- validate_critical_contour_closure.py
- Gate/checks matrix updates for local, loop, PR, and nightly
- Focused unit and process tests for the new behavior

## Out Of Scope

- New gate lanes
- Pilot acceptance passports
- Broad static analysis beyond the pilot contours

## Change Surfaces

- GOV-RUNTIME
- CTX-CONTRACTS
- GOV-DOCS

## Constraints

- Fail closed when required contour evidence is missing.
- Do not duplicate existing proving flow.
- Keep tests focused on shortcut-pattern detection, not generic architecture theory.

## Commands

- python -m pytest tests/process/test_validate_task_request_contract.py -q
- python -m pytest tests/process/test_context_router.py -q
- python scripts/validate_task_request_contract.py
- python scripts/validate_session_handoff.py
- python scripts/run_loop_gate.py --from-git --git-ref HEAD

## Done Evidence

- Both validators exist and are called by the existing checks and gates.
- Tests prove that `target` without proper closure evidence fails, and valid `staged` wording passes.
- Docs-only and low-risk shell diffs still avoid the extra critical-contour burden.

## Rollback Note

- Remove gate wiring and validator scripts together if enforcement proves too noisy or blocks non-critical work incorrectly.

## Next Allowed Step

- After phase 01 is green, implement validator logic and gate wiring in one reviewable patch set.
