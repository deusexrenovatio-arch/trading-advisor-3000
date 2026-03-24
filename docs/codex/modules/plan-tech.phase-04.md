# Module Phase Brief

Updated: 2026-03-24 15:00 UTC

## Parent

- Parent Brief: docs/codex/modules/plan-tech.parent.md
- Execution Contract: docs/codex/contracts/plan-tech.execution-contract.md

## Phase

- Name: Observation Counters and Expansion Criteria
- Status: planned

## Objective

- Observe the pilot contours, keep only the three lightweight counters, and define the rule for widening the contour set.

## In Scope

- Observation notes for the 1-2 week pilot window
- Three limited dashboard counters
- Expansion criteria for adding new contours after the pilot

## Out Of Scope

- Full KPI program
- Separate analytics layer
- Immediate rollout to all contours

## Change Surfaces

- GOV-DOCS
- GOV-RUNTIME

## Constraints

- Keep observation lightweight and tied to real false-positive behavior.
- Expand contour coverage only after the pilot shows acceptable noise levels.
- Preserve the repo's existing reporting lanes instead of creating a new analytics subsystem.

## Commands

- python scripts/validate_docs_links.py --roots AGENTS.md docs
- python scripts/run_loop_gate.py --from-git --git-ref HEAD

## Done Evidence

- The three counters are defined and mapped to the pilot contours.
- Expansion criteria exist for live transport closure and durable storage closure.
- The pilot observation window is explicit before broadening scope.

## Rollback Note

- Drop the extra observation layer if it starts behaving like a heavyweight KPI program instead of a lightweight pilot aid.

## Next Allowed Step

- Start this phase only after the pilot contours have been exercised long enough to judge false positives and staged-vs-target quality.
