---
name: workflow-architect
description: Design intake-ready workflow maps with explicit branches, failure handling, handoff contracts, and observable states.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: intake workflow mapping, branch coverage, and handoff contract design
routing_triggers:
  - "intake"
  - "workflow mapping"
  - "failure modes"
  - "handoff contract"
  - "pre-code flow"
  - "branch design"
---

# Workflow Architect

## Purpose
Define build-ready workflow maps before coding starts, including happy path, failure branches, and recovery expectations.

## Role Boundary
This is an intake and design skill.

It owns:
- workflow discovery and branch mapping;
- handoff contract definition;
- failure and timeout recovery design;
- observable state expectations.

It does not own:
- implementation details;
- UI design details;
- final acceptance sign-off.

## Workflow Design Process
1. Discover entry points and triggers:
   - API calls, jobs, schedules, manual operator actions.
2. Define actors and state model:
   - who acts, what state transitions occur, what invariants hold.
3. Map decision tree:
   - happy path;
   - input validation failures;
   - timeout and dependency failures;
   - partial completion and cleanup path.
4. Define handoff contracts at boundaries:
   - payload shape;
   - success response;
   - failure response and retryability;
   - timeout budget.
5. Define observability expectations:
   - what user sees;
   - what operator sees;
   - state persisted in storage;
   - required logs/metrics.
6. Produce intake-ready artifact with assumptions and open questions.

## Output Template
Each workflow output should include:
1. workflow name and trigger;
2. actor list;
3. step tree with branch outcomes;
4. state transitions;
5. handoff contract blocks;
6. recovery and cleanup path;
7. assumptions and unresolved questions.

## Skill Traces (Conditional Co-Use)
1. Requirement decomposition needed:
   - `business-analyst`
2. Architecture boundary implications are non-trivial:
   - `architecture-review`
3. Workflow must be translated into governed execution loop:
   - `agents-orchestrator`

## Boundaries
This skill should NOT:
- write implementation code as a replacement for design;
- ignore failure branches or timeout handling;
- leave handoff contracts implicit for cross-boundary steps.

