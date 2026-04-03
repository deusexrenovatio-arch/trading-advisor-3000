---
name: agents-orchestrator
description: Coordinate multi-role delivery pipelines with explicit phase gates, retry policy, and evidence-based progression.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: multi-agent orchestration policy, handoffs, and governed progression
routing_triggers:
  - "orchestrator"
  - "orchestration"
  - "multi-agent pipeline"
  - "dev qa loop"
  - "phase gate"
  - "handoff"
---

# Agents Orchestrator

## Purpose
Run end-to-end delivery flow with explicit phase control, independent quality gates, and fail-closed progression.

## Governed Baseline
- Execution must follow governed route entry, not chat-only continuation.
- Phase progression is blocked until acceptance criteria are met.
- Retry loops are bounded and must escalate when repeated failures persist.

## Phase Model
1. Intake and scope framing
2. Architecture and boundary shaping
3. Worker implementation
4. Acceptance and verification
5. Closeout and documentation sync

Only one phase is active at a time. Next phase unlocks only after current phase passes.

## Orchestration Workflow
1. Build current-phase objective and evidence contract.
2. Launch role with explicit scope, expected artifacts, and boundary constraints.
3. Collect output and run phase gate checks.
4. Decision:
   - `PASS`: unlock next phase;
   - `BLOCKED`: trigger remediation cycle;
   - `ESCALATE`: stop expansion and record blocker.
5. Record state:
   - active phase;
   - attempts;
   - blockers;
   - accepted evidence.

## Retry and Escalation Policy
- Default max attempts per blocked phase: 3.
- Every retry must include concrete feedback, not generic "try again".
- If blocker pattern repeats, escalate with remediation options and stop drift.

## Handoff Contract
Every role handoff should include:
1. objective and out-of-scope list;
2. required checks and evidence artifacts;
3. hard constraints and forbidden shortcuts;
4. exact unblock condition.

## Skill Composition
Use minimal set needed for each phase:
1. Intake:
   - `workflow-architect`
   - `business-analyst`
2. Architecture shaping:
   - `architecture-review`
   - `ai-agent-architect`
3. Worker implementation:
   - `code-implementation-worker`
4. Acceptance:
   - `phase-acceptance-governor`
   - `verification-before-completion`
   - `testing-suite`
5. Documentation closure:
   - `docs-sync`

## Boundaries
This skill should NOT:
- bypass governed route or phase gate contracts;
- collapse independent acceptance into worker self-approval;
- continue phase expansion when repeated blockers indicate unresolved root cause.

