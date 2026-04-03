---
name: code-implementation-worker
description: Implement code with first-pass quality discipline for worker-stage delivery, including requirements traceability, safe error paths, and focused primary tests.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: worker-stage code implementation and primary quality guardrails
routing_triggers:
  - "write code"
  - "implement"
  - "worker"
  - "first-pass quality"
  - "code practices"
  - "performance"
---

# Code Implementation Worker

## Purpose
Increase first-pass code quality during worker execution: fewer follow-up fixes, cleaner behavior boundaries, and stronger confidence in changed paths.

## Role Boundary
This skill is for worker-stage coding only.

Worker owns:
- implementing requested behavior;
- preserving contracts and boundaries of changed paths;
- adding primary tests for changed behavior;
- running focused local checks for changed paths.

Worker does NOT own:
- intake discovery and requirement negotiation;
- architecture acceptance sign-off;
- final release acceptance and broad QA closure.

## First-Pass Implementation Workflow
1. Map request to executable change units before editing:
   - behavior to add or change;
   - interfaces or contracts touched;
   - data or state assumptions.
2. Implement smallest complete delta that closes the requested behavior.
3. Apply in-code quality guards during implementation:
   - explicit error paths;
   - predictable state transitions;
   - no hidden side effects across module boundaries.
4. Add primary tests closest to changed behavior:
   - one success-path test;
   - one failure-path or edge-case test;
   - no broad test expansion outside changed risk.
5. Run focused checks and report residual risk explicitly.

## Worker Quality Guardrails

### 1) Requirements Traceability In Code
- Every changed code path should map to a concrete requested behavior.
- Avoid speculative code not required by current scope.
- Keep naming aligned with requested outcomes, not internal shortcuts.

### 2) Contract And Boundary Safety
- If a payload, schema, or interface changes, update all connected boundaries in the same patch set.
- Do not leave partial contract migrations across layers.
- Prefer explicit adapters over direct cross-layer reach-through.

### 3) Error Handling Discipline
- Handle expected failures where they originate.
- Return actionable failure signals instead of silent fallbacks.
- Avoid broad exception swallowing.

### 4) Performance Hygiene For Changed Paths
- Check obvious hot-path risks in changed code:
  - repeated heavy calls inside loops;
  - unnecessary full-scan work for bounded queries;
  - blocking I/O in frequently called paths.
- Optimize only where change risk justifies it; avoid unrelated micro-optimization.

### 5) Test Scope Discipline
- Primary tests must cover changed behavior, not the whole subsystem.
- Keep tests deterministic and fast.
- Prefer behavior assertions over implementation-detail assertions.

## Skill Traces (Conditional Co-Use)
Use these traces only when the condition is true:

1. Contract or schema touched:
   - `registry-first`
2. Change crosses multiple layers or module boundaries:
   - `validate-crosslayer`
3. Need stronger primary test shaping for changed paths:
   - `testing-suite`
4. Config, env, or secrets touched:
   - `secrets-and-config-hardening`
5. Dependency add/upgrade in worker patch:
   - `dependency-and-license-audit`
6. Repeated implementation failure on same path:
   - `repeated-issue-review`

Do not auto-load all traces at once.
Select the minimum co-use set needed for the current patch.

## Output Contract For Worker Handoff
Worker handoff should state:
1. what behavior changed;
2. which contracts or interfaces were touched;
3. which primary tests were added or updated;
4. what remains as residual risk (if any).

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`

## Boundaries
This skill should NOT:
- expand into intake-phase requirement discovery work;
- replace architecture acceptance or release acceptance flows;
- claim completion without primary executable test evidence for changed behavior.
