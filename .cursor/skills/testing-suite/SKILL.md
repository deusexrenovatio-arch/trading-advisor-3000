---
name: testing-suite
description: Maintain unit, integration, contract, and process test suites for governance confidence.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: test suite strategy and maintenance
routing_triggers:
  - "tests"
  - "coverage"
  - "integration"
  - "contract tests"
  - "flaky tests"
  - "test anti-patterns"
---

# Testing Suite

## Purpose
Maintain unit, integration, contract, and process test suites for governance confidence.

## Trigger Patterns
- "add tests for this change"
- "coverage is not enough"
- "failing test keeps coming back"
- "need reliable regression net"

## Capabilities
- Build layered test evidence: unit for logic, integration for wiring, contract for boundary stability, process for governance.
- Detect and remove anti-patterns: assertion-free tests, over-mocking critical paths, fixture-only closure claims, and non-deterministic sleeps.
- Align tests to release-blocking contracts and acceptance gates, not only code branches.
- Convert vague "tested locally" statements into executable and reproducible evidence.

## Workflow
1. Map change surface to minimum evidence class (unit/integration/contract/process).
2. Define expected behavior, failure mode, and regression risk before writing tests.
3. Add or update targeted tests closest to the changed boundary.
4. Remove flaky assumptions (time races, unordered assertions, hidden global state).
5. Execute the relevant lane commands and record exact rerun command set.

## Integration
- Pair with `phase-acceptance-governor` for pass/block verdict quality.
- Pair with `verification-before-completion` to ensure completion claims have runnable proof.
- Pair with `qa-test-engineer` for broader scenario coverage and acceptance matrices.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`

## Boundaries

This skill should NOT:
- mark a task as verified when only smoke/manual checks were run for contract-sensitive changes.
- use synthetic fixtures as sole proof for real-boundary closure when stronger evidence is required.
