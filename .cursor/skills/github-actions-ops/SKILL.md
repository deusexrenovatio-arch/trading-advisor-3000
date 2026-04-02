---
name: github-actions-ops
description: Design, debug, and harden GitHub Actions workflows used by governed CI lanes.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-OPS
scope: github actions diagnostics and lane reliability
routing_triggers:
  - "github actions"
  - "ci check failed"
  - "workflow yaml"
  - "actions debug"
---

# GitHub Actions Ops

## Purpose
Keep GitHub Actions workflows deterministic, debuggable, and aligned with lane governance contracts.

## Trigger Patterns
- ".github/workflows change"
- "check failed in CI"
- "job hangs or flakes"
- "matrix or cache regression"

## Capabilities
- Triage failing workflow runs by separating infrastructure noise from contract or code regressions.
- Improve workflow reliability with explicit concurrency, cache strategy, retries, and artifact diagnostics.
- Align workflow triggers and required checks with merge policy (`loop-lane`, `pr-lane`).
- Keep workflow YAML readable, reviewable, and diff-safe for policy audits.

## Workflow
1. Classify failure source: config, environment, dependency drift, or real code regression.
2. Reproduce lane command locally where possible to validate root cause.
3. Apply minimal workflow patch with explicit rationale and rollback-safe behavior.
4. Validate trigger semantics (`pull_request`, `push`, `schedule`, `workflow_dispatch`) and required outputs.
5. Re-run relevant gates and publish a concise failure-to-fix evidence summary.

## Integration
- Co-load with `ci-bootstrap` for lane-level architecture and merge-gate wiring.
- Co-load with `commit-and-pr-hygiene` when workflow changes need isolated, reviewable commits.
- Co-load with `verification-before-completion` before marking CI incidents as resolved.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`

## Boundaries
This skill should NOT:
- disable required checks or lower branch protection to hide unresolved failures.
- store secrets or sensitive tokens inside workflow YAML or logs.
