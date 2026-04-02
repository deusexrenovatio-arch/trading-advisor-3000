---
name: ci-bootstrap
description: Bootstrap CI lanes and merge gates aligned with repository governance.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-OPS
scope: ci lane setup and gate wiring
routing_triggers:
  - "ci"
  - "pipeline"
  - "merge gate"
  - "workflow"
  - "hosted runners"
  - "github actions"
  - "failing check"
---

# Ci Bootstrap

## Purpose
Bootstrap CI lanes and merge gates aligned with repository governance.

## Trigger Patterns
- "build failed in actions"
- "required check is red"
- "need PR gate"
- "workflow matrix"
- "cache regression in CI"

## Capabilities
- Define lane topology (`loop`, `pr`, `nightly`, `dashboard`) with deterministic ownership.
- Keep merge requirements strict while avoiding false-red noise from infrastructure limits.
- Add reliable GitHub Actions patterns: checkout depth, matrix strategy, cache keys, concurrency, and artifact retention.
- Ensure CI changes are traceable to change-surface contracts and governance checks.

## Workflow
1. Identify target lane and required acceptance effect (loop feedback, PR confidence, nightly hygiene, report refresh).
2. Verify required checks and branch-protection expectations before changing workflow logic.
3. Apply minimal workflow edits with explicit fail conditions and deterministic command ordering.
4. Map each job to owned validation commands from `docs/agent/checks.md`.
5. Run local lane-equivalent commands and capture residual risk if hosted CI is unavailable.
6. Update docs/routing only when skill-gating behavior or lane policy changed.

## Integration
- Use with `github-actions-ops` for failing-check triage and run-level diagnostics.
- Use with `commit-and-pr-hygiene` when lane changes must be split into reviewable PR slices.
- Use with `verification-before-completion` before claiming CI hardening is complete.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`

## Boundaries

This skill should NOT:
- weaken required merge checks to pass transient failures silently.
- treat green infrastructure status as proof of feature or contract correctness.
