---
name: commit-and-pr-hygiene
description: Keep commit series and pull requests atomic, reviewable, and policy-compliant.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-OPS
scope: commit hygiene and pull request structure
routing_triggers:
  - "commit hygiene"
  - "pr hygiene"
  - "atomic changes"
  - "reviewability"
  - "pr checklist"
  - "split commits"
---

# Commit And Pr Hygiene

## Purpose
Keep commit series and pull requests atomic, reviewable, and policy-compliant.

## Trigger Patterns
- "PR is too large"
- "history is messy"
- "review is blocked"
- "need checklist before merge"

## Capabilities
- Split mixed diffs into reviewable units (`contracts -> code -> docs`) for high-risk contours.
- Enforce explicit PR narrative: what changed, why, risk profile, and verification evidence.
- Apply a practical PR quality checklist so no hidden assumptions/fallbacks are merged.
- Keep commit intent aligned with ownership surfaces and gate routing.

## Workflow
1. Classify changed files by ownership context and identify mixed-risk segments.
2. Split commits by concept and validation boundary, not by file count.
3. Ensure each commit message states objective, scope, and validation impact.
4. Prepare PR checklist:
   - contract impact explicit
   - tests executed and relevant
   - docs updated for operator flow
   - unresolved assumptions listed or removed
5. Verify gate commands on the resulting patch set before merge handoff.

## Integration
- Pair with `phase-acceptance-governor` when PR closure implies phase acceptance.
- Pair with `testing-suite` to prevent "tests listed but not executed" regressions.
- Pair with `ci-bootstrap` when PR changes affect lane or required checks.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`

## Boundaries

This skill should NOT:
- combine unrelated high-risk changes into one commit to "save time".
- hide acceptance blockers under soft language like "follow-up later" in PR closeout.
