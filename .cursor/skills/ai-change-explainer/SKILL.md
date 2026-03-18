---
name: ai-change-explainer
description: Produce explainable change summaries for AI-generated diffs and governance updates.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: change explanation and PR narrative quality
routing_triggers:
  - "change summary"
  - "pr narrative"
  - "diff explanation"
  - "impact report"
---

# Ai Change Explainer

## Purpose
Produce explainable change summaries for AI-generated diffs and governance updates.

## Workflow
1. Confirm request scope and expected output.
2. Apply the skill workflow only to the relevant change surface.
3. Keep changes small, deterministic, and review-friendly.
4. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_skills.py --strict`
