---
name: architecture-review
description: Review module boundaries, dependencies, and architecture contracts for regressions.
---

# Architecture Review

## Goal
Keep architecture boundaries explicit and enforceable.

## Workflow
1. Check boundary assumptions and intended dependency direction.
2. Validate architecture docs and tests remain aligned.
3. Flag coupling risks and missing guards.

## Governance Baseline
1. Run `python -m pytest tests/architecture -q`.
2. Run `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
