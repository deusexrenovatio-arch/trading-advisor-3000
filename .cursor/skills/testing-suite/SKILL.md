---
name: testing-suite
description: Build and maintain unit, integration, and process governance tests.
---

# Testing Suite

## Goal
Ensure behavior changes are protected by deterministic tests.

## Workflow
1. Classify required test level (unit/process/architecture).
2. Add focused tests near changed surfaces.
3. Run targeted tests, then full governance suite.

## Governance Baseline
1. Run `python -m pytest tests/process tests/architecture -q`.
2. Run `python scripts/run_pr_gate.py --from-git --git-ref HEAD`.
