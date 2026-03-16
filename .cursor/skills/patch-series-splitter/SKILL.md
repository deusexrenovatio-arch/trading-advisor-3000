---
name: patch-series-splitter
description: Split large diffs into ordered, reviewable patch sets by risk and responsibility.
---

# Patch Series Splitter

## Goal
Convert large mixed changes into focused patch sets with predictable review flow.

## Workflow
1. Group files by responsibility.
2. Apply ordered sequence:
   - contracts/policy
   - runtime/code
   - tests/docs
3. Validate each patch set before moving to the next.

## Governance Baseline
1. Run `python scripts/run_loop_gate.py --from-git --git-ref HEAD` after each patch set.
2. Stop expansion after repeated failures and execute remediation runbook.
