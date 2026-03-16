---
name: docs-sync
description: Keep policy, architecture, and workflow documents aligned with code and registry changes.
---

# Docs Sync

## Goal
Prevent drift between scripts, tests, and source-of-truth documentation.

## Workflow
1. Identify changed policy/runtime surfaces.
2. Update affected docs and indexes.
3. Validate markdown links and command references.

## Governance Baseline
1. Run `python scripts/validate_docs_links.py --roots AGENTS.md docs`.
2. Run `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
