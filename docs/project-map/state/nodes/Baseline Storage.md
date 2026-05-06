---
title: Baseline Storage
type: project-node
node_id: baseline-storage
surface: product-plane
level: 2
parent_node: data-plane
state: ok
aliases:
- Baseline Storage
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: STATUS.md, moex-baseline-storage-runbook.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/ok
source_refs:
- docs/architecture/product-plane/STATUS.md
- docs/runbooks/app/moex-baseline-storage-runbook.md
dfd_refs:
- docs/obsidian/dfd/level-2-data-plane.md
proof_refs:
- scripts/pin_moex_baseline_storage.ps1
- tests/product-plane/unit/test_moex_t3_storage_binding.py
- tests/product-plane/unit/test_moex_baseline_update.py
---

# Baseline Storage

Accepted raw/canonical baseline storage under the external TA3000 data root.
Downstream consumers bind to stable baseline paths, not arbitrary rerun folders.

## Graph Links

- Receives outputs from [[Canonical Data Refresh]].
- Supplies canonical inputs to [[Research Data Prep]].

## Update Rule

Update this node when accepted data roots, pinned baseline run, or downstream
binding policy changes.
