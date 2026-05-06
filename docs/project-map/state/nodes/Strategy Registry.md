---
title: Strategy Registry
type: project-node
node_id: strategy-registry
surface: product-plane
level: 2
parent_node: research-plane
state: attention
aliases:
- Strategy Registry
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: research-plane-platform.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/attention
source_refs:
- docs/architecture/product-plane/research-plane-platform.md
dfd_refs:
- docs/obsidian/dfd/level-2-research-to-runtime.md
proof_refs:
- tests/product-plane/unit/test_research_registry_store.py
- tests/product-plane/unit/test_research_campaign_runner.py
---

# Strategy Registry

Delta-backed strategy families, templates, modules, instances, campaigns, run
indices, rankings, and strategy notes.

## Graph Links

- Consumes reusable outputs from [[Research Data Prep]].
- Defines campaign inventory for [[Backtest And Ranking]].

## Update Rule

Update this node when strategy inventory, adapter count, registry tables, or
campaign routing semantics change.
