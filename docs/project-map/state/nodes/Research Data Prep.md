---
title: Research Data Prep
type: project-node
node_id: research-data-prep
surface: product-plane
level: 2
parent_node: research-plane
state: attention
aliases:
- Research Data Prep
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: research-plane-platform.md, native-runtime-ownership.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/attention
source_refs:
- docs/architecture/product-plane/research-plane-platform.md
- docs/architecture/product-plane/native-runtime-ownership.md
dfd_refs:
- docs/obsidian/dfd/level-2-research-to-runtime.md
proof_refs:
- tests/product-plane/integration/test_research_indicator_materialization.py
- tests/product-plane/integration/test_research_dagster_jobs.py
- tests/product-plane/unit/test_research_dataset_layer.py
---

# Research Data Prep

Materialized research datasets, bar views, indicator frames, and derived
indicator frames built after accepted canonical data refresh.

## Graph Links

- Consumes [[Baseline Storage]] and [[Canonical Data Refresh]] output.
- Feeds [[Strategy Registry]] and [[Backtest And Ranking]].

## Update Rule

Update this node when research materialization ownership, Delta table set,
indicator semantics, or data-prep schedule changes.
