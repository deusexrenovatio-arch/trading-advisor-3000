---
title: Research Plane
type: project-node
node_id: research-plane
surface: product-plane
level: 1
parent_node: product-plane
state: attention
aliases:
- Research Plane
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: research-plane-platform.md, STATUS.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/1
- state/attention
source_refs:
- docs/architecture/trading-advisor-3000.md
- docs/architecture/repository-surfaces.md
- docs/architecture/product-plane/STATUS.md
- docs/architecture/product-plane/research-plane-platform.md
- docs/runbooks/app/research-plane-operations.md
dfd_refs:
- docs/obsidian/dfd/level-2-research-to-runtime.md
- docs/obsidian/dfd/level-1-product-plane.md
proof_refs:
- tests/product-plane/integration/test_research_plane.py
- tests/product-plane/integration/test_research_campaign_route.py
---

# Research Plane

Materialized research data prep, strategy registry, backtest outputs, rankings,
findings, and candidate projection.

## Current Note

Research state should be checked through persisted Delta tables and current
status docs, not only route descriptions.

## Graph Links

- Contains [[Research Data Prep]],
  [[Strategy Registry]],
  [[Backtest And Ranking]], and
  [[Signal Candidate Projection]].
- Projects validated outputs toward [[Runtime Plane]].

## Linked Items

Use the Items By Node base view and filter `linked_node` to `research-plane`.
