---
title: Signal Candidate Projection
type: project-node
node_id: signal-candidate-projection
surface: product-plane
level: 2
parent_node: research-plane
state: attention
aliases:
- Signal Candidate Projection
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: research-plane-platform.md, CONTRACT_SURFACES.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/attention
source_refs:
- docs/architecture/product-plane/research-plane-platform.md
- docs/architecture/product-plane/CONTRACT_SURFACES.md
dfd_refs:
- docs/obsidian/dfd/level-2-research-to-runtime.md
proof_refs:
- tests/product-plane/unit/test_research_ranking_projection.py
- tests/product-plane/contracts
---

# Signal Candidate Projection

Contract-safe candidate projection from research outputs toward runtime
consumption. Runtime consumes projected candidates, not research internals.

## Graph Links

- Receives evidence from [[Backtest And Ranking]].
- Feeds [[Signal Lifecycle]].

## Contract Coverage

Must satisfy release-blocking research and signal candidate contracts.

## Update Rule

Update this node when candidate schemas, projection eligibility, promotion
profile, or runtime handoff semantics change.
