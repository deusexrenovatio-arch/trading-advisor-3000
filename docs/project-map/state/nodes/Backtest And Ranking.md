---
title: Backtest And Ranking
type: project-node
node_id: backtest-and-ranking
surface: product-plane
level: 2
parent_node: research-plane
state: attention
aliases:
- Backtest And Ranking
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
- tests/product-plane/integration/test_research_vectorbt_backtests.py
- tests/product-plane/unit/test_research_backtest_layer.py
- tests/product-plane/unit/test_research_ranking_projection.py
---

# Backtest And Ranking

vectorbt-owned portfolio simulation, persisted backtest runs, strategy stats,
trades, orders, drawdowns, rankings, and findings.

## Graph Links

- Uses [[Research Data Prep]] and [[Strategy Registry]].
- Sends selected outputs to [[Signal Candidate Projection]].

## Update Rule

Update this node when backtest runtime ownership, ranking policy, stored output
tables, or benchmark evidence changes.
