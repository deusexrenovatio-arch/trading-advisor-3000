---
title: Canonical Data Refresh
type: project-node
node_id: canonical-data-refresh
surface: product-plane
level: 2
parent_node: data-plane
state: attention
aliases:
- Canonical Data Refresh
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: moex-historical-route-decision.md, native-runtime-ownership.md, STATUS.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/attention
source_refs:
- docs/architecture/product-plane/moex-historical-route-decision.md
- docs/architecture/product-plane/native-runtime-ownership.md
- docs/architecture/product-plane/STATUS.md
dfd_refs:
- docs/obsidian/dfd/level-2-data-plane.md
proof_refs:
- scripts/run_moex_canonical_refresh.py
- scripts/run_moex_historical_canonical_route_spark.py
- tests/product-plane/unit/test_moex_canonical_route.py
---

# Canonical Data Refresh

Spark-owned canonical recompute, resampling, provenance rebuild, and canonical
Delta output for downstream research.

## Graph Links

- Consumes [[Raw Data Ingest]] output.
- Writes accepted outputs into [[Baseline Storage]].
- Feeds [[Research Data Prep]].

## Update Rule

Update this node when canonical ownership, runtime owner, refresh scope,
provenance, or acceptance proof changes.
