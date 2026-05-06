---
title: MOEX Historical Route
type: project-node
node_id: moex-historical-route
surface: product-plane
level: 2
parent_node: data-plane
state: attention
aliases:
- MOEX Historical Route
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: moex-historical-route-decision.md, STATUS.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/attention
source_refs:
- docs/architecture/product-plane/moex-historical-route-decision.md
- docs/architecture/product-plane/moex-nightly-v2-architecture.md
- docs/architecture/product-plane/STATUS.md
dfd_refs:
- docs/obsidian/dfd/level-2-data-plane.md
proof_refs:
- scripts/run_moex_dagster_cutover.py
- scripts/prove_moex_dagster_route.py
- tests/product-plane/unit/test_moex_dagster_route_cutover.py
---

# MOEX Historical Route

The governed historical-data route: Dagster orchestration, Python raw ingest,
Spark canonical refresh, and stable raw/canonical storage.

## Graph Links

- Orchestrates [[Raw Data Ingest]] before [[Canonical Data Refresh]].
- Publishes to [[Baseline Storage]].
- Feeds [[Research Data Prep]] after accepted refresh.

## Update Rule

Update this node when the authoritative MOEX route decision, schedule, writer
ownership, or baseline pin changes.
