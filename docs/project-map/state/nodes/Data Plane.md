---
title: Data Plane
type: project-node
node_id: data-plane
surface: product-plane
level: 1
parent_node: product-plane
state: attention
aliases:
- Data Plane
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: STATUS.md, MOEX route decision, storage runbook
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
- docs/architecture/product-plane/moex-historical-route-decision.md
- docs/architecture/product-plane/moex-nightly-v2-architecture.md
- docs/runbooks/app/moex-baseline-storage-runbook.md
dfd_refs:
- docs/obsidian/dfd/level-2-data-plane.md
- docs/obsidian/dfd/level-1-product-plane.md
proof_refs:
- scripts/run_moex_raw_ingest.py
- scripts/run_moex_canonical_refresh.py
- tests/product-plane/unit/test_moex_canonical_route.py
---

# Data Plane

Historical market ingestion, MOEX raw/canonical baseline, QC, and research
handoff.

## Current Note

DFD now uses short store names and keeps full paths in canonical status/runbook
docs.

## Graph Links

- Contains [[MOEX Historical Route]],
  [[Raw Data Ingest]],
  [[Canonical Data Refresh]], and
  [[Baseline Storage]].
- Feeds [[Research Plane]] through persisted research handoff data.
- Hands canonical data to [[Research Data Prep]].

## Linked Items

Use the Items By Node base view and filter `linked_node` to `data-plane`.
