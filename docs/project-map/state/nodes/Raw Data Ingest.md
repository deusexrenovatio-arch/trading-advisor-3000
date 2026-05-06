---
title: Raw Data Ingest
type: project-node
node_id: raw-data-ingest
surface: product-plane
level: 2
parent_node: data-plane
state: attention
aliases:
- Raw Data Ingest
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
- docs/architecture/product-plane/historical-data-plane.md
- docs/architecture/product-plane/STATUS.md
dfd_refs:
- docs/obsidian/dfd/level-2-data-plane.md
proof_refs:
- scripts/run_moex_raw_ingest.py
- tests/product-plane/integration/test_moex_raw_ingest.py
- tests/product-plane/unit/test_moex_raw_ingest_errors.py
---

# Raw Data Ingest

Source-aware MOEX extraction and raw Delta mutation boundary. Python owns the
connector behavior, request diagnostics, retries, and raw writer discipline.

## Graph Links

- Runs inside [[MOEX Historical Route]].
- Writes raw history used by [[Canonical Data Refresh]].

## Update Rule

Update this node when raw writer ownership, source diagnostics, raw report
contracts, or raw storage semantics change.
