---
title: Durable Signal Store
type: project-node
node_id: durable-signal-store
surface: product-plane
level: 2
parent_node: runtime-plane
state: ok
aliases:
- Durable Signal Store
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: STATUS.md, CONTRACT_SURFACES.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/ok
source_refs:
- docs/architecture/product-plane/STATUS.md
- docs/architecture/product-plane/CONTRACT_SURFACES.md
dfd_refs:
- docs/obsidian/dfd/level-2-research-to-runtime.md
- docs/obsidian/dfd/level-2-contracts-and-execution.md
proof_refs:
- tests/product-plane/integration/test_runtime_postgres_store.py
- tests/product-plane/unit/test_durable_runtime_bootstrap.py
---

# Durable Signal Store

Durable runtime signal, event, publication, and migration-tracking boundary.
Production-like profiles block non-durable fallbacks.

## Graph Links

- Stores [[Signal Lifecycle]] state.

## Contract Coverage

Must satisfy release-blocking persistence and signal/event/publication
contracts.

## Update Rule

Update this node when runtime persistence backend, migration boundary, or
durable fallback policy changes.
