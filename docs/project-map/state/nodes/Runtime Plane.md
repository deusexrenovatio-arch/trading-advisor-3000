---
title: Runtime Plane
type: project-node
node_id: runtime-plane
surface: product-plane
level: 1
parent_node: product-plane
state: unknown
aliases:
- Runtime Plane
needs_user_attention: false
confidence: low
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: STATUS.md, runtime-lifecycle.md, runtime API docs
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/1
- state/unknown
source_refs:
- docs/architecture/trading-advisor-3000.md
- docs/architecture/repository-surfaces.md
- docs/architecture/product-plane/STATUS.md
- docs/architecture/product-plane/runtime-lifecycle.md
- docs/architecture/product-plane/CONTRACT_SURFACES.md
dfd_refs:
- docs/obsidian/dfd/level-2-research-to-runtime.md
- docs/obsidian/dfd/level-1-product-plane.md
proof_refs:
- tests/product-plane/integration/test_runtime_lifecycle.py
- tests/product-plane/integration/test_runtime_postgres_store.py
---

# Runtime Plane

Runtime signal lifecycle, durable signal store, publication state, and API
runtime surface.

## Current Note

State graph MVP marks this as `unknown` until the runtime-specific state view is
reviewed against current proof.

## Graph Links

- Contains [[Signal Lifecycle]],
  [[Durable Signal Store]],
  [[Runtime API]], and
  [[Live Decision Boundary]].
- Feeds [[Execution Plane]] only after runtime proof is clear.
- Carries [[Runtime State Unknown]].

## Linked Items

Use the Items By Node base view and filter `linked_node` to `runtime-plane`.
