---
title: Execution Plane
type: project-node
node_id: execution-plane
surface: product-plane
level: 1
parent_node: product-plane
state: attention
aliases:
- Execution Plane
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: execution-flow.md, controlled-live-execution.md, STATUS.md
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
- docs/architecture/product-plane/execution-flow.md
- docs/architecture/product-plane/controlled-live-execution.md
- docs/architecture/product-plane/sidecar-wire-api-v1.md
dfd_refs:
- docs/obsidian/dfd/level-2-contracts-and-execution.md
- docs/obsidian/dfd/level-1-product-plane.md
proof_refs:
- tests/product-plane/integration/test_execution_flow.py
- tests/product-plane/integration/test_real_execution_staging_rollout.py
---

# Execution Plane

Paper execution, sidecar boundary, broker events, reconciliation, and
position/risk snapshots.

## Current Note

The current DFD intentionally does not invent a durable execution DB.

## Graph Links

- Contains [[Paper Execution]],
  [[Sidecar Wire Boundary]], and
  [[Broker Process Closure]].
- Receives runtime decisions from the runtime plane.

## Linked Items

Use the Items By Node base view and filter `linked_node` to `execution-plane`.
