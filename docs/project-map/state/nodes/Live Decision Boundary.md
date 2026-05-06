---
title: Live Decision Boundary
type: project-node
node_id: live-decision-boundary
surface: product-plane
level: 2
parent_node: runtime-plane
state: attention
aliases:
- Live Decision Boundary
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: STATUS.md, controlled-live-execution.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/attention
source_refs:
- docs/architecture/product-plane/STATUS.md
- docs/architecture/product-plane/controlled-live-execution.md
dfd_refs:
- docs/obsidian/dfd/level-2-contracts-and-execution.md
proof_refs:
- tests/product-plane/integration/test_real_execution_staging_rollout.py
- tests/product-plane/unit/test_real_execution_runtime_profile_ops.py
---

# Live Decision Boundary

Strict boundary between historical/batch context and intraday live decisions.
Live decisions require confirmed broker live data through the real execution
boundary.

## Graph Links

- Constrains [[Signal Lifecycle]] before live publication.
- Depends on [[Broker Process Closure]] for real broker confirmation.
- Protects [[Execution Plane]] from historical-data leakage into live action.

## Update Rule

Update this node when live decision policy, broker confirmation rule, or
fail-closed behavior changes.
