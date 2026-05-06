---
title: Signal Lifecycle
type: project-node
node_id: signal-lifecycle
surface: product-plane
level: 2
parent_node: runtime-plane
state: ok
aliases:
- Signal Lifecycle
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: STATUS.md, runtime-lifecycle.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/ok
source_refs:
- docs/architecture/product-plane/STATUS.md
- docs/architecture/product-plane/runtime-lifecycle.md
dfd_refs:
- docs/obsidian/dfd/level-2-research-to-runtime.md
proof_refs:
- tests/product-plane/integration/test_runtime_lifecycle.py
- tests/product-plane/unit/test_runtime_components.py
---

# Signal Lifecycle

Candidate replay, active signal state, close/cancel/expire flow, publication
lifecycle, and signal-event history.

## Graph Links

- Consumes [[Signal Candidate Projection]].
- Persists state through [[Durable Signal Store]].
- Publishes outward through [[Runtime API]] and toward [[Execution Plane]].

## Update Rule

Update this node when runtime state transitions, replay behavior, signal event
contracts, or publication lifecycle semantics change.
