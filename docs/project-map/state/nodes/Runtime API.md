---
title: Runtime API
type: project-node
node_id: runtime-api
surface: product-plane
level: 2
parent_node: runtime-plane
state: ok
aliases:
- Runtime API
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
- tests/product-plane/unit/test_runtime_api_smoke.py
- tests/product-plane/contracts
---

# Runtime API

FastAPI ASGI runtime surface for health, readiness, replay, close/cancel, and
operator-facing runtime projections.

## Graph Links

- Exposes [[Signal Lifecycle]] state.

## Contract Coverage

Must satisfy release-blocking runtime API envelopes and does not bypass the
live decision boundary.

## Update Rule

Update this node when runtime API envelopes, readiness behavior, or operator
runtime projections change.
