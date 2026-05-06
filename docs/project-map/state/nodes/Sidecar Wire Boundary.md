---
title: Sidecar Wire Boundary
type: project-node
node_id: sidecar-wire-boundary
surface: product-plane
level: 2
parent_node: execution-plane
state: ok
aliases:
- Sidecar Wire Boundary
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: sidecar-wire-api-v1.md, CONTRACT_SURFACES.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/ok
source_refs:
- docs/architecture/product-plane/sidecar-wire-api-v1.md
- docs/architecture/product-plane/CONTRACT_SURFACES.md
dfd_refs:
- docs/obsidian/dfd/level-2-contracts-and-execution.md
proof_refs:
- scripts/run_f1d_sidecar_immutable_evidence.py
- tests/product-plane/unit/test_real_execution_http_transport.py
- tests/product-plane/unit/test_dotnet_sidecar_assets.py
---

# Sidecar Wire Boundary

HTTP/JSON wire contract between the Python execution bridge and the StockSharp
sidecar gateway, including submit/cancel/replace, streams, probes, and
kill-switch behavior.

## Graph Links

- Supports [[Broker Process Closure]].

## Contract Coverage

Must satisfy release-blocking sidecar HTTP wire envelopes.

## Update Rule

Update this node when sidecar endpoints, idempotency, readiness, metrics,
kill-switch, or error semantics change.
