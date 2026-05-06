---
title: Release Blocking Contracts
type: project-node
node_id: release-blocking-contracts
surface: product-plane
level: 2
parent_node: contract-surfaces
state: ok
aliases:
- Release Blocking Contracts
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: CONTRACT_SURFACES.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/2
- state/ok
source_refs:
- docs/architecture/product-plane/CONTRACT_SURFACES.md
- src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml
dfd_refs:
- docs/obsidian/dfd/level-2-contracts-and-execution.md
proof_refs:
- tests/product-plane/contracts
- src/trading_advisor_3000/product_plane/contracts/schemas
---

# Release Blocking Contracts

Versioned schemas, fixtures, tests, inventory, and compatibility classes that
block release-sensitive payload drift.

## Graph Links

- Applies through [[Contract Change Policy]].

## Contract Coverage

Governs signal candidate projection, runtime API envelopes, durable signal
store payloads, paper execution DTOs, and sidecar wire envelopes. These
coverage relationships are intentionally not rendered as graph edges.

## Update Rule

Update this node when contract inventory, schema set, fixture requirements, or
compatibility class rules change.
