---
title: Contract Surfaces
type: project-node
node_id: contract-surfaces
surface: product-plane
level: 1
parent_node: product-plane
state: ok
aliases:
- Contract Surfaces
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: CONTRACT_SURFACES.md, contract-change-policy.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/product-plane
- level/1
- state/ok
source_refs:
- docs/architecture/product-plane/CONTRACT_SURFACES.md
- docs/architecture/product-plane/contract-change-policy.md
dfd_refs:
- docs/obsidian/dfd/level-2-contracts-and-execution.md
- docs/obsidian/dfd/level-1-product-plane.md
proof_refs:
- src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml
- tests/product-plane/contracts
---

# Contract Surfaces

Release-blocking schemas, fixtures, contract tests, and compatibility inventory.

## Current Note

DFD points at short contract store names; canonical contract truth stays in the
contract-surfaces architecture document.

## Graph Links

- Product-plane contract boundary.
- Contains [[Release Blocking Contracts]] and
  [[Contract Change Policy]].

## Contract Coverage

Governs payload boundaries used by runtime API, signal candidate projection,
durable signal storage, paper execution, and sidecar wire transport. These are
kept as coverage semantics here, not graph edges.

## Linked Items

Use the Items By Node base view and filter `linked_node` to
`contract-surfaces`.
