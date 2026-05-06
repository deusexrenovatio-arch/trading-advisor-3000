---
title: Product Plane
type: project-node
node_id: product-plane
surface: product-plane
level: 1
parent_node: null
state: attention
aliases:
- Product Plane
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: docs/architecture/product-plane/STATUS.md
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
- docs/architecture/product-plane/native-runtime-ownership.md
- docs/architecture/product-plane/CONTRACT_SURFACES.md
dfd_refs:
- docs/obsidian/dfd/level-1-product-plane.md
proof_refs:
- scripts/validate_stack_conformance.py
- scripts/validate_product_surface_naming.py
- tests/product-plane
---

# Product Plane

Application plane for contracts, data, research, runtime, execution, app docs,
and product tests.

## Current Note

Status is broad and should be read through child nodes before making delivery
claims.

## Graph Links

- Starts at [[Data Plane]] for the product data flow.
- Continues through [[Research Plane]], [[Runtime Plane]], and
  [[Execution Plane]].
- Holds [[Contract Surfaces]] as the product contract boundary.
- Is governed externally by the delivery-shell process.

## Linked Items

Use the Items By Node base view and filter `linked_node` to `product-plane`.
