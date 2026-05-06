---
title: Project Command Center
type: project-state-dashboard
status: active
tags:
  - ta3000/project-map
  - ta3000/state-graph
---

# Project Command Center

Use this as the secondary filtered/checklist view. For visual orientation, start
from Project Graph Lens and open the Obsidian local graph.

This is not an architecture diagram. It is the operational view: what needs
attention, where to put new ideas, and which area to inspect next.

## Now

| Lane | Open |
| --- | --- |
| Needs attention | [[project-attention.base|Project Attention]] |
| Triage inbox | [[project-items.base|Project Items]] |
| Area status | [[project-nodes.base|Project Nodes]] |
| Items grouped by area | [[items-by-node.base|Items By Node]] |
| Update rules | [[project-governance.base|Project Governance]] |

## Areas

| Area | State | Use when |
| --- | --- | --- |
| [[Delivery Shell]] | ok | Process, context, gates, PR evidence |
| [[Product Plane]] | attention | Broad product-plane status before going deeper |
| [[Data Plane]] | attention | MOEX raw/canonical storage and research handoff |
| [[Research Plane]] | attention | Research tables, strategy registry, backtests, rankings |
| [[Runtime Plane]] | unknown | Runtime signal lifecycle and durable state |
| [[Contract Surfaces]] | ok | Schemas, fixtures, release-blocking inventory |
| [[Execution Plane]] | attention | Paper/sidecar/broker boundary and reconciliation |
| [[Project Map]] | attention | This Obsidian state/DFD layer |

## Capability Depth

Start from level-1 areas, then open level-2 contours when a question needs
more detail.

| Parent | Level-2 contours |
| --- | --- |
| [[Delivery Shell]] | [[Agent Entry And Routing]], [[Delivery Gates]], [[Durable Work State]], [[Validation Reports]] |
| [[Data Plane]] | [[MOEX Historical Route]], [[Raw Data Ingest]], [[Canonical Data Refresh]], [[Baseline Storage]] |
| [[Research Plane]] | [[Research Data Prep]], [[Strategy Registry]], [[Backtest And Ranking]], [[Signal Candidate Projection]] |
| [[Runtime Plane]] | [[Signal Lifecycle]], [[Durable Signal Store]], [[Runtime API]], [[Live Decision Boundary]] |
| [[Execution Plane]] | [[Paper Execution]], [[Sidecar Wire Boundary]], [[Broker Process Closure]] |
| [[Contract Surfaces]] | [[Release Blocking Contracts]], [[Contract Change Policy]] |

## Add Something

Create a note in `docs/project-map/state/items/`.

Use this minimal frontmatter:

```yaml
---
title: Short Title
type: project-item
item_id: short-id
item_type: idea
linked_node: project-map
status: inbox
severity: low
needs_user_attention: false
owner: user-or-agent
created: 2026-05-05
tags:
  - ta3000/project-item
  - item/idea
---
```

## State Rule

- Node cards are the stable area records.
- Level-2 nodes are capability contours, not source-file inventory.
- Item cards are the work queue.
- If a node has open high-severity items, treat it as needing attention even if
  its node card has not yet been manually updated.
- Full map update rules live in [[Project Map Update Rules]].

## Optional Visual Sketch

[[project-state.canvas|Project State Canvas]] is
kept only as an optional area sketch. Do not use it as the main view.
