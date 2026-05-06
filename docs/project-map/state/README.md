---
title: Project State Graph
type: project-state-index
status: active
tags:
  - ta3000/project-map
  - ta3000/state-graph
---

# Project State Graph

Use this folder to track project areas and attention items. The primary readable
visual entrypoint is the generated HTML cockpit.

## Model

| Card | Folder | Purpose |
| --- | --- | --- |
| Project node | `nodes/` | Stable area of the project |
| Project item | `items/` | Idea, problem, risk, question, or task linked to a node |
| Candidate report | `candidates/` | Advisory inbox for memory-recovered signals |
| Project rule | root note | Governance for map updates |
| HTML cockpit | `../project-cockpit.html` | Main readable visual projection |
| Graph lens | Project Graph Lens | Secondary relationship radar |
| Dashboard | Project Command Center | Secondary filtered/checklist view |
| Bases | `bases/` | Filtered work views over nodes and items |
| Canvas | `canvases/` | Optional area sketch, not the main view |

## State Values

| State | Meaning |
| --- | --- |
| `ok` | No known attention needed |
| `attention` | Needs review, decision, or follow-up |
| `blocked` | Cannot progress without external action or missing proof |
| `unknown` | Not verified enough to claim status |

## Item Status Values

| Status | Meaning |
| --- | --- |
| `inbox` | Captured, not triaged |
| `open` | Accepted and active |
| `watch` | Known, not urgent |
| `done` | Resolved |
| `dropped` | Intentionally closed without action |

## MVP Operating Rule

For now this is hybrid:

- A node can have manual `state`.
- A node has `level`, `parent_node`, `update_rule`, and `state_source`.
- Items carry `needs_user_attention`, `severity`, and `status`.
- Items also carry `priority`: `p0` blocks current progress, `p1` needs a
  decision, `p2` needs review, and `p3` is watch/backlog.
- Items carry `origin_kind` and `origin_refs`; generated items normally come
  from current node state through `scripts/sync_project_map_items.py`.
- Legacy task-note blockers from `docs/tasks/active` are excluded by default
  because main no longer contains authoritative merged task state.
- Historical generated task-blocker items are archived under
  `docs/archive/historical-project-map-items/`; they are not part of the active
  map queue.
- Memory-recovered candidates stay under `candidates/` until a human or agent
  verifies them against current repo, artifacts, or an explicit decision.
- If a high-severity open item is linked to a node, treat that node as needing
  attention even if the node card has not been manually updated yet.
- The generated cockpit computes large-block roll-up status from node state,
  child state, active item priority, and user-attention flags.
- Map structure follows [[Project Map Update Rules]].
- Prototype seed items should stay `dropped` once generated signals replace
  them.

## Views

- [Generated HTML Cockpit](../project-cockpit.html)
- Project Graph Lens
- [[dashboard|Project Command Center]]
- [[project-attention.base|Project Attention]]
- [[project-nodes.base|Project Nodes]]
- [[project-items.base|Project Items]]
- [[items-by-node.base|Items By Node]]
- [[project-governance.base|Project Governance]]
- [[project-state.canvas|Project State Canvas]] - optional sketch
