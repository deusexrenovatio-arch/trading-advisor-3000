---
title: Project Map Cockpit
type: project-map-cockpit
status: active
tags:
  - ta3000/project-map
  - ta3000/state-graph
---

# Project Map Cockpit

Use this as the project-map start page.

## Start Here

| Need | Open |
| --- | --- |
| Browser cockpit | [Generated HTML Cockpit](../project-cockpit.html) |
| Visual overview | [[Project Map Overview]] |
| Relationship radar | [[project-graph|Project Graph Lens]] |
| Current attention | [[project-attention.base|Project Attention]] |
| DFD/source/proof alignment | [[project-governance.base|Project Governance]] |
| Process/data-flow diagrams | [DFD Index](docs/obsidian/dfd/README.md) |
| Operating dashboard | [[dashboard|Project Command Center]] |

## Graph View

The generated HTML cockpit is the main readable view. Global Graph is secondary:
use it when you need a relationship radar, not as the primary architecture
picture.

The least noisy Global Graph filter is:

```text
tag:#ta3000/project-graph -tag:#level/2
```

This keeps level-1 architecture and active items visible without loading the
full capability layer.

## Drill-Down Filters

| View | Filter |
| --- | --- |
| Full project graph | `tag:#ta3000/project-graph` |
| Level 1 architecture | `tag:#ta3000/project-node tag:#level/1` |
| Level 2 capability layer | `tag:#ta3000/project-node tag:#level/2` |
| Attention surface | `tag:#ta3000/project-graph tag:#state/attention` |

## Embedded Views

![[project-attention.base]]

![[project-governance.base#DFD Alignment]]

## Item Intake

Use `scripts/add_project_map_item.py` for new problems, questions, risks,
ideas, or tasks. It creates the markdown item and binds it to an existing
`node_id`.

Use `--priority p0|p1|p2|p3` to separate urgent decisions from ordinary open
questions. The HTML cockpit uses priority to compute large-block roll-up state.

Run `scripts/sync_project_map_items.py` before rebuilding the cockpit to refresh
generated signals from current node state. Legacy task notes are excluded from
the normal cockpit queue.
