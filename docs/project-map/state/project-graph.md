---
title: Project Graph Lens
type: project-state-graph
status: active
aliases:
  - Project Graph
  - Project State Graph
tags:
  - ta3000/project-map
  - ta3000/state-graph
  - graph/root
---

# Project Graph Lens

Open the Obsidian **Graph view** and use the filter below.

This is the graph-first project view. It is intentionally not a table of
contents: too many direct links from this note turn the graph into a hub.

Do not use Local graph from this note for the main map: this note is only the
setup note, and Local graph will turn it into a fake hub. The real graph is the
filtered global graph over tagged project nodes and items.

## Graph View Setup

Use these settings in Obsidian Graph view:

- Search files: `tag:#ta3000/project-graph -tag:#level/2`
- Turn arrows on.
- Turn attachments off.
- Keep tags hidden unless tag nodes help orientation.
- Keep the force layout wide enough that labels do not overlap.

Current color logic:

| Graph signal | Meaning |
| --- | --- |
| Red | Needs user attention |
| Orange | Open project item |
| Blue | Watch/inbox project item |
| Purple | Map governance/rule note |
| Dark blue | Level-1 architecture/project area |
| Steel | Level-2 capability contour |

State fields still live on the cards and in Bases, but the graph prioritizes
object type first. This keeps architecture separate from open questions.

## Depth Contract

- Level 1 shows the stable map areas.
- Level 2 shows capability contours inside those areas.
- Level 3 is allowed only when a level-2 contour is too dense to explain with
  one node and a few durable links.

Every new node must follow [[Project Map Update Rules]].

## DFD Alignment

The native graph is not the DFD. It should echo DFD levels visually:

- level-1 nodes map to Level 1 DFD areas;
- level-2 nodes map to Level 2 DFD process/data-flow maps;
- source/proof traceability lives in node properties and Bases, not as extra
  visual edges.

## Useful Filters

Use the full graph first, then narrow it when it gets visually noisy.

| View | Search |
| --- | --- |
| Overview | `tag:#ta3000/project-graph -tag:#level/2` |
| Full map | `tag:#ta3000/project-graph` |
| Level 1 architecture | `tag:#ta3000/project-node tag:#level/1` |
| Capability layer | `tag:#ta3000/project-node tag:#level/2` |
| Attention surface | `tag:#ta3000/project-graph tag:#state/attention` |

## Convenience

Open [[Project Map Cockpit]] from the Bookmarks sidebar. It is the normal entry
point; this note is only the graph configuration reference.

## Other Graph Engines To Try

Use native Graph view first because it is already enabled. If it is too tangled,
test other graph engines only in a throwaway vault first:

- ExcaliBrain: more structured parent/child/friend graph navigation, but it needs Dataview and Excalidraw.
- Breadcrumbs: useful if we decide to encode explicit hierarchy fields instead of relying only on links.
