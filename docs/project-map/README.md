---
title: Project Map
type: project-map-index
status: active
tags:
  - ta3000/project-map
---

# Project Map

This folder contains the project state graph MVP.

## Current Truth Entry Points

For the current TA3000 reset, read these before old task notes, package-intake
artifacts, or target-shape specs:

- `docs/project-map/current-truth-map-2026-05-05.md` - separates current truth from historical evidence.
- `docs/project-map/documentation-currentness-map-2026-05-06.md` - classifies project documentation by currentness.
- `docs/project-map/documentation-cleanup-candidates-2026-05-06.md` - working list for archive/rewrite decisions.
- `docs/project-map/product-reset-audit-2026-05-05.md` - product audit for Research Factory -> Signal-to-Action.
- `docs/project-map/documentation-reality-audit-2026-05-05.md` - documentation noise audit and cleanup policy.
- `docs/project-map/status-inventory-2026-05-05.md` - code/worktree/data snapshot.
- `docs/project-map/worktree-diff-digest-2026-05-05.md` - recovery triage for local worktrees.

It is separate from DFD:

- DFD maps data movement.
- Project state maps where attention is needed and which capability nodes
  correspond to the DFD levels.

## Source Of Truth

- `state/nodes/*.md` - project areas.
- `state/items/*.md` - ideas, problems, risks, questions, and follow-up tasks.
- `project-cockpit.html` - generated browser cockpit over the same notes.
- `docs/project-map/state/candidates/project-map-candidates.md` - generated
  candidate inbox from advisory memory recall; not active project truth.
- Project Graph Lens - graph-first Obsidian entrypoint.
- `state/bases/*.base` - secondary filtered views over nodes and items.
- `state/canvases/*.canvas` - optional projections over the same notes.
- `scripts/validate_project_map.py` - validation that nodes have parent,
  DFD, source, and proof references.
- `scripts/build_project_cockpit.py` - deterministic HTML cockpit generator.
- `scripts/add_project_map_item.py` - item intake helper for new open questions.
- `scripts/sync_project_map_items.py` - generated item sync from current node
  state. Legacy task-note blockers can be imported only with an explicit flag.
- `scripts/collect_project_map_candidates.py` - advisory MemPalace candidate
  collector for lost or stale problem signals.

## Primary View

Open Project Map Cockpit from the Bookmarks sidebar.

The primary visual page is the generated HTML cockpit. Project Map Overview is
the compact DFD-like orientation page. Global Graph is intentionally secondary:
use it as a relationship radar, not as the architecture diagram.

Markdown notes are the source of truth. The browser cockpit, Bases, Mermaid
overview, and Graph view are projections over those notes; none of them should
be edited as the source of truth. Canvas is optional and should not become the
source of truth.

The graph should not invent architecture. Each project node must point to the
DFD page, source documents, and proof surfaces that justify it.

Project item priority is separate from severity. Use priority for operating
order (`p0` now, `p1` decision, `p2` review, `p3` watch) and severity for impact.
Each item must also declare `origin_kind` and `origin_refs`, so the cockpit does
not show unexplained problems.

`docs/tasks/active` is not a current source of truth for this map. Those notes
were often created inside separate worktrees and were not reliably merged, so
they are treated as legacy diagnostic material rather than live project state.

The candidate report has the same boundary: it helps recover possible lost
signals from memory, but it must be reviewed before anything is promoted to an
active project item.
