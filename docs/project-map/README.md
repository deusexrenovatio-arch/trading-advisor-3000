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
- `docs/project-map/product-reality-audit-data-to-signal-2026-05-06.md` - current Data Plane -> Research Factory -> Signal-to-Action audit.
- `docs/project-map/documentation-currentness-map-2026-05-06.md` - classifies project documentation by currentness.
- `docs/project-map/documentation-cleanup-candidates-2026-05-06.md` - working list for archive/rewrite decisions.
- `docs/project-map/product-reset-audit-2026-05-05.md` - reset-frame product audit; use the 2026-05-06 data-to-signal audit for fresher Optuna and candidate-projection state.
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
- `state/readiness-scores.yaml` - numeric readiness estimates rendered into the cockpit.
- `project-cockpit.html` - generated browser cockpit over the same notes.
- Project Graph Lens - graph-first Obsidian entrypoint.
- `state/bases/*.base` - secondary filtered views over nodes and items.
- `state/canvases/*.canvas` - optional projections over the same notes.
- `scripts/validate_project_map.py` - validation that nodes have parent,
  DFD, source, and proof references.
- `scripts/build_project_cockpit.py` - deterministic HTML cockpit generator.
- `scripts/add_project_map_item.py` - item intake helper for new open questions.
- `scripts/sync_project_map_items.py` - generated item sync from current node
  state. Legacy task-note blockers can be imported only with an explicit flag.

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
