---
title: DFD Index
type: dfd-index
status: active
source_of_truth: false
change_surface: mixed
tags:
  - ta3000/dfd
  - ta3000/architecture
  - ta3000/obsidian
canonical_sources:
  - trading-advisor-3000.md
  - repository-surfaces.md
  - STATUS.md
  - CONTRACT_SURFACES.md
  - DEV_WORKFLOW.md
---

# DFD Index

This folder replaces the previous ad-hoc Obsidian flowchart views.

These notes use DFD semantics:

| DFD element | Mermaid shape used here | Meaning |
| --- | --- | --- |
| External entity | `E["Entity"]` | Actor or system outside the process boundary |
| Process | `P(["Process"])` | Transformation, validation, orchestration, or decision process |
| Data store | `D[("Store")]` | Durable state, table, registry, artifact, or evidence store |
| Data flow | labelled arrow | Named payload, evidence, request, state, or publication flow |

DFD maps are navigation artifacts. Canonical architecture and implementation
truth stays in the linked source documents.

## Maps

1. [Level 0 - System Context](docs/obsidian/dfd/level-0-system-context.md)
2. [Level 1 - Delivery Shell](docs/obsidian/dfd/level-1-delivery-shell.md)
3. [Level 1 - Product Plane](docs/obsidian/dfd/level-1-product-plane.md)
4. [Level 2 - Shell Context And Task Lifecycle](docs/obsidian/dfd/level-2-shell-context-and-task-lifecycle.md)
5. [Level 2 - Shell Gate Runtime](docs/obsidian/dfd/level-2-shell-gate-runtime.md)
6. [Level 2 - Shell PR And Evidence](docs/obsidian/dfd/level-2-shell-pr-and-evidence.md)
7. [Level 2 - Data Plane](docs/obsidian/dfd/level-2-data-plane.md)
8. [Level 2 - Research To Runtime](docs/obsidian/dfd/level-2-research-to-runtime.md)
9. [Level 2 - Contracts And Execution](docs/obsidian/dfd/level-2-contracts-and-execution.md)

## Rules

- Keep each DFD level about data movement and ownership, not general prose.
- Name flows on arrows.
- Keep process numbers stable enough for discussion.
- Do not show a data store unless it maps to a real repo path, runtime data
  root, Delta table/folder, database-backed store, or evidence artifact.
- Keep diagram labels short: use file, table, store, or artifact names. Full
  paths belong in canonical source docs, not on the DFD canvas.
- Keep Level 0 boring. Internal stores belong in Level 1 or Level 2.
- When a DFD and a canonical source disagree, the canonical source wins.
- Do not use this folder for the future project state graph.
