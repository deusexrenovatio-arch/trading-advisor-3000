# Obsidian Project Maps

This folder contains Obsidian-friendly project maps for Trading Advisor 3000.

These files are not source-of-truth architecture documents. They are reader-facing
maps over the canonical docs under `docs/architecture/` and the shell workflow
docs under `docs/agent/` and `docs/DEV_WORKFLOW.md`.

## Rules

- Keep source-of-truth claims in the canonical architecture documents.
- Use views for orientation, links, diagrams, and reading paths.
- When a view and a canonical document disagree, the canonical document wins.
- Keep maps small enough to be useful inside an Obsidian graph and backlinks.
- Use DFD for architecture diagrams. Do not mix ad-hoc flowcharts into this
  layer.

## Active DFD Maps

- [DFD Index](docs/obsidian/dfd/README.md)
- [DFD Level 0 - System Context](docs/obsidian/dfd/level-0-system-context.md)
- [DFD Level 1 - Delivery Shell](docs/obsidian/dfd/level-1-delivery-shell.md)
- [DFD Level 1 - Product Plane](docs/obsidian/dfd/level-1-product-plane.md)
- [DFD Level 2 - Shell Context And Task Lifecycle](docs/obsidian/dfd/level-2-shell-context-and-task-lifecycle.md)
- [DFD Level 2 - Shell Gate Runtime](docs/obsidian/dfd/level-2-shell-gate-runtime.md)
- [DFD Level 2 - Shell PR And Evidence](docs/obsidian/dfd/level-2-shell-pr-and-evidence.md)
- [DFD Level 2 - Data Plane](docs/obsidian/dfd/level-2-data-plane.md)
- [DFD Level 2 - Research To Runtime](docs/obsidian/dfd/level-2-research-to-runtime.md)
- [DFD Level 2 - Contracts And Execution](docs/obsidian/dfd/level-2-contracts-and-execution.md)

## Parked

The interactive project state graph is intentionally separate from DFD and will
be designed later.
