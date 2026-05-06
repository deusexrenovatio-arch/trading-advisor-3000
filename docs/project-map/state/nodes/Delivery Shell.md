---
title: Delivery Shell
type: project-node
node_id: delivery-shell
surface: shell
level: 1
parent_node: null
state: ok
aliases:
- Delivery Shell
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: docs/agent entrypoints, shell policy, validation gates
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/shell
- level/1
- state/ok
source_refs:
- AGENTS.md
- docs/DEV_WORKFLOW.md
- docs/architecture/repository-surfaces.md
- docs/agent/entrypoint.md
- docs/agent/checks.md
- docs/agent/skills-routing.md
dfd_refs:
- docs/obsidian/dfd/level-1-delivery-shell.md
proof_refs:
- scripts/run_loop_gate.py
- scripts/run_pr_gate.py
- scripts/validate_session_handoff.py
---

# Delivery Shell

Control plane for policy, context routing, task lifecycle, gates, PR evidence,
and handoff state.

## Current Note

DFD shape was corrected from a linear workflow to a control-plane model.

## Graph Links

- Contains [[Agent Entry And Routing]],
  [[Delivery Gates]],
  [[Durable Work State]], and
  [[Validation Reports]].
- Controls delivery process around [[Product Plane]] without
  owning product trading logic.
- Maintains [[Project Map]] and architecture orientation surfaces.

## Linked Items

Use the Items By Node base view and filter `linked_node` to `delivery-shell`.
