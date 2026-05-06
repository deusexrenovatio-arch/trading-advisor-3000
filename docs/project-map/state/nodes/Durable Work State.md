---
title: Durable Work State
type: project-node
node_id: durable-work-state
surface: shell
level: 2
parent_node: delivery-shell
state: ok
aliases:
- Durable Work State
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: docs/session_handoff.md, plans/items, task notes
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/shell
- level/2
- state/ok
source_refs:
- docs/session_handoff.md
- docs/DEV_WORKFLOW.md
- docs/agent/runtime.md
dfd_refs:
- docs/obsidian/dfd/level-2-shell-context-and-task-lifecycle.md
proof_refs:
- scripts/validate_session_handoff.py
- scripts/validate_task_outcomes.py
- scripts/validate_plans.py
---

# Durable Work State

Task notes, handoff pointers, plan items, and evidence references that let work
continue without relying only on chat memory.

## Graph Links

- Provides continuity for [[Delivery Gates]].
- Feeds status and evidence into [[Validation Reports]].

## Update Rule

Update this node when the canonical task-state, handoff, or plan storage model
changes.
