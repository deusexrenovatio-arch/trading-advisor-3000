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
state_source: plans/items, memory, validation and report artifacts
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/shell
- level/2
- state/ok
source_refs:
- docs/DEV_WORKFLOW.md
- docs/agent/runtime.md
dfd_refs:
- docs/obsidian/dfd/level-2-shell-context-and-skill-routing.md
proof_refs:
- scripts/validate_plans.py
- scripts/validate_agent_memory.py
---

# Durable Work State

Plan items, memory checks, and evidence references that let work continue
without relying only on chat memory.

## Graph Links

- Provides continuity for [[Delivery Gates]].
- Feeds status and evidence into [[Validation Reports]].

## Update Rule

Update this node when the canonical plan, memory, or evidence storage model
changes.
