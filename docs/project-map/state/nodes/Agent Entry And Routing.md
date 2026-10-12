---
title: Agent Entry And Routing
type: project-node
node_id: agent-entry-and-routing
surface: shell
level: 2
parent_node: delivery-shell
state: ok
aliases:
- Agent Entry And Routing
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: docs/agent/entrypoint.md
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/shell
- level/2
- state/ok
source_refs:
- AGENTS.md
- docs/agent/entrypoint.md
- docs/agent/domains.md
- docs/agent-contexts/README.md
dfd_refs:
- docs/obsidian/dfd/level-2-shell-context-routing.md
proof_refs:
- scripts/context_router.py
- scripts/validate_agent_contexts.py
---

# Agent Entry And Routing

Entry docs, domain routing, and context-card orientation for agent work in the
repository.

## Graph Links

- Routes work toward [[Delivery Gates]] when a change needs validation.
- Protects [[Product Plane]] from shell-side domain logic drift.

## Update Rule

Update this node when agent entry docs, routing rules, or context cards change.
