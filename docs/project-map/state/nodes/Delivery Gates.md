---
title: Delivery Gates
type: project-node
node_id: delivery-gates
surface: shell
level: 2
parent_node: delivery-shell
state: ok
aliases:
- Delivery Gates
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: docs/agent/checks.md, scripts/run_loop_gate.py, scripts/run_pr_gate.py
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/shell
- level/2
- state/ok
source_refs:
- docs/agent/checks.md
- docs/DEV_WORKFLOW.md
- docs/agent/critical-contours.md
dfd_refs:
- docs/obsidian/dfd/level-2-shell-gate-runtime.md
proof_refs:
- scripts/run_loop_gate.py
- scripts/run_pr_gate.py
- scripts/validate_critical_contour_closure.py
---

# Delivery Gates

Loop gate, PR gate, validation commands, and acceptance checks used to keep
changes reviewable before they move toward main.

## Graph Links

- Reads work context from [[Durable Work State]].
- Produces proof for [[Validation Reports]].
- Gates changes touching [[Product Plane]].

## Update Rule

Update this node when canonical gate names, validation commands, or acceptance
policy changes.
