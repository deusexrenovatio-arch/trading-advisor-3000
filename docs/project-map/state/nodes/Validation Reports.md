---
title: Validation Reports
type: project-node
node_id: validation-reports
surface: shell
level: 2
parent_node: delivery-shell
state: ok
aliases:
- Validation Reports
needs_user_attention: false
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: docs/agent/checks.md, validation artifacts
tags:
- ta3000/project-node
- ta3000/project-graph
- surface/shell
- level/2
- state/ok
source_refs:
- docs/agent/checks.md
- docs/DEV_WORKFLOW.md
dfd_refs:
- docs/obsidian/dfd/level-2-shell-pr-and-evidence.md
- docs/obsidian/dfd/level-2-shell-gate-runtime.md
proof_refs:
- scripts/run_loop_gate.py
- scripts/run_pr_gate.py
- scripts/validate_docs_links.py
---

# Validation Reports

Reports, validation artifacts, and gate outputs used as evidence that a change
met its acceptance route.

## Graph Links

- Receives proof from [[Delivery Gates]].
- Supports change claims for [[Product Plane]].

## Update Rule

Update this node when validation artifact locations or report semantics change.
