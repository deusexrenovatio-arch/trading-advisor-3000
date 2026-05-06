---
title: State Ownership Rule
type: project-item
item_id: question-state-ownership-rule
item_type: question
linked_node: project-map
status: dropped
aliases:
  - State Ownership Rule
severity: medium
priority: p1
needs_user_attention: true
owner: user-and-agent
created: 2026-05-05
origin_kind: manual-seed
origin_refs:
  - docs/project-map/state/Project Map Update Rules.md
tags:
  - ta3000/project-item
  - ta3000/project-graph
  - item/question
  - status/dropped
  - priority/p1
  - severity/medium
  - origin/manual-seed
  - attention/user
---

# State Ownership Rule

This seed item is outdated. It was created during project-map prototyping before
automated signal sync existed.

Decide how strict the state graph should be:

- manual state on node cards;
- computed state from linked items;
- hybrid state, where high-severity open items force attention even when manual
  node state is stale.

## Current Lean

Hybrid mode is the current MVP default.

## Graph Links

- Belongs to Project Map.
- Governs how the graph lens should treat node state, item severity, and
  attention tags.
