---
title: DFD Level 2 - Shell Context And Task Lifecycle
type: dfd
dfd_level: 2
status: active
source_of_truth: false
change_surface: shell
tags:
  - ta3000/dfd
  - ta3000/delivery-shell
  - ta3000/task-lifecycle
canonical_sources:
  - DEV_WORKFLOW.md
  - entrypoint.md
  - skills-routing.md
  - session_handoff.md
  - repository-surfaces.md
---

# DFD Level 2 - Shell Context And Task Lifecycle

Purpose: decompose Shell request intake, context routing, skill routing, task
session creation, and handoff state.

```mermaid
flowchart LR
    User["External Entity: User / Operator"]
    Agent["External Entity: Codex Agent"]
    Git["External Entity: Git / Working Tree"]

    P121(["P1.2.1 Read Hot Policy Docs"])
    P122(["P1.2.2 Classify Change Surface"])
    P123(["P1.2.3 Route Context"])
    P124(["P1.2.4 Route Skills"])
    P131(["P1.3.1 Begin Task Session"])
    P132(["P1.3.2 Maintain Task Contract"])
    P133(["P1.3.3 Update Session Handoff"])
    P134(["P1.3.4 End Or Block Session"])

    D121[("D1.2.1 AGENTS.md / hot agent docs")]
    D122[("D1.2.2 context cards")]
    D123[("D1.2.3 global and repo-local skills")]
    D131[("D1.3.1 TASK-*.md")]
    D132[("D1.3.2 active index.yaml")]
    D133[("D1.3.3 session-lock.json")]
    D134[("D1.3.4 session_handoff.md")]
    D135[("D1.3.5 archive index.yaml")]

    User -->|"request, constraints, clarification"| P121
    Agent -->|"current interpretation"| P121
    D121 -->|"execution rules and boundaries"| P122
    P122 -->|"surface: shell/product-plane/mixed"| P123
    Git -->|"changed files and git signals"| P123
    D122 -->|"navigation order and search seeds"| P123
    P123 -->|"primary context and expansion reason"| P124
    D123 -->|"global and repo-local skill placement rules"| P124
    P124 -->|"selected skills and route"| P131
    P131 -->|"new task note"| D131
    P131 -->|"active task record"| D132
    P131 -->|"session lock"| D133
    D131 -->|"task request contract"| P132
    P132 -->|"updated task outcome and evidence fields"| D131
    D131 -->|"active note path and status"| P133
    P133 -->|"lightweight pointer"| D134
    D131 -->|"terminal outcome"| P134
    D133 -->|"active lock"| P134
    P134 -->|"completed/partial/blocked archive state"| D135
```

## Key Distinction

This map is about state control for the work session. It does not prove product
behavior. Product proof is produced by the relevant product-plane route and then
referenced by Shell evidence.

## Parent Map

- [Level 1 - Delivery Shell](docs/obsidian/dfd/level-1-delivery-shell.md)
