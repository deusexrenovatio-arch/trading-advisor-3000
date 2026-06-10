---
title: DFD Level 2 - Shell Context And Skill Routing
type: dfd
dfd_level: 2
status: active
source_of_truth: false
change_surface: shell
tags:
  - ta3000/dfd
  - ta3000/delivery-shell
  - ta3000/context-routing
canonical_sources:
  - DEV_WORKFLOW.md
  - entrypoint.md
  - skills-routing.md
  - repository-surfaces.md
---

# DFD Level 2 - Shell Context And Skill Routing

Purpose: decompose Shell request intake, surface/risk classification, context
routing, skill routing, and explicit proof-note selection.

```mermaid
flowchart LR
    User["External Entity: User / Operator"]
    Agent["External Entity: Codex Agent"]
    Git["External Entity: Git / Working Tree"]

    P121(["P1.2.1 Read Hot Policy Docs"])
    P122(["P1.2.2 Classify Surface And Risk"])
    P123(["P1.2.3 Route Minimal Context"])
    P124(["P1.2.4 Select Optional Skill"])
    P125(["P1.2.5 Select Verification Path"])
    P131(["P1.3.1 Create Explicit Proof Note"])

    D121[("D1.2.1 AGENTS.md / hot agent docs")]
    D122[("D1.2.2 context cards")]
    D123[("D1.2.3 global and repo-local skills")]
    D124[("D1.2.4 checks.md / gate policy")]
    D131[("D1.3.1 explicit task note")]

    User -->|"request, constraints, clarification"| P121
    Agent -->|"current interpretation"| P121
    D121 -->|"execution rules and boundaries"| P122
    P122 -->|"surface: shell/product-plane/mixed"| P123
    Git -->|"changed files and git signals"| P123
    D122 -->|"navigation order and search seeds"| P123
    P123 -->|"primary context and expansion reason when needed"| P124
    D123 -->|"global and repo-local skill placement rules"| P124
    P124 -->|"ordinary route: zero or one relevant skill"| P125
    D124 -->|"surface-aware checks"| P125
    P125 -->|"durable intent required by risk or validator"| P131
    P131 -->|"solution intent or acceptance evidence"| D131
```

## Key Distinction

The ordinary route does not create a session, handoff pointer, or long process
record. Durable notes are explicit proof artifacts for specific risks, not
navigation memory.

Product proof is produced by the relevant product-plane route and then
referenced by Shell evidence.

## Parent Map

- [Level 1 - Delivery Shell](docs/obsidian/dfd/level-1-delivery-shell.md)
