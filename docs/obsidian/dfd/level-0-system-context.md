---
title: DFD Level 0 - System Context
type: dfd
dfd_level: 0
status: active
source_of_truth: false
change_surface: mixed
tags:
  - ta3000/dfd
  - ta3000/system-context
canonical_sources:
  - trading-advisor-3000.md
  - repository-surfaces.md
  - STATUS.md
---

# DFD Level 0 - System Context

Purpose: show TA3000 as one system boundary. Level 0 intentionally does not
show internal stores; those belong in Level 1 and Level 2 maps where concrete
paths and table names can be used.

```mermaid
flowchart LR
    User["External Entity: User / Operator"]
    Agent["External Entity: Codex Agent"]
    Market["External Entity: Historical Market Sources"]
    Broker["External Entity: Broker / Live Boundary"]
    GitHub["External Entity: GitHub / PR Review"]

    System(["P0 Trading Advisor 3000"])

    User -->|"requests, ideas, acceptance input"| System
    Agent -->|"planned changes, validation runs"| System
    Market -->|"historical market data"| System
    Broker -->|"live boundary events and broker responses"| System
    System -->|"PRs, checks, review payloads"| GitHub
    System -->|"status, blockers, evidence summaries"| User
```

## Boundary Notes

- Delivery Shell and Product Plane are both inside `P0 Trading Advisor 3000`.
- Delivery Shell moves policy, task state, diff state, checks, and evidence.
- Product Plane moves product contracts, market data, research artifacts,
  runtime state, publications, and execution records.
- Historical/batch data is not a valid intraday live-decision source.

## Next Level

- [Level 1 - Delivery Shell](docs/obsidian/dfd/level-1-delivery-shell.md)
- [Level 1 - Product Plane](docs/obsidian/dfd/level-1-product-plane.md)
