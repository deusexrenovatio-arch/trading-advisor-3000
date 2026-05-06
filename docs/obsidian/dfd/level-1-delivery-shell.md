---
title: DFD Level 1 - Delivery Shell
type: dfd
dfd_level: 1
status: active
source_of_truth: false
change_surface: shell
tags:
  - ta3000/dfd
  - ta3000/delivery-shell
canonical_sources:
  - DEV_WORKFLOW.md
  - entrypoint.md
  - checks.md
  - skills-routing.md
  - repository-surfaces.md
---

# DFD Level 1 - Delivery Shell

Purpose: show the Delivery Shell as a control plane. It moves requests, policy,
context, diff state, task state, validation results, and evidence.

It does not move product market data and it must not own trading business logic.

```mermaid
flowchart TB
    User["External Entity: User / Operator"]
    Agent["External Entity: Codex Agent"]
    Git["External Entity: Git / Working Tree"]
    GitHub["External Entity: GitHub / PR Review"]

    P11(["P1.1 Request and Surface Intake"])
    P12(["P1.2 Context and Skill Routing"])
    P13(["P1.3 Task Lifecycle Control"])
    P14(["P1.4 Gate Runtime Orchestration"])
    P15(["P1.5 PR and Evidence Packaging"])
    P16(["P1.6 Handoff and Closeout"])

    D11[("D1.1 AGENTS.md / hot agent docs")]
    D12[("D1.2 context cards / skills-routing.md")]
    D13[("D1.3 TASK-*.md / index.yaml / session-lock.json")]
    D14[("D1.4 git working tree diff and change markers")]
    D15[("D1.5 checks.md / validate_*.py")]
    D16[("D1.6 gate summaries / validation output")]
    D17[("D1.7 session_handoff.md")]

    User -->|"request, constraints, acceptance signal"| P11
    Agent -->|"candidate action, selected skills"| P11
    D11 -->|"surface policy and non-negotiable rules"| P11
    P11 -->|"declared surface and task intent"| P12
    D12 -->|"context route, navigation order, skill placement"| P12
    P12 -->|"selected context and required sources"| P13
    P13 -->|"task note, active index, session lock"| D13
    D13 -->|"task contract and lifecycle state"| P14
    Git -->|"changed files, staged state, git ref"| D14
    D14 -->|"diff markers and surface signals"| P14
    D15 -->|"validator commands and gate policy"| P14
    P14 -->|"loop gate result, failures, markers"| D16
    D16 -->|"validated evidence set"| P15
    D14 -->|"PR candidate diff"| P15
    P15 -->|"PR summary, review evidence"| GitHub
    P15 -->|"closeout evidence"| P16
    P16 -->|"handoff status and active pointer"| D17
    P16 -->|"status, blockers, next action"| User
```

## What This Means

Delivery Shell is not a single linear script. It is a control plane made of:

| Control-plane area | Main responsibility |
| --- | --- |
| Policy and routing | Decide surface, context, skills, and source-of-truth path |
| Task lifecycle | Persist active work state, task contract, and handoff pointer |
| Gate runtime | Convert diff and task state into scoped validation commands |
| PR/evidence packaging | Turn checks and artifacts into reviewable closeout evidence |

## Store Discipline

Every Shell store in this DFD is path-backed or command-backed, but labels stay
short. Full paths belong in the linked source docs.

## Decomposed Shell Maps

- [Level 2 - Shell Context And Task Lifecycle](docs/obsidian/dfd/level-2-shell-context-and-task-lifecycle.md)
- [Level 2 - Shell Gate Runtime](docs/obsidian/dfd/level-2-shell-gate-runtime.md)
- [Level 2 - Shell PR And Evidence](docs/obsidian/dfd/level-2-shell-pr-and-evidence.md)

## Shell Boundary Rules

- `shell` owns process policy, lifecycle, gates, durable work state, and reports.
- `shell` must not hold trading business logic or runtime market behavior.
- Product-plane implementation truth stays in product-plane status and contracts.
