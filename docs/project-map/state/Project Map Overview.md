---
title: Project Map Overview
type: project-map-overview
status: active
tags:
  - ta3000/project-map
  - ta3000/state-graph
---

# Project Map Overview

This is the primary visual overview. Use Global Graph only as a secondary
relationship radar.

## Level 1

```mermaid
flowchart LR
    Shell["Delivery Shell"]
    Product["Product Plane"]
    Data["Data Plane"]
    Research["Research Plane"]
    Runtime["Runtime Plane"]
    Execution["Execution Plane"]
    Contracts["Contract Surfaces"]
    Map["Project Map"]

    Shell -. governs delivery .-> Product
    Product --> Data
    Data --> Research
    Research --> Runtime
    Runtime --> Execution
    Contracts -. constrains payloads .-> Research
    Contracts -. constrains payloads .-> Runtime
    Contracts -. constrains payloads .-> Execution
    Map -. tracks state .-> Shell
    Map -. tracks state .-> Product
```

## Level 2 Lanes

```mermaid
flowchart TB
    subgraph ShellLane["Delivery Shell"]
        ShellEntry["Agent Entry And Routing"]
        ShellState["Durable Work State"]
        ShellGates["Delivery Gates"]
        ShellReports["Validation Reports"]
        ShellEntry --> ShellState --> ShellGates --> ShellReports
    end

    subgraph DataLane["Data Plane"]
        Route["MOEX Historical Route"]
        Raw["Raw Data Ingest"]
        Canonical["Canonical Data Refresh"]
        Baseline["Baseline Storage"]
        Route --> Raw --> Canonical --> Baseline
    end

    subgraph ResearchLane["Research Plane"]
        Prep["Research Data Prep"]
        Registry["Strategy Registry"]
        Backtest["Backtest And Ranking"]
        Projection["Signal Candidate Projection"]
        Prep --> Registry --> Backtest --> Projection
        Prep --> Backtest
    end

    subgraph RuntimeLane["Runtime Plane"]
        Signal["Signal Lifecycle"]
        Store["Durable Signal Store"]
        Api["Runtime API"]
        Live["Live Decision Boundary"]
        Signal --> Store
        Signal --> Api
        Live -. constrains .-> Signal
    end

    subgraph ExecutionLane["Execution Plane"]
        Paper["Paper Execution"]
        Sidecar["Sidecar Wire Boundary"]
        Broker["Broker Process Closure"]
        Paper --> Sidecar --> Broker
    end

    Baseline --> Prep
    Projection --> Signal
    Signal --> Paper
    Broker -. confirms live boundary .-> Live
```

## Open From Here

| Area | Open |
| --- | --- |
| Delivery shell | [[Delivery Shell]] |
| Product plane | [[Product Plane]] |
| Data plane | [[Data Plane]] |
| Research plane | [[Research Plane]] |
| Runtime plane | [[Runtime Plane]] |
| Execution plane | [[Execution Plane]] |
| Contract surfaces | [[Contract Surfaces]] |
| Project map governance | [[Project Map Update Rules]] |

## Supporting Views

- [[project-governance.base|Project Governance]] shows DFD/source/proof refs.
- [[project-attention.base|Project Attention]] shows current attention items.
- [DFD Index](docs/obsidian/dfd/README.md) is the process/data-flow map set.
- [[project-graph|Project Graph Lens]] is secondary, not the primary view.
