# Modules

## Governance Module
- Owns policy docs, ownership rules, and gate entrypoints.
- Must remain free of product business logic.

## Process Module
- Owns lifecycle, context routing, and validation orchestration.
- Enforces deterministic local/PR/nightly lanes.

## State Module
- Owns plans/memory/task-outcomes registries and sync scripts.
- Keeps canonical item-per-file plus generated compatibility outputs.

## Skills Module
- Owns local generic skills and routing policy.
- Baseline excludes domain-specialized skills.

## App Plane Module
- Owns isolated application/product-plane code and app-specific tests.
- Must not leak shell-sensitive behavior back into governance surfaces.

### Product Plane Deep Modules

The App Plane Module is not a single flat product module. Inside
`product-plane`, the target architecture is organized as deep modules:

| Product module | Owns |
| --- | --- |
| Contracts | Shared vocabulary, versioned payloads, schemas, fixtures, compatibility policy. |
| Market Data Foundation | Raw/canonical market truth, sessions, roll maps, freshness, baseline evidence. |
| Research Data Factory | Research-ready frames, indicators, derived indicators, materialization manifests, point-in-time research data rules. |
| Strategy Factory | Strategy definitions, campaigns, vectorbt/Optuna research execution, rankings, findings, projected candidates. |
| Runtime Plane | Signal lifecycle, publication lifecycle, durable runtime state, replay/outcome observations, operator runtime API. |
| Execution Plane | Order intent handoff, broker/sidecar transport, paper/controlled-live execution, reconciliation, execution proof. |

Architecture ownership source:
- `docs/architecture/product-plane/product-plane-module-charters.md`

Public communication contract:
- `docs/architecture/product-plane/product-plane-module-apis.md`

Shell governance treats these as product-plane modules. It may validate their
boundaries, but it must not absorb their trading, data, research, runtime, or
execution logic.
