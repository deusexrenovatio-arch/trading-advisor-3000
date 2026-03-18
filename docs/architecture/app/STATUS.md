# Product Plane Status

This document is the current truth source for implemented product-plane reality.
It supersedes older phase-closure claims when they disagree.

## Phase Namespaces
- `S0-S8` = shell delivery/governance phases.
- `P0-P7` = product capability phases from the product-plane spec.
- [phase8-ci-pilot-operational-proving.md](docs/architecture/app/phase8-ci-pilot-operational-proving.md) is a delivery/evidence overlay, not proof that product capability phases are closed.

## Current Capability Status

| Surface | Status | What is real now | What is not yet real |
| --- | --- | --- | --- |
| Product-plane landing in shell repo | implemented | Code/tests/docs are isolated under product-plane paths. | None at baseline scope. |
| Data/research/runtime scaffolding | implemented | Contracts, fixtures, runtime/research/data modules, and test slices exist. | Full production hardening is not implied. |
| Runtime signal lifecycle | implemented | Candidate replay, publication lifecycle, close/cancel/expire flow, and signal-event history exist. | Multi-worker operational hardening is still future work. |
| Durable runtime state | partial | `PostgresSignalStore`, migrations, and restart/idempotency coverage now exist. | There is no env-wired default runtime entrypoint that forces Postgres in every path yet. |
| Paper execution path | implemented | Paper broker flow, replay, reconciliation, and related tests exist. | This does not imply live broker readiness. |
| Live execution transport baseline | partial | Python live bridge, HTTP transport, staging gateway stub, rollout script, and runbooks exist. | A real .NET StockSharp sidecar project is not landed in this repo. |
| StockSharp/QUIK/Finam real broker process | planned | Wire contract and staging-first rollout surface are defined. | No production broker bridge binary/process is shipped here. |
| Contracts freeze | partial | Versioned schemas/fixtures cover market, signal, runtime signal state, execution, and publications. | Full config/runtime/external envelope inventory still needs expansion as live path grows. |
| Production readiness / operational readiness | not accepted | There is production-like scaffolding and proving flow for shell delivery. | The product plane is not accepted as live/prod ready. |

## Acceptance Reading Rule
1. Accept scaffold/runtime claims only to the level evidenced by code, migrations, tests, and runbooks.
2. Do not treat staging gateway or sidecar stubs as proof of real broker execution closure.
3. Do not treat shell `S8` operational proving as product `P7` scale-up closure.
