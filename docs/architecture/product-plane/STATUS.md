# Product Plane Status

This document is the current truth source for implemented product-plane reality.
It supersedes older phase-closure claims when they disagree.

## Phase Namespaces
- `S0-S8` = shell delivery/governance phases.
- `P0-P7` = product capability phases from the product-plane spec.
- [phase8-ci-pilot-operational-proving.md](docs/architecture/product-plane/phase8-ci-pilot-operational-proving.md) is a delivery/evidence overlay, not proof that product capability phases are closed.

## Current Capability Status

| Surface | Status | What is real now | What is not yet real |
| --- | --- | --- | --- |
| Product-plane landing in shell repo | implemented | Code/tests/docs are isolated under product-plane paths. | None at baseline scope. |
| Data/research/runtime scaffolding | implemented | Contracts, fixtures, runtime/research/data modules, and test slices exist. | Full production hardening is not implied. |
| Runtime signal lifecycle | implemented | Candidate replay, publication lifecycle, close/cancel/expire flow, and signal-event history exist. | Multi-worker operational hardening is still future work. |
| Durable runtime state | implemented | Runtime bootstrap now wires `staging/production` profiles to `PostgresSignalStore` and blocks non-durable fallbacks; restart proof is covered through the env bootstrap path. | HA topology, replication, and multi-region runtime recovery are not claimed. |
| Service/API runtime surface | implemented | FastAPI ASGI runtime entrypoint boots through the same profile-aware durable runtime bootstrap and has smoke coverage for `/health` and `/ready`. | This is not yet a full production API perimeter (authn/z, rate limiting, external gateway hardening). |
| Paper execution path | implemented | Paper broker flow, replay, reconciliation, and related tests exist. | This does not imply live broker readiness. |
| Live execution transport baseline | implemented | Python live bridge, HTTP transport, staging gateway stub, and an in-repo .NET 8 sidecar project (build/test/publish + compiled-binary Python smoke path) exist; immutable sidecar replay evidence is automated via `scripts/run_f1d_sidecar_immutable_evidence.py`. | This slice still does not imply production broker rollout readiness. |
| StockSharp/QUIK/Finam real broker process | planned | Governed `F1-E` has Finam-native session preflight, fail-closed secret checks, and explicit blocker detection for synthetic/stub lifecycle contour. | Real submit/replace/cancel/updates/fills transport through an external Finam/StockSharp/QUIK boundary is not yet closed, so this surface cannot be promoted to implemented. |
| Contracts freeze | implemented | Release-blocking runtime API, Telegram, sidecar wire (including `/metrics`), runtime config, persistence/migration, rollout/connectivity envelopes are versioned with schema+fixture+tests and explicit compatibility policy; runtime API exclusion decision `F1-C-RUNTIME-API-INVENTORY-SCOPE-V1` is documented in the inventory source. | This does not imply real broker-process closure or production readiness. |
| Production readiness / operational readiness | not accepted | There is production-like scaffolding and proving flow for shell delivery. | The product plane is not accepted as live/prod ready. |

## Acceptance Reading Rule
1. Accept scaffold/runtime claims only to the level evidenced by code, migrations, tests, and runbooks.
2. Do not treat staging gateway or sidecar stubs as proof of real broker execution closure.
3. Do not treat shell `S8` operational proving as product `P7` scale-up closure.
