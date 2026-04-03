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
| MOEX history foundation contour (Etap 1) | implemented | Versioned MOEX universe + mapping registry, fail-closed mapping validation, real candleborders discovery, and deterministic bootstrap ingest workflow with idempotent rerun check are implemented under phase-01 surfaces; raw ingest preserves native MOEX source granularity (`1m`/`10m`/`1h`/`1d`/`1w`) and keeps runtime `5m`/`15m`/`1h`/`4h`/`1d`/`1w` as downstream resampling targets. Universe covers commodity and index futures families with multi-contract chain discovery for long-horizon backfill. | Reconciliation and release decision contours remain out of scope for this slice. |
| MOEX canonical/resampling contour (Etap 2) | in_progress | Phase-02 canonical runner builds contract-safe canonical_bar.v1 outputs for runtime `5m`/`15m`/`1h`/`4h`/`1d`/`1w` via deterministic aggregation, writes provenance into a separate technical table, and emits fail-closed QC/compatibility/runtime-decoupling artifacts. | Cross-source reconciliation and release decision contours remain out of scope for this slice. |
| MOEX cross-source reconciliation contour (Etap 3) | in_progress | Phase-03 reconciliation runner ingests Finam archive snapshots with latency metadata, compares MOEX/Finam overlap windows on close/volume/missing/lag-class dimensions, persists queryable drift metrics, and emits threshold-driven alert/escalation artifacts with fail-closed publish behavior. | Phase acceptance for this contour is not yet granted; phase-04 operations hardening and final release decision remain out of scope for this slice. |
| MOEX operations hardening contour (Etap 4) | in_progress | Phase-04 production hardening runner validates scheduler observability, monitoring signal coverage, recovery replay/rollback safety, and emits release decision bundle artifacts with fail-closed Gate A-D + P1/P2 checks. Night profile is set to 21:00 local backfill start with sharded worker mode for morning canonical readiness. | Live-real acceptance verdict is still external; the contour remains blocked until production evidence is accepted without blockers. |
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

## Live Decision Data Policy (Strict)
1. Historical and batch datasets (including MOEX phase contours, canonical bars, and nightly backfill outputs) are not a valid source for intraday live decision-making.
2. Intraday (`within-day`) trading decisions must be taken only from broker live data delivered through the real execution boundary (`StockSharp -> QUIK -> broker/Finam`).
3. Before publishing or acting on any live signal, broker connectivity and live market state must be confirmed through the broker path; MOEX historical snapshots may be used only for research/backfill/analytics context.
4. If broker live data is unavailable, stale, or not confirmed, live decision publication must remain fail-closed.
