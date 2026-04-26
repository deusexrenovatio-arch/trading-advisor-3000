# Product Plane Status

This document is the current truth source for implemented product-plane reality.
It supersedes older phase-closure claims when they disagree.

## Naming Rule
- Active surfaces use capability names, not phase labels.
- Historical phase shorthand may remain only when provenance would otherwise be lost.
- [shell-delivery-operational-proving.md](docs/architecture/product-plane/shell-delivery-operational-proving.md) is a delivery/evidence overlay, not proof that product capability slices are closed.

## Current Capability Status

| Surface | Status | What is real now | What is not yet real |
| --- | --- | --- | --- |
| Product-plane landing in shell repo | implemented | Code/tests/docs are isolated under product-plane paths. | None at baseline scope. |
| Data/research/runtime scaffolding | implemented | Contracts, fixtures, runtime/research/data modules, and test slices exist. | Full production hardening is not implied. |
| Research plane primary path | implemented | Public research execution now routes through the materialized research path with explicit data prep, Delta-first strategy registry and instance materialization (`research_strategy_families`, `research_strategy_templates`, `research_strategy_template_modules`, `research_strategy_instances`, `research_strategy_instance_modules`), `strategy_space` campaign cutover, vectorbt backtest, ranking, findings, projection, global run-stat/ranking indices, strategy notes, publish marker/quarantine behavior, benchmark evidence, and operational CLI entrypoints. | Legacy compatibility shims remain on a removal path and are no longer accepted as a primary route. |
| MOEX raw-ingest contour | implemented | Versioned MOEX universe + mapping registry, fail-closed mapping validation, real candleborders discovery, and deterministic raw-ingest workflow with idempotent rerun check are implemented under the raw-ingest surfaces; raw ingest preserves native MOEX source granularity (`1m`/`10m`/`1h`/`1d`/`1w`) and keeps runtime `5m`/`15m`/`1h`/`4h`/`1d`/`1w` as downstream resampling targets. Universe covers commodity and index futures families with multi-contract chain discovery for long-horizon backfill. | Reconciliation and release decision contours remain out of scope for this slice. |
| MOEX canonical-refresh contour | in_progress | The canonical-refresh runner builds contract-safe `canonical_bar.v1` outputs for runtime `5m`/`15m`/`1h`/`4h`/`1d`/`1w` via deterministic aggregation, writes provenance into a separate technical table, and emits fail-closed QC/compatibility/runtime-decoupling artifacts. A successful 4-year raw+canonical baseline was pinned from run `20260409T162421Z`. | Cross-source reconciliation and release decision contours remain out of scope for this slice. |
| MOEX reconciliation contour | in_progress | The reconciliation runner ingests Finam archive snapshots with latency metadata, compares MOEX/Finam overlap windows on close/volume/missing/lag-class dimensions, persists queryable drift metrics, and emits threshold-driven alert/escalation artifacts with fail-closed publish behavior. | Acceptance for this contour is not yet granted; operations readiness and the final release decision remain out of scope for this slice. |
| MOEX operations-readiness contour | in_progress | The operations-readiness runner validates scheduler observability, monitoring signal coverage, recovery replay/rollback safety, and emits release decision bundle artifacts with fail-closed Gate A-D + P1/P2 checks. Night profile is set to 21:00 local backfill start with sharded worker mode for morning canonical readiness. | Live-real acceptance verdict is still external; the contour remains blocked until production evidence is accepted without blockers. |
| MOEX historical route consolidation planning truth | in_progress | The active governed planning baseline is the `moex-historical-route-consolidation` contract/module pair: one operator-facing route, fixed `06:00 Europe/Moscow` morning publish target, no separate trust-gate in this module, and deterministic Phase 01-04 migration. Phase-01 contract materialization and the CAS lease API are landed; Phase-02 route code consumes `raw_ingest_run_report.v2`, scopes canonical recompute to governed `changed_windows`, emits parity artifacts, and preserves machine-safe `PASS-NOOP`; Phase-03 runtime surfaces now include a canonical Dagster cutover job, baseline update job, schedule binding, retry policy, single-writer ledger enforcement, recovery/nightly-cycle proof artifacts, and an explicit staging evidence collection path that builds the external Dagster binding report consumed by the canonical runner. | Final governed acceptance of Dagster-only operating ownership, later cleanup closure, and removal of remaining legacy migration guidance are not landed yet. |
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
4. Do not treat legacy research compatibility routes as alternative primary paths when judging the research plane.

## Accepted MOEX Baseline Storage
- Data root: `D:/TA3000-data/trading-advisor-3000-nightly`
- Authoritative canonical root: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- Authoritative raw root: `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current`
- Accepted pinned run: `20260409T162421Z`
- Downstream consumers must resolve raw and canonical Delta tables from the stable data-root baseline paths, not from the latest route rerun folders.
- Later reruns may be useful for refresh attempts, but they are non-authoritative until explicitly pinned into the data-root baseline layout.
- Layout truth source: `docs/runbooks/app/moex-baseline-storage-runbook.md`

## Active Governed MOEX Route Decision
- Active governed planning truth now lives in:
  - the `moex-historical-route-consolidation` execution contract
  - the `moex-historical-route-consolidation` parent module
- Target-state route ownership is fixed to `Dagster -> Python raw ingest -> Spark canonical refresh`.
- Legacy scripts and proof contours may still exist physically during migration, but they are not the target-state route and do not remain sanctioned after cleanup.
- The older `moex-spark-deltalake` module remains historical planning evidence only.
- The explicit routing policy is documented in `docs/architecture/product-plane/moex-historical-route-decision.md`.

## Governed Research Promotion Profile (v2)
- Timeframes for promotion evidence: `15m`, `1h`, `4h`, `1d`.
- `5m` is excluded from the current medium-term promotion profile.
- Approved instrument universe is defined in `docs/architecture/product-plane/approved-universe-v1.md`.
- Promotion runs must use at least 3 instruments from the approved universe and pin the canonical baseline root + universe version in evidence artifacts.

## Live Decision Data Policy (Strict)
1. Historical and batch datasets (including MOEX ingest, canonicalization, reconciliation outputs, canonical bars, and nightly backfill outputs) are not a valid source for intraday live decision-making.
2. Intraday (`within-day`) trading decisions must be taken only from broker live data delivered through the real execution boundary (`StockSharp -> QUIK -> broker/Finam`).
3. Before publishing or acting on any live signal, broker connectivity and live market state must be confirmed through the broker path; MOEX historical snapshots may be used only for research/backfill/analytics context.
4. If broker live data is unavailable, stale, or not confirmed, live decision publication must remain fail-closed.
