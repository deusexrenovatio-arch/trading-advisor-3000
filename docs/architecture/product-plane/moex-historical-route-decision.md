# MOEX Historical Route Decision

## Purpose
Fix one explicit operator-facing route for MOEX historical data so the repository does not keep multiple competing entrypoints that look equally valid.

This document is the control point for:
- which jobs are authoritative,
- which jobs are manual-only support tools,
- which modules are reusable only as implementation patterns,
- which routes are retired from normal operations.

If this document conflicts with older MOEX architecture narratives, this route-decision document is the source of truth for operator-facing ownership.

## Active Planning Truth

- Active governed planning truth now lives in:
  - the `moex-historical-route-consolidation` execution contract
  - the `moex-historical-route-consolidation` parent module
- Repo-side schemas and CAS lease surfaces now freeze the route contract boundary.
- `raw_ingest_run_report.v2` is the canonical handoff boundary for changed-window scoped canonical recompute, parity manifests, and machine-safe `PASS-NOOP`.
- Runtime surfaces now bind the canonical route to a Dagster job plus nightly schedule and retry policy, while repair and backfill reuse that same job instead of separate operator routes.
- The older `moex-spark-deltalake` governed module remains historical planning evidence only.
- Reconciliation as an independent contour is not part of the active route-consolidation module.

## Decision Summary
The historical route is fixed as one directional flow:

`Dagster orchestration -> Python raw ingest -> authoritative raw delta -> Spark canonical refresh -> authoritative canonical delta`

This means:
1. There is one orchestration owner.
2. There is one raw writer.
3. There is one canonical writer.
4. Manual full-route reruns are no longer a normal operator route.
5. Proof contours remain in the repo, but they are not allowed to compete with the MOEX route.
6. Scheduled ownership belongs to the Dagster route; the manual raw-ingest and canonical-refresh tools remain bounded bootstrap or forensic repair aids, not the normal operator path.

## Authoritative Responsibilities

| Responsibility | Canonical owner | Why |
| --- | --- | --- |
| Refresh window, order of execution, retries, single-writer discipline | Dagster | This is orchestration, not compute or connector logic. |
| MOEX extraction, source-aware retries, chunking, overlap, raw validation | Python `moex_raw_ingest` job | This is connector behavior and needs precise request diagnostics. |
| Heavy canonical recompute, resampling, provenance rebuild | Spark `moex_canonical_refresh` job | This is batch compute and should stay out of the connector layer. |
| Raw storage mutation | Python raw ingest only | Prevents accidental canonical or orchestration code from writing source history. |
| Canonical storage mutation | Spark canonical refresh only | Keeps canonical ownership obvious and fail-closed. |

## Storage Decision
- Authoritative data root stays:
  - `D:/TA3000-data/trading-advisor-3000-nightly`
- Authoritative raw root stays:
  - `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current`
- Authoritative canonical root stays:
  - `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- Authoritative stable data paths stay:
  - `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current/raw_moex_history.delta`
  - `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bars.delta`
  - `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current/canonical_bar_provenance.delta`
- Derived storage stays reserved under:
  - `D:/TA3000-data/trading-advisor-3000-nightly/derived/moex`
- Downstream readers must bind to the stable raw/canonical baseline paths, not to whichever rerun folder happened to be created last.
- Storage layout truth source:
  - `docs/runbooks/app/moex-baseline-storage-runbook.md`

## Existing Entry Point Decision

| Entry point | Decision | Role after cleanup |
| --- | --- | --- |
| `scripts/run_moex_raw_ingest.py` | Keep as manual-only support tool | Manual bootstrap and repair entrypoint; not part of the scheduled route. |
| `scripts/run_moex_canonical_refresh.py` | Keep as manual-only support tool | Manual canonical refresh entrypoint; not part of the scheduled route. |
| `scripts/run_moex_route_refresh.py` | Retire from normal operations | Manual full-route runner; blocked by default and allowed only by explicit acknowledgement for forensic reruns. |
| `scripts/run_phase2a_dagster_proof.py` | Keep as proof-only until cleanup | Tested Dagster proof contour, not the canonical production-like route. |
| `scripts/run_phase2a_spark_proof.py` | Keep as proof-only until cleanup | Tested Spark proof contour, not the canonical production-like route. |

## Reuse Decision For Existing Spark And Dagster Code
Existing `phase2a` Spark and Dagster modules are not deleted because they still provide useful, tested implementation patterns.

They may be reused only at the level of:
- Spark session/runtime setup patterns,
- Delta output validation patterns,
- Dagster asset/job composition patterns,
- proof scaffolding and tests.

They must not be reused as the canonical MOEX route without first being refactored onto the authoritative MOEX raw/canonical storage contract.

Reason:
- they were built around `phase2a` proof scope,
- they materialize proof tables such as `raw_market_backfill`,
- they represent an older proof contour and would otherwise create a second, conflicting historical route.

## Operator Rule
Operators should see only one clear answer to "which route is responsible for historical data":

1. Scheduled refresh ownership belongs to Dagster.
2. Raw history ownership belongs to the Python raw ingest job.
3. Canonical ownership belongs to the Spark canonical refresh job.
4. Repair and backfill must reuse the same Dagster-owned cutover job and the same single-writer ledger discipline.
5. Manual helper scripts are allowed only for bounded bootstrap, repair, or forensic use.
6. Manual full-route reruns are not a normal operating path.

## Cleanup Rule
To avoid accidental launches of the wrong contour:
- active docs must point only to the authoritative route,
- the manual full-route script must stay blocked by default,
- proof contours must remain explicitly labeled as proof-only,
- no runbook may present manual-only or proof-only entrypoints as the normal overnight path.
