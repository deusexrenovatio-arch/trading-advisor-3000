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

`Dagster orchestration -> Python MOEX extraction/staging -> Spark raw Delta ingest -> authoritative raw delta -> Spark canonical refresh -> authoritative canonical delta`

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
| MOEX extraction, source-aware retries, chunking, overlap planning, raw staging | Python foundation job | This is connector behavior and needs precise request diagnostics before Spark owns storage mutation. |
| Raw Delta storage mutation, tail append, scoped overlap reconcile, raw validation report | Spark `moex_raw_ingest` job | Raw history is a durable Delta table; Spark/Delta Lake own the write, merge, and storage proof path. |
| Heavy canonical recompute, resampling, provenance rebuild | Spark `moex_canonical_refresh` job | This is batch compute and should stay out of the connector layer. |
| Canonical storage mutation | Spark canonical refresh only | Keeps canonical ownership obvious and fail-closed. |

## Storage Decision
- Runtime-instance roots and stable table paths are owned by the
  [MOEX runtime registry](deployment/runtime-instances/moex-runtime-instances.v1.yaml).
  Select the instance before resolving its `paths`; product and disposable
  verification instances have separate roots and mutation policies.
- The product host-root default is `instances.moex_product_staging.docker.host_root_default`.
  Its `host_root_env` names the launch override. The
  [product Compose bind](deployment/docker/dagster-staging/docker-compose.dagster-product-main-bind.yml)
  applies that binding to the registered container paths.
- Accepted reads bind to the selected root's exact snapshot/table references,
  not to whichever rerun folder was created last. Source defaults and prose
  paths alone do not establish the active mount or accepted data.
- Raw Delta physical layout is partitioned only by derived `ts_close_year`.
- The raw layout migration is a one-time Spark rewrite with explicit stage/report/promote steps; it must not remain as an alternate active raw route after promotion.
- Storage layout truth source:
  - `docs/runbooks/app/moex-baseline-storage-runbook.md`

### Historical root provenance

The earlier route used `D:/TA3000-data/trading-advisor-3000-nightly`, with
`raw/moex/baseline-4y-current`, `canonical/moex/baseline-4y-current` and reserved
`derived/moex` subpaths. Those dated paths explain historical reports; they do
not override the current registry/environment binding. Runtime selection and
promotion follow the
[production runbook](docs/runbooks/app/ta3000-production-nightly.md), with
accepted manifest and physical readback evidence for the selected instance.

## Existing Entry Point Decision

| Entry point | Decision | Role after cleanup |
| --- | --- | --- |
| `scripts/run_moex_raw_ingest.py` | Keep as manual-only support tool | Manual bootstrap and repair entrypoint; not part of the scheduled route. |
| `scripts/run_moex_raw_layout_migration.py` | Keep as manual-only support tool | One-time Spark raw layout migration: stage, validate, and explicitly promote the stable raw Delta root. |
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
2. MOEX request/extraction ownership belongs to the Python foundation job.
3. Raw Delta mutation belongs to the Spark raw ingest job.
4. Canonical ownership belongs to the Spark canonical refresh job.
5. Repair and backfill must reuse the same Dagster-owned cutover job and the same single-writer ledger discipline.
6. Manual helper scripts are allowed only for bounded bootstrap, repair, or forensic use.
7. Manual full-route reruns are not a normal operating path.

## Cleanup Rule
To avoid accidental launches of the wrong contour:
- active docs must point only to the authoritative route,
- the manual full-route script must stay blocked by default,
- proof contours must remain explicitly labeled as proof-only,
- no runbook may present manual-only or proof-only entrypoints as the normal overnight path.
