# Execution Contract

Updated: 2026-04-11 13:10 UTC

## Source Package

- Package Zip: D:/trading advisor 3000/artifacts/packages/moex-historical-route-consolidation-2026-04-10.zip
- Package Manifest: artifacts/codex/package-intake/20260411T124636Z-moex-historical-route-consolidation-2026-04-10/manifest.md
- Suggested Primary Document: artifacts/codex/package-intake/20260411T124636Z-moex-historical-route-consolidation-2026-04-10/extracted/PLAN_MOEX_spark_dagster.md
- Source Title: MOEX Historical Route Consolidation Plan

## Prompt / Spec Quality

- Verdict: READY
- Why: the source now fixes one canonical route, explicit supersession intent, deterministic phase order, fail-closed handoff rules, fixed morning readiness target, and explicit non-goals without leaving the main route shape ambiguous.

## Operator Decision Overrides

- Independent cross-source trust-gate is intentionally excluded from this module.
- No separate fallback route is part of the target-state operating model.
- No separate human go/no-go gate is required for the morning update path inside this module.
- Canonical morning readiness target is fixed to `06:00 Europe/Moscow`.

## Objective

- Replace the old MOEX governed planning baseline with one explicit historical-route consolidation module that first freezes the contract surfaces, then implements parity-safe route behavior, then cuts over orchestration to Dagster, and only then removes legacy/proof paths.

## Supersession Decision

- This contract is the new active governed planning truth for MOEX historical routing.
- It supersedes:
  - `docs/codex/contracts/moex-spark-deltalake.execution-contract.md`
  - `docs/codex/modules/moex-spark-deltalake.parent.md`
- The superseded module remains historical planning and implementation evidence only.
- Earlier Etap 1 and Etap 2 artifacts remain reusable evidence and implementation patterns.
- Earlier Etap 3 reconciliation scope is intentionally excluded from this module and is not part of the active target-state.
- Earlier Etap 4 production-hardening expectations are replaced by this module's one-route Dagster-first operating model.

## Release Target Contract

- Target Decision: ALLOW_MOEX_HISTORICAL_ROUTE_CONSOLIDATION
- Target Environment: one operator-facing historical data route with authoritative baseline storage, contract-safe handoff, deterministic parity proof, Dagster-owned orchestration, and cleanup of competing legacy/proof paths
- Forbidden Proof Substitutes: docs-only, schema-only, fixture-only, mock-only, smoke-only, dry-run-only
- Release-Ready Proof Class: staging-real for parity/cutover, live-real for morning publish and stabilization evidence

## Mandatory Route Contours

- moex_historical_handoff_contract_contour: versioned `raw_ingest_run_report.v2`, `parity_manifest.v1`, and `technical_route_run_ledger` with executable contract tests
- moex_historical_parity_contour: deterministic raw/canonical parity on the active universe and fixed proof windows
- moex_historical_dagster_cutover_contour: canonical Dagster graph, single-writer enforcement, repair/recovery proof, and two successful governed nightly cycles
- moex_historical_cleanup_contour: one remaining operator-facing route, no dangling proof/legacy references, and stabilization-window evidence

## Mandatory Real Contours

- moex_historical_parity_contour: real governed parity windows on the active universe
- moex_historical_dagster_cutover_contour: real governed nightly cycles through the canonical route
- moex_historical_cleanup_contour: real stabilization-window evidence on the canonical route

## Release Surface Matrix

- Surface: moex_historical_handoff_contract_contour | Owner Phase: Phase 01 | Required Proof Class: schema | Must Reach: executable_route_contracts
- Surface: moex_historical_parity_contour | Owner Phase: Phase 02 | Required Proof Class: staging-real | Must Reach: deterministic_scoped_recompute_and_noop
- Surface: moex_historical_dagster_cutover_contour | Owner Phase: Phase 03 | Required Proof Class: staging-real | Must Reach: canonical_dagster_owned_route
- Surface: moex_historical_cleanup_contour | Owner Phase: Phase 04 | Required Proof Class: live-real | Must Reach: one_route_only_after_stabilization

## In Scope

- One canonical execution contract under `docs/codex/contracts/`.
- One canonical parent brief under `docs/codex/modules/`.
- Four phase briefs preserving the deterministic order `Phase 01 -> Phase 02 -> Phase 03 -> Phase 04`.
- Product-plane truth updates that make the new route the active planning baseline.
- Explicit supersession of the old MOEX governed module as planning truth.

## Out Of Scope

- Reintroducing an independent reconciliation/trust module into this route.
- Live intraday decision data and broker-execution closure.
- Full code implementation of Phase 01+ in this planning patch.
- Changing shell gate names, mainline policy, or `docs/session_handoff.md`.

## Constraints

- Keep one operator-facing route only in the target-state.
- Keep the retained baseline root and stable Delta paths as downstream truth.
- High-risk execution order remains `contracts -> code -> docs/cleanup`.
- The module serves only historical, research-refresh, runtime warm-start, and operator-diagnostics consumers.
- The route must not become a live intraday decision source.

## Done Evidence

- `docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md` exists.
- `docs/codex/modules/moex-historical-route-consolidation.parent.md` exists.
- `docs/codex/modules/moex-historical-route-consolidation.phase-01.md` through `docs/codex/modules/moex-historical-route-consolidation.phase-04.md` exist.
- `docs/architecture/product-plane/STATUS.md` and `docs/architecture/product-plane/moex-historical-route-decision.md` are updated to align the active planning truth.
- `python scripts/validate_phase_planning_contract.py`
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`

## Primary Change Surfaces

- PROCESS-STATE
- ARCH-DOCS
- GOV-DOCS

## Routing

- Path: module
- Rationale: the source is an explicit multi-phase migration with a new active planning truth and a deterministic continuation order, not a one-shot implementation request.

## Mode Hint

- continue

## Next Allowed Unit Of Work
- Execute Phase 04 - Legacy And Proof Cleanup only: Remove competing legacy and proof routes after stabilization and leave exactly one operator-facing MOEX historical route in repo truth, docs, and executable surfaces.
