# Execution Contract

Updated: 2026-04-02 08:18 UTC

## Source Package

- Package Zip: D:/trading advisor 3000/artifacts/packages/moex-spark-deltalake-2026-04-01.zip
- Package Manifest: artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/manifest.md
- Manifest Suggested Primary Document: artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/01_TZ_MOEX_Spark_Delta.md
- Source Title: MOEX + Spark + Delta Lake Delivery Package

## Primary Source Decision

- Selected Primary Document: artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/01_TZ_MOEX_Spark_Delta.md
- Selection Rule: the manifest produced one clear top-ranked document, and that document contains the end-to-end scope, functional requirements, hard boundaries, rollout phases, and acceptance structure for governed planning.
- Supporting Documents:
  - `artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/02_DoD_and_Acceptance.md` for Gate A-D acceptance requirements and evidence expectations.
  - `artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/03_Implementation_Backlog.md` for epic/task dependency order behind each source phase.
  - `artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/04_Integration_Contracts_and_Mapping.md` for mapping, contract boundaries, and timeframe policy.
  - `artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/05_Spark_Jobs_and_Schedules.md` for deterministic job inventory and orchestration order.
  - `artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/06_Data_Quality_and_Reconciliation.md` for fail-closed quality and reconciliation rules.
  - `artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/07_Operations_Runbook.md` for recovery and incident requirements.
  - `artifacts/codex/package-intake/20260402T080438Z-moex-spark-deltalake-2026-04-01/extracted/docs/08_Initial_Asset_Universe_Futures_MOEX.md` for initial asset-universe boundaries.
- Conflict Status: no material contradiction was found; supporting documents refine acceptance and operational details without changing the source phase order.

## Prompt / Spec Quality

- Verdict: READY
- Why: the selected primary document is explicit on goals, in/out boundaries, job contour, quality gates, phase rollout, and non-goals, so intake can proceed without clarification.

## Normalization Note

- Suggested phase compiler artifact is `NONE`, so this run preserves the explicit source rollout (`Этап 1` through `Этап 4`) by manual phase normalization.
- This intake patch is phase-planning only: it materializes the execution contract and module phase briefs and does not collapse multi-phase implementation into one package run.

## Objective

- Convert the MOEX + Spark + Delta package into a governed module path with explicit phase ownership, release-gate semantics, and fail-closed contour proof requirements.

## Release Target Contract

- Target Decision: ALLOW_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR
- Target Environment: production data contour with real MOEX ISS history ingest, real Finam archive overlap, scheduled Spark/Delta jobs, fail-closed QC/reconciliation, and replayable operational evidence
- Forbidden Proof Substitutes: docs-only, schema-only, fixture-only, mock-only, stub-only, smoke-only, dry-run-only
- Release-Ready Proof Class: live-real

## Mandatory Real Contours

- moex_history_ingest_contour: real MOEX coverage discovery plus bootstrap/increment ingest for the active futures universe with idempotent reruns
- canonical_resampling_qc_contour: real canonical/resampling path with enforced fail-closed QC on runtime-compatible outputs
- finam_moex_reconciliation_contour: real overlap reconciliation contour with persisted drift metrics and threshold-driven escalation
- operations_recovery_contour: real scheduled operations contour with monitored jobs, replay recovery drill, and incident-ready runbook behavior

## Release Surface Matrix

- Surface: moex_history_ingest_contour | Owner Phase: Этап 1 | Required Proof Class: staging-real | Must Reach: deterministic_coverage_and_bootstrap_ingest
- Surface: canonical_resampling_qc_contour | Owner Phase: Этап 2 | Required Proof Class: staging-real | Must Reach: contract_safe_canonical_and_resampled_outputs
- Surface: finam_moex_reconciliation_contour | Owner Phase: Этап 3 | Required Proof Class: staging-real | Must Reach: operational_cross_source_reconciliation
- Surface: operations_recovery_contour | Owner Phase: Этап 4 | Required Proof Class: live-real | Must Reach: production_hardened_release_decision_bundle

## In Scope

- One canonical execution contract under `docs/codex/contracts/`.
- One module parent brief under `docs/codex/modules/`.
- Four phase briefs preserving the source rollout `Этап 1` to `Этап 4`.
- Active task-note normalization for package-intake traceability.

## Out Of Scope

- Implementing MOEX connectors, Spark jobs, Delta schema migrations, reconciliation runtime, or scheduler code in this intake patch.
- Declaring full broker execution closure (explicitly outside package scope).
- Changing shell gate names, mainline policy, or `docs/session_handoff.md` pointer-shim behavior.
- Injecting trading/business logic into shell control-plane files.

## Constraints

- Do not call the governed launcher again from inside this runtime prompt.
- Treat the `zip` as one source package, not as an already-clean single spec.
- Keep source phase order and semantics intact (`Этап 1 -> Этап 2 -> Этап 3 -> Этап 4`).
- Keep canonical gate names only.
- Keep `docs/session_handoff.md` lightweight and pointer-only.

## Done Evidence

- `docs/codex/contracts/moex-spark-deltalake.execution-contract.md` exists.
- `docs/codex/modules/moex-spark-deltalake.parent.md` exists.
- `docs/codex/modules/moex-spark-deltalake.phase-01.md` through `docs/codex/modules/moex-spark-deltalake.phase-04.md` exist.
- `python scripts/validate_phase_planning_contract.py`
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Primary Change Surfaces

- PROCESS-STATE
- GOV-DOCS
- ARCH-DOCS

## Routing

- Path: module
- Rationale: the primary document declares an explicit multi-phase rollout and corresponding acceptance structure, so the governed continuation path is phase-by-phase execution, not one-shot implementation.

## Mode Hint

- continue

## Next Allowed Unit Of Work
- All planned phases are accepted. Prepare closeout or a new module run.
