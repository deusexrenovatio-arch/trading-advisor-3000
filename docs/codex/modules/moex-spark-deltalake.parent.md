# Module Parent Brief

Updated: 2026-04-02 08:19 UTC

## Source

- Package Zip: D:/trading advisor 3000/artifacts/packages/moex-spark-deltalake-2026-04-01.zip
- Execution Contract: docs/codex/contracts/moex-spark-deltalake.execution-contract.md

## Module Objective

- Execute the MOEX + Spark + Delta rollout through governed phases that first establish real ingest coverage, then canonical/resampling correctness, then cross-source reconciliation, and only then issue the production contour decision.

## Why This Is Module Path

- The selected primary source defines explicit rollout phases (`Этап 1` to `Этап 4`), not a single implementation increment.
- Acceptance criteria are phase-gated through Foundation, Canonical, Reconciliation, and Operations hardening requirements.
- The package forbids scope drift into broker-execution closure and keeps runtime Finam live contour unchanged.

## Phase Order

1. Phase 01 - Этап 1: Foundation
2. Phase 02 - Этап 2: Canonical
3. Phase 03 - Этап 3: Reconciliation
4. Phase 04 - Этап 4: Production hardening

## Phase State Reconstruction

- Intake normalization only: all four phases are initialized as `planned`; no implementation closure is claimed by this patch.

## Global Constraints

- Preserve source decision boundary: Finam remains live decision contour, MOEX is historical/backfill contour.
- Keep `canonical_bar.v1` contract-safe and do not extend it in place for provenance metadata.
- Keep quality gates fail-closed and block publish on critical failures.
- Keep shell control-plane surfaces free of trading business logic.

## Global Done Evidence

- Four phase briefs exist and keep the source phase order intact.
- Release-surface ownership is one-to-one and every mandatory real contour is owned by a non-`prep_closed` phase.
- Phase planning validation and loop-gate checks pass on the intake diff.

## Open Risks

- Mapping drift between Finam and MOEX identifiers can invalidate downstream reconciliation if versioning is weak.
- Long historical backfill windows can degrade runtime operations if checkpointing and partition strategy are under-specified.
- Reconciliation thresholds may require tuning by instrument class after first live cycles.

## Next Phase To Execute
- none
