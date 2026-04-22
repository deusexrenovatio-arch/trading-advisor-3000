# Module Parent Brief

Updated: 2026-04-11 13:10 UTC

## Source

- Package Zip: D:/trading advisor 3000/artifacts/packages/moex-historical-route-consolidation-2026-04-10.zip
- Execution Contract: docs/codex/contracts/moex-historical-route-consolidation.execution-contract.md

## Module Objective

- Replace the old MOEX governed planning baseline with one active route-consolidation module that lands executable handoff contracts first, then parity-safe implementation, then Dagster cutover, and only then final cleanup of legacy/proof paths.

## Why This Is Module Path

- The source defines a deterministic four-phase migration rather than one implementation increment.
- The work spans governed contracts, product-plane route ownership, parity evidence, orchestration cutover, and cleanup policy.
- The route intentionally changes the active planning truth and therefore needs explicit governed continuation instead of ad-hoc follow-up edits.

## Phase Order

1. Phase 01 - Contracts And Handoff Freeze
2. Phase 02 - Route Implementation And Parity Proof
3. Phase 03 - Dagster Cutover And Recovery Proof
4. Phase 04 - Legacy And Proof Cleanup

## Supersession Status

- This parent brief is the active governed continuation pointer for MOEX historical routing.
- `docs/codex/modules/moex-spark-deltalake.parent.md` is historical planning evidence only.
- Earlier Etap 1 and Etap 2 artifacts remain valid implementation and evidence context.
- Earlier Etap 3 reconciliation expectations are intentionally not part of this module's target-state.

## Phase State Reconstruction

- Intake normalization only: all four phases are initialized as `planned`.
- No code, runtime, or operational closure is claimed by this patch.
- Existing implementation and runbook artifacts remain physical repo context until later phases retire or replace them.

## Global Constraints

- Keep one active planning truth for MOEX historical routing.
- Preserve the retained baseline root and stable Delta paths as downstream truth.
- Do not reintroduce an independent trust-gate or a separate fallback route inside this module.
- Keep the morning publish target fixed to `06:00 Europe/Moscow`.
- Keep shell control-plane surfaces free of trading business logic.

## Global Done Evidence

- Four phase briefs exist with explicit objectives, acceptance, disprovers, and surface ownership.
- The parent pointer is explicit and points to `Phase 01`.
- Product-plane truth docs name this module as the active planning baseline.

## Open Risks

- Old repo truth may continue to contradict the new route until supersession updates are complete.
- The CAS lease surface is a meaningful complexity point and must be frozen as an executable contract before code work starts.
- Dagster cutover remains blocked until the canonical route graph exists as a real, non-proof contour.

## Next Phase To Execute
- docs/codex/modules/moex-historical-route-consolidation.phase-04.md
