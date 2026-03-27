# Phase 2B Acceptance Checklist

Date: 2026-03-16

## Acceptance Disposition (updated 2026-03-17)
- [x] Phase 2B accepted as MVP baseline slice
- [x] Full module DoD closure is not accepted in the current truth source (`docs/architecture/app/STATUS.md`)

## Baseline Evidence Snapshot (captured 2026-03-17; not full closure)
- Backtest now supports walk-forward windows.
- Commissions, slippage, and session filters are integrated in research execution.
- Strategy metrics are persisted as explicit research outputs and reflected in manifest/tests.
- Research outputs are materialized as physical Delta directories with `_delta_log` and validated via runtime read-back paths.

## Deliverables
- [x] Feature engine added
- [x] Feature/research Delta contract manifest added
- [x] Backtest engine added
- [x] Sample strategy implementations added
- [x] Candidate outputs persisted as physical Delta tables (`*.delta`) with `_delta_log`
- [x] Phase 2B unit/integration tests added

## Acceptance Criteria
- [x] Backtest reproducible
- [x] Point-in-time tests green
- [x] Research outputs written as physical Delta tables and read back through runtime APIs
- [x] Disprover confirms read failure when physical Delta data is deleted while metadata remains
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/app/unit/test_phase2b_features.py -q`
- [x] `python -m pytest tests/app/unit/test_phase2b_manifests.py -q`
- [x] `python -m pytest tests/app/integration/test_phase2b_research_plane.py -q`
- [x] `python -m pytest tests/app/integration/test_phase2a_data_plane.py::test_sample_backfill_disprover_fails_when_physical_delta_data_is_deleted -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## F1 Re-Acceptance Snapshot (2026-03-27)
- [x] Phase 2B remains baseline accepted for a bounded research slice and does not override `docs/architecture/app/STATUS.md`.
- [x] Registry mapping stays explicit: research Delta execution evidence contributes to `delta_lake` (`partial`) and does not imply full distributed closure.
- [x] Negative-test evidence remains required and documented for physical data loss paths.
- [x] Checklist wording remains constrained to non-overclaiming terms.
