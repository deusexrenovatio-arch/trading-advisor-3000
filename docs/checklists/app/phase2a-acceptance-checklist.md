# Phase 2A Acceptance Checklist

Date: 2026-03-16

## Deliverables
- [x] Ingestion module added
- [x] Canonical builder added
- [x] Data quality gate added
- [x] Delta schema manifest added
- [x] Initial Dagster assets skeleton added
- [x] Minimal Spark job skeleton added
- [x] Sample backfill fixture added
- [x] Integration and unit tests for Phase 2A added

## Acceptance Criteria
- [x] Sample backfill works
- [x] Canonical bars built for whitelist instruments
- [x] Data quality tests green
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/app/integration/test_phase2a_data_plane.py -q`
- [x] `python -m pytest tests/app/unit/test_phase2a_builder.py -q`
- [x] `python -m pytest tests/app/unit/test_phase2a_quality.py -q`
- [x] `python -m pytest tests/app/unit/test_phase2a_manifests.py -q`
- [x] `python -m pytest tests/app -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
