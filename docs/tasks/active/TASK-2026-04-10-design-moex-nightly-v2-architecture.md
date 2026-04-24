# Task Note
Updated: 2026-04-10 09:40 UTC

## Goal
- Deliver: design the target MOEX nightly v2 architecture and migration plan after the 4-year baseline was pinned

## Task Request Contract
- Objective: define the future architecture for incremental nightly refresh without full 4-year rebuilds
- In Scope: architecture docs, planning docs, current-state analysis of MOEX nightly, Spark, Dagster, and live broker boundary
- Out of Scope: implementation of nightly v2 jobs, Dagster assets, Spark refactor, and live broker market-data closure
- Constraints: keep the pinned baseline authoritative; do not rewrite accepted historical evidence; do not claim closures that are still partial
- Done Evidence: architecture document added; current live broker data boundary explained from code; migration stages fixed in writing
- Priority Rule: correctness of architecture and boundaries over speed of implementation

## Current Delta
- Session started and current architecture scope captured.

## First-Time-Right Report
1. Confirmed coverage: the main risk is not storage anymore, but the mismatch between current nightly implementation and the intended Spark + Dagster architecture.
2. Missing or risky scenarios: live broker execution-state sync exists, but a first-class broker market-data pipeline is not equally closed.
3. Resource/time risks and chosen controls: planning-only patch; no production data mutation.
4. Highest-priority fixes or follow-ups: move nightly to candidate delta refresh + Spark canonical recompute + Dagster orchestration.

## Task Outcome
- Outcome Status: completed
- Decision Quality: architecture captured with explicit current-state vs target-state split
- Final Contexts: `docs/architecture/product-plane/moex-nightly-v2-architecture.md`
- Route Match: planning
- Primary Rework Cause: current nightly model still behaves like a rebuild candidate flow instead of an incremental refresh contour
- Incident Signature: nightly-full-rebuild-anti-pattern
- Improvement Action: define candidate/baseline/promotion split and align Python, Spark, and Dagster roles
- Improvement Artifact: `docs/architecture/product-plane/moex-nightly-v2-architecture.md`

## Blockers
- No blocker for planning. Implementation remains a separate change set.

## Next Step
- Implement Stage 1: candidate raw delta refresh from baseline watermark instead of full-horizon nightly rebuild.

## Validation
- `python scripts/validate_docs_links.py`
