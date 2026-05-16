# Task Note
Updated: 2026-05-16 14:52 UTC

## Goal
- Deliver: Phase 4 Research Indicators on test Spark staging: bounded Delta/Spark-native planning/read/publish, no implicit fallback, no stale staging reuse, no derived-indicator ownership in this phase.

## Task Request Contract
- Objective: make research indicators and related calculations bounded on Delta/Spark-native read, planning, reuse, and publish paths while keeping Python as formula runtime only.
- Change Surface: product-plane.
- In Scope: indicator partition planning, indicator partition metadata reuse, continuous-front input/base indicator sidecars, and test-staging cleanup inventory.
- Out of Scope: derived indicator sidecar ownership, live/current data root mutation, raw/canonical baseline mutation, and shell-layer trading logic.
- Constraints: no implicit Python full-table fallback, no stale staging reuse, no unbounded hot Delta read, and no `L1` naming in implementation or closeout language.
- Done Evidence: focused pytest, loop gate, PR gate, cleanup inventory/proof artifact, and Spark staging proof against the authoritative current data root.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Solution Intent
- Class: product-plane data/compute behavior change.
- Old behavior now forbidden: indicator planning by Python row-batch iteration over large Delta tables; continuous-front indicators mixing input/base sidecar publication with derived sidecar ownership in the indicators phase; silent reuse of stale staging tails.
- Required proof: test-staging/verification root must be fresh or allowlist-cleaned before proof; proof must report Delta logs, row counts, partition metadata, runtime/worktree binding, and comparison against the authoritative current root where applicable.

## Current Delta
- Added Delta-native Arrow aggregates for indicator partition counts and reuse metadata reads.
- Kept `pandas-ta` as the bounded formula runtime on scoped partition batches.
- Added explicit missing volume-profile source status instead of silent success fallback.
- Split continuous-front input/base indicator sidecars out of the derived-indicator sidecar path.
- Added allowlisted cleanup inventory/deletion script for test-staging/verification indicator tails.
- Verified fresh staging proof under `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/phase4-main-current-compare-20260516T-staging-78-v3`.
- Compared scoped staging rows against `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current`.
- Accepted only the expected volume-profile delta: fresh staging materialized VP values where current has null VP values in the sampled scope.

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: stale staging reuse, implicit Python fallback, and mixed derived-indicator ownership were treated as blockers.
3. Resource/time risks and chosen controls: scoped proof, explicit file-scope gates, and allowlisted cleanup instead of broad root mutation.
4. Highest-priority fixes or follow-ups: derived indicator sidecar ownership remains intentionally outside this phase.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: indicator planning/reuse IO, continuous-front sidecar ownership, staging cleanup, and current-root proof.
- Next Probe: rerun the smallest failing command before any follow-up patch.

## Task Outcome
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-DATA, CTX-RESEARCH, CTX-OPS
- Route Match: expanded
- Primary Rework Cause: workflow_gap
- Incident Signature: phase4 research indicators needed fresh Spark staging proof plus current-root comparison before PR publication.
- Improvement Action: workflow
- Improvement Artifact: docs/tasks/archive/TASK-2026-05-16-phase-4-research-indicators-on-test-spark-stagin.md

## Blockers
- No blocker.

## Next Step
- Publish draft PR to remote `main`.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/integration/test_research_indicator_materialization.py tests/product-plane/continuous_front_indicators/test_rules_and_projection.py tests/product-plane/unit/test_research_indicator_staging_cleanup.py -q`
- `python scripts/cleanup_research_indicator_staging.py --root "D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/phase4-main-current-compare-20260516T-staging-78-v3"`
- `python scripts/run_loop_gate.py --changed-files <phase4-files> --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --changed-files <phase4-files> --snapshot-mode changed-files --profile none`
