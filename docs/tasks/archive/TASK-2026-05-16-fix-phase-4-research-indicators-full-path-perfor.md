# Task Note
Updated: 2026-05-16 16:20 UTC

## Goal
- Deliver: Fix Phase 4 research indicators full-path performance problems from staging analysis.

## Task Request Contract
- Objective: make research indicator materialization stream bounded partition work into Delta writes and remove Pandas fragmentation in indicator frame assembly.
- Change Surface: product-plane.
- In Scope: indicator partition count grouping, fresh/existing indicator write planning, Pandas column assembly, regression tests, and scoped staging proof.
- Out of Scope: derived-indicator ownership changes, live/current root mutation, raw/canonical baseline mutation, and full authoritative refresh promotion.
- Constraints: keep `pandas-ta` as formula runtime, keep Spark/Delta as storage/planning owner, avoid stale staging reuse, and avoid unbounded hot Delta reads.
- Done Evidence: failing regression tests turned green, focused pytest, Tier 1 and Tier 2 verification roots, loop gate, and PR gate.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Added regression coverage for fresh indicator streaming into writer before loading all partitions.
- Added regression coverage for continuous-front counts spanning roll contracts.
- Added regression coverage for non-fragmenting default and continuous-front profile assembly.
- Fresh indicator table path now streams lazy batches directly to Delta writer.
- Existing table path now uses a light refresh plan and partition-scoped delete/append after rows are built.
- Continuous-front partition counts no longer group by contract_id.
- Pandas output columns are joined in batches instead of inserted one by one.
- Tier 1 and Tier 2 proof roots passed under `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification`.

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: stale staging reuse, partition-count undercount, and delete-before-extend were covered by tests.
3. Resource/time risks and chosen controls: scoped current-root proof before any full refresh attempt.
4. Highest-priority fixes or follow-ups: full current-scope run remains the next expensive acceptance tier.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: indicator materialization planning, Delta partition writer, and continuous-front calculation assembly.
- Next Probe: rerun focused pytest or the smallest failing proof scope before any follow-up patch.

## Task Outcome
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-DATA, CTX-RESEARCH, CTX-OPS
- Route Match: expanded
- Primary Rework Cause: test_gap
- Incident Signature: full-path staging spent too long in Python/Pandas before first indicator Delta output.
- Improvement Action: workflow
- Improvement Artifact: docs/tasks/archive/TASK-2026-05-16-fix-phase-4-research-indicators-full-path-perfor.md

## Blockers
- No blocker.

## Next Step
- Run governance gates and update the draft PR branch.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/integration/test_research_indicator_materialization.py tests/product-plane/continuous_front_indicators/test_rules_and_projection.py tests/product-plane/unit/test_research_indicator_staging_cleanup.py -q`
- Tier 1 proof: `phase4-perf-fix-tier1-nasd-1d-20260516T1608Z`, 941 rows, 1.954s, PASS.
- Tier 2 proof: `phase4-perf-fix-tier2-br-15m-20260516T1615Z`, 65,626 rows, 235.844s, 2.306 GB peak RSS, PASS.
- `python scripts/run_loop_gate.py --changed-files <phase4-performance-files> --snapshot-mode changed-files --profile none`
