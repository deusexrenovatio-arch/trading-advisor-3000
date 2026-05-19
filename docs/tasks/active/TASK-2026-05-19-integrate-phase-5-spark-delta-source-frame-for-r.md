# Task Note
Updated: 2026-05-19 13:52 UTC

## Goal
- Deliver: integrate Phase 5 Spark/Delta source-frame for Research L2 through the PR gate.

## Task Request Contract
- Objective: make Research L2 production materialization use a persisted Spark/Delta source-frame instead of a Python-owned L0/L1 join.
- Change Surface: product-plane.
- In Scope: L2 source-frame table contract, Spark source-frame job, materialization and Dagster wiring, hot-table guardrails, focused tests, and staged/current equality proof.
- Out of Scope: live current promotion, formula rewrite away from pandas, direct main push, shell-domain trading logic, and unrelated session artifacts.
- Constraints: no hidden Python merge fallback, stale source-frame must fail or rebuild, missing Spark runtime must fail, and existing unrelated worktree changes must remain untouched.
- Done Evidence: focused pytest, local loop gate, GitHub CI, staged source-frame proof, current-contour equality proof, and PR merge through GitHub.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Phase 5 implementation is isolated on `codex/phase5-research-l2-source-frame`.
- PR #115 exists and targets `main` through the required PR-only flow.
- Staged source-frame proof passed with persisted Delta output and source versions/hashes.
- Current-contour equality proof matched base current L2 for the selected instruments/date range.
- CI found a missing Solution Intent in the active handoff path; this note records the required contour evidence.

## First-Time-Right Report
1. Confirmed coverage: the implementation, runtime proof, and PR gate are covered by explicit evidence.
2. Missing or risky scenarios: hidden fallback, stale source reuse, and current-contour drift were treated as blocking cases.
3. Resource/time risks and chosen controls: real-root proof stayed isolated under verification storage and did not promote live current data.
4. Highest-priority fixes or follow-ups: merge only after GitHub branch and loop lanes are green.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: none
- Closure Evidence: integration test coverage plus downstream research current-slice equality proof and runtime-ready surface proof for Spark/Delta source-frame output.
- Shortcut Waiver: none
- Chosen path: persisted scoped Delta source-frame assembled by Spark before L2 formula computation.
- Why it is not a shortcut: Python no longer owns the production L0/L1 join, and missing Spark, duplicate L1 keys, or stale source metadata block materialization.
- Future shape preserved: pandas remains the derived-formula runtime while Spark/Delta owns source-frame assembly and lineage.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-DATA, CTX-RESEARCH, CTX-OPS
- Route Match: expanded
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: add PR-gate Solution Intent for the active Phase 5 contour.
- Improvement Artifact: docs/tasks/active/TASK-2026-05-19-integrate-phase-5-spark-delta-source-frame-for-r.md

## Blockers
- No blocker.

## Next Step
- Rerun local gate, push PR metadata fix, mark PR ready, wait for GitHub checks, then merge through the PR.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
