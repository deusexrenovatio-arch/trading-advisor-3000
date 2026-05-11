# Task Note
Updated: 2026-05-11 12:38 UTC

## Goal
- Deliver: Assemble MOEX Delta Spark phase 1 PR over current main

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: integration test evidence plus repo gate for the main-based PR branch covering Spark/Delta raw ingest, raw-Delta canonicalization, and staging runtime split.
- Shortcut Waiver: none

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Created clean PR branch `codex/moex-delta-spark-phase-1` from current `origin/main`.
- Applied phase 1 MOEX Delta/Spark changes without `.serena/project.yml` and without old branch-only commits.
- Restored mainline continuous-front Spark export while keeping new MOEX raw/canonical Spark exports.
- Focused raw/staging and canonical route checks passed; full Dagster route unit set starts heavy integration subprocesses and was stopped after timeout.

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: unknown integrations and policy drifts.
3. Resource/time risks and chosen controls: phased patches and deterministic checks.
4. Highest-priority fixes or follow-ups: stabilize contract and validation first.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: pending
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Rerun loop gate, commit, push, and open or update PR.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/product-plane/unit/test_moex_raw_ingest_spark_delta_contract.py tests/product-plane/unit/test_moex_baseline_update.py tests/product-plane/contracts/test_moex_historical_foundation_contracts.py tests/product-plane/integration/test_moex_raw_ingest_spark_delta_job.py tests/product-plane/unit/test_moex_runtime_instances.py tests/product-plane/unit/test_moex_staging_roots.py tests/product-plane/unit/test_moex_baseline_staging_job_script.py -q`
- `python -m pytest tests/product-plane/unit/test_moex_canonical_route.py -q`
