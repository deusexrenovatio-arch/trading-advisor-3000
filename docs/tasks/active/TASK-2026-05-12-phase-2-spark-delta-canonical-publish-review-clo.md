# Task Note
Updated: 2026-05-12 12:31 UTC

## Goal
- Deliver: Phase 2 Spark/Delta canonical publish review closure

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Review P1: canonical/provenance/sidecar publish no longer uses delete-then-append; Spark job uses Delta merge/upsert and writes a recovery manifest before mutation.
- Review P1: QC now includes monotonicity gates for canonical timestamps and provenance source windows.
- Review P2: route contract compatibility no longer passes `bars=[]`; route consumes the Spark publish contract report with real checked row counts.
- Review P2: behavioral Spark/Delta publish proof now covers staged Delta input, merge/upsert mutation, `_delta_log`, row counts, contract rows, recovery manifest, and monotonicity fail-close behavior.

## Solution Intent
- Solution Class: target
- Critical Contour: multi-contour
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: behavioral Spark/Delta integration proof writes the canonical dataset as Delta with `_delta_log`, keeps downstream research sidecars refreshed with scoped overlap, preserves the runtime-ready surface through Dagster/manual route callers, and is checked by `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Shortcut Waiver: none

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
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: review P1/P2 findings, Spark/Delta publish proof, route integration, loop gate.
- Route Match: expanded
- Primary Rework Cause: acceptance_gap
- Incident Signature: publish protocol, monotonicity QC, and contract row-count proof were not yet behavioral enough for merge.
- Improvement Action: tests
- Improvement Artifact: Delta merge/upsert publish protocol, Spark-owned contract report, monotonicity gates, and Docker-backed integration proof.
- Closed At: 2026-05-12T12:31:00Z

## Blockers
- No blocker.

## Next Step
- Ready for PR merge review.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `docker run --rm -v "D:/CodexHome/worktrees/1765/trading advisor 3000:/workspace" -w /workspace -e PYTHONPATH=/workspace/src -e HOME=/tmp/ta3000-phase-proof -e TA3000_SPARK_RUNTIME_ROOT=/tmp/ta3000-phase-proof ta3000-phase-proof:latest python -m pytest tests/product-plane/integration/test_moex_canonical_publish_spark_delta_job.py -q --basetemp /tmp/pytest-phase2-publish-proof-pr`
