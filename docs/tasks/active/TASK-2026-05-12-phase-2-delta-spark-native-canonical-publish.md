# Task Note
Updated: 2026-05-12 12:31 UTC

## Goal
- Deliver: Phase 2 Delta/Spark-native canonical publish

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Phase 2 canonical publish moved to Spark/Delta as the production/default route.
- Historical route rejects legacy full-overwrite mode and delegates canonical publish to the Spark job.
- Dagster and manual refresh callers pass scoped Spark/Delta publish mode by default.
- Python Delta read/write hot path for canonical publish and sidecars was removed from the route.
- Spark publish job owns staged canonical/provenance/sidecar Delta mutation, QC, contract reporting, and recovery manifest output.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: integration test coverage for canonical dataset publish path plus `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
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
- Final Contexts: PLAN_delta_spark phase 2, native Spark/Delta publish, route/caller cutover, sidecar overlap, loop gate, Docker Spark proof.
- Route Match: expanded
- Primary Rework Cause: workflow_gap
- Incident Signature: legacy route/caller defaults and Python hot-path publish remained after the first Phase 2 cut.
- Improvement Action: workflow
- Improvement Artifact: Spark/Delta canonical publish job, scoped route/caller cutover, sidecar overlap handling, and behavioral publish proof.
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
