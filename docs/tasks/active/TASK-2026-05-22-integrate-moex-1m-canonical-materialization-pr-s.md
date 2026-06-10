# Task Note
Updated: 2026-05-22 12:01 UTC

## Goal
- Deliver: Integrate MOEX 1m canonical materialization PR stack

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: integration test coverage proves the canonical dataset handoff through MOEX Spark canonicalization into downstream research materialization, runtime-ready Docker proof hardening, and full branch loop gate on the PR diff against origin/main.
- Shortcut Waiver: none
- Design Checkpoint: chosen path=publish 1m through product-plane contracts, Spark canonical routes, proof runtime, and research materialization; why_not_shortcut=the route uses canonical MOEX storage/contracts and Docker/native runtime guards rather than fixture-only closure; future_shape=the same canonical tables remain the baseline for downstream research rebuilds and runtime-ready publication.

## Task Request Contract
- Objective: publish one reviewable PR stack for MOEX 1m timeframe propagation and research rebuild integration.
- In Scope: product-plane contracts, MOEX data-plane routes, proof runtime hardening, research materialization wiring, focused tests, docs, and required governance state.
- Out of Scope: repo-local skill changes, Plugin Eval artifacts, context-router cleanup, data-inspector UI/API work, and unrelated shell workflow edits.
- Constraints: PR-only main policy, active task session lock for push, keep shell governance separate from product-plane trading/data logic, and preserve unrelated dirty-tree changes.
- Done Evidence: focused product-plane pytest suite, changed-file boring checks, loop gate against origin/main, and draft PR publication.
- Priority Rule: quality and policy correctness over speed.

## Current Delta
- Split current MOEX 1m/materialization work into capability-shaped commits.
- Excluded skills, Plugin Eval, context-router, data-inspector, and unrelated workflow changes from this branch.
- Started the required task session so local pre-push policy can validate the branch.

## First-Time-Right Report
1. Confirmed coverage: contracts, data-plane, proof runtime, research materialization, tests, docs, and governance are separately reviewable.
2. Missing or risky scenarios: local Windows Spark/Delta tests require Hadoop NativeIO and are skipped unless the native runtime is installed.
3. Resource/time risks and chosen controls: use clean verification worktree, focused changed-test suite, and full branch loop gate.
4. Highest-priority fixes or follow-ups: keep unrelated skill/plugin-eval/data-inspector changes out of this PR.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: stop expansion, preserve logs, and reframe around the failing validator/runtime boundary.
- New Search Space: validator contract, runtime guard, or branch diff scope.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: CTX-DATA, CTX-RESEARCH, CTX-CONTRACTS
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Push branch and open draft PR after loop gate and focused tests pass on final HEAD.

## Validation
- `python -m pytest -q --tb=short -ra <focused changed product-plane tests>`
- `python scripts/run_boring_checks.py --profile quick --scope changed --base-sha origin/main --head-sha HEAD`
- `python scripts/run_loop_gate.py --skip-session-check --from-git --base-ref origin/main --head-ref HEAD --snapshot-mode changed-files --profile none`
