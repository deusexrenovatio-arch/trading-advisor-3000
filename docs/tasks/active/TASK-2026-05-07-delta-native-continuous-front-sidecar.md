# Task Note
Updated: 2026-05-07 17:25 UTC

## Goal
- Deliver: Make continuous-front indicator sidecars materialize from Delta batches instead of full Python row loaders.

## Task Request Contract
- Objective: remove the expensive full-row Python materialization path from continuous-front sidecar rebuilds.
- In Scope: Delta/Arrow table reads, sidecar materialization, lineage/QC checks, regression coverage for the hot path, and local hook hardening needed to publish the branch.
- Out of Scope: changing indicator formulas, changing canonical/raw ingest, Docker runtime repair, or publishing isolated verification output to current.
- Constraints: preserve existing Delta table contracts, stable row hashes, and acceptance semantics.
- Done Evidence: focused tests, loop gate, PR-only policy validation, and production-scale isolated verification on the nightly data root.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: none
- Closure Evidence: downstream research sidecars were rebuilt from the canonical dataset in an isolated production-scale verification root with accepted QC and current row counts.
- Shortcut Waiver: none

## Current Delta
- Added filtered Arrow/Delta reads so table scans can stay column-selected and batched.
- Reworked continuous-front indicator sidecars to join and write Delta batches instead of loading full table rows into Python lists.
- Added regression coverage that fails if the hot path calls the old full-row loaders.
- Hardened the Serena post-checkout hook against inherited Git hook environment so pre-push process tests run in their own temp worktrees.

## First-Time-Right Report
1. Confirmed coverage: input projection, base indicators, derived indicators, lineage, QC, and hot-path loader avoidance are covered.
2. Missing or risky scenarios: Docker API health is still a runtime environment issue and is not repaired by this code patch.
3. Resource/time risks and chosen controls: production-scale verification was run into an isolated output root before any current-table publication.
4. Highest-priority fixes or follow-ups: rerun the current gold sidecar materialization after PR merge and Docker runtime recovery.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same gate or runtime failure repeats after two focused edits.
- Reset Action: pause edits, capture the failing command or artifact, and reframe the materialization path.
- New Search Space: Delta scanner filters, Arrow joins, sidecar write batching, and QC readback.
- Next Probe: rerun the smallest failed validator or production-scale sidecar verification.

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
- Docker API remained unhealthy during the earlier data refresh and needs separate runtime repair before orchestration can be treated as clean.

## Next Step
- Push the focused PR and rerun current gold sidecar materialization after merge.

## Validation
- `python -m pytest tests/product-plane/continuous_front_indicators/test_rules_and_projection.py -q`
- `python -m pytest tests/product-plane/continuous_front_indicators/test_legacy_contract_compatibility.py tests/product-plane/contracts/test_continuous_front_contracts.py tests/product-plane/unit/test_continuous_front.py -q`
- `python scripts/run_loop_gate.py --skip-session-check --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
