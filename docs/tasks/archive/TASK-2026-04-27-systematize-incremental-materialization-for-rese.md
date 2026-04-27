# Task Note
Updated: 2026-04-27 07:03 UTC

## Goal
- Deliver: Systematize incremental materialization for research derived indicators by reusing the canonical/Spark-style partition freshness pattern and align derived-indicator refresh behavior with base indicators

## Task Request Contract
- Objective: make `research_derived_indicator_frames` refresh symmetrically with base indicators by reusing existing derived partitions when source bars, source indicators, profile, and row count are unchanged.
- Change Surface: `product-plane`.
- Primary Contexts: `CTX-RESEARCH`, `CTX-DATA`.
- In Scope: derived-indicator materialization/store logic, focused unit coverage, and a short operator/systematization note if the reusable pattern needs to be named.
- Out of Scope: changing canonical MOEX business rules, adding Spark as a new research runtime, changing live/broker decision paths, recreating features, and direct `main` publication.
- Constraints: keep `5m` outside the active research profile, preserve the base-indicator/derived-indicator boundary, keep changes reviewable on the current product-plane branch, and do not move domain logic into shell surfaces.
- Done Evidence: targeted derived-indicator tests proving partition reuse and refresh, existing research-layer tests, and loop gate on the changed files.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: integration test coverage confirms downstream research materialization reuses unchanged derived-indicator partitions, rewrites only changed canonical dataset handoff partitions, and keeps the runtime-ready surface observable through refreshed/reused/deleted counts.
- Shortcut Waiver: none
- Target Shape: one materialized-layer freshness contract shared in spirit with canonical/Spark paths: stable partition identity, source fingerprint comparison, reuse unchanged partitions, rewrite only changed/deleted partitions, and report reused/refreshed/deleted counts.
- Non-Goals: a broad Spark port of research derived indicators or a one-off speed hack that bypasses Delta correctness.

## Current Delta
- Session started and baseline scope captured.
- Prior full materialization proved the real data root but exposed that derived indicators still rebuild the full layer while base indicators can reuse unchanged partitions.
- Context router selected `CTX-RESEARCH` first, with `CTX-DATA` and `data-integration-closure` as the important companion contour.

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
- Final Contexts: CTX-RESEARCH, CTX-DATA
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: research plane operations note and focused partition-reuse tests.

## Blockers
- No blocker.

## Next Step
- Prepare PR review of the materialized research data patch set.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
