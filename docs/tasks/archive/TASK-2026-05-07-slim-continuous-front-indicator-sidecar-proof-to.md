# Task Note
Updated: 2026-05-07 07:29 UTC

## Goal
- Deliver: Slim continuous-front indicator sidecar proof to read materialized Delta frames instead of recomputing indicators

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Continuous-front indicator sidecar proof now reads already materialized
  `research_indicator_frames.delta` and `research_derived_indicator_frames.delta`
  instead of recomputing indicator and derived stacks inside proof.
- Real-data sidecar benchmark against
  `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current` accepted
  1,026,585 rows per continuous-front sidecar in 648.589 seconds.

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
- Final Contexts: product-plane research runtime, task governance, docs
- Route Match: expanded
- Primary Rework Cause: workflow_gap
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: src/trading_advisor_3000/product_plane/research/continuous_front_indicators/pandas_job.py

## Blockers
- No blocker.

## Next Step
- Open PR, then refresh authoritative current sidecars after merge.

## Validation
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators -q --basetemp=.tmp/pytest-cf-proof-slim-full-v5`
- `py -3.11 -m pytest tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py -q --basetemp=.tmp/pytest-cf-proof-slim-nearby-v5`
- `py -3.11 -m compileall src/trading_advisor_3000/product_plane/research/continuous_front_indicators src/trading_advisor_3000/product_plane/research/indicators src/trading_advisor_3000/product_plane/research/derived_indicators`
- `py -3.11 scripts/validate_docs_links.py --roots docs/runbooks/app/continuous-front-indicator-refresh.md docs/architecture/product-plane/continuous-front-indicator-storage.md`
- `git diff --check`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
