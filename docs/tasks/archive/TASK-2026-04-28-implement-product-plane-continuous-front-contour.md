# Task Note
Updated: 2026-04-28 15:54 UTC

## Goal
- Deliver: Implement product-plane continuous_front contour from PLAN_continious_front.md
- Change Surface: product-plane
- Solution Intent: target

## Scope
- In Scope: continuous_front Delta contracts, causal roll/adjustment materialization, Dagster research data-prep wiring, campaign policy identity, focused product-plane tests, and product-plane docs alignment.
- Out of Scope: live intraday decision use, broker/live execution changes, `5m` activation, chart-only non-causal back-adjusted series, and overwriting accepted production research outputs during verification.
- Boundary: canonical bars/session calendar/roll map remain input truth; continuous_front is a downstream research/backfill contour, not raw ingest or canonical refresh logic.

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Implemented continuous_front as a first-class product-plane research contour after canonical refresh and before research materialization.
- Added deterministic materialization, Delta contracts, Dagster data-prep wiring, campaign policy identity, date-windowed verification, focused tests, and product-plane docs.

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
- Final Contexts: product-plane research data-prep, continuous_front contracts, Dagster assets, campaign config, and docs.
- Route Match: matched
- Primary Rework Cause: test_gap
- Incident Signature: continuous_front_policy_missing_from_loaded_manifest.
- Improvement Action: test
- Improvement Artifact: tests/product-plane/integration/test_research_dagster_jobs.py

## Blockers
- No blocker.

## Next Step
- Ready for PR review or publication.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- Real-data proof: `FUT_BR`, `15m`, `2022-03-12T00:00:00Z..2022-04-26T00:00:00Z` under `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260428-br-202203`.
