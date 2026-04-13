# Task Note
Updated: 2026-04-13 13:31 UTC

## Goal
- Deliver: Implement Phase 4 derived feature layer for the vectorbt plus pandas-ta research plane migration package

## Task Request Contract
- Objective: implement the Phase 4 derived feature layer as a versioned, materialized `research_feature_frames` contour built over research bars plus indicator frames.
- In Scope: `src/trading_advisor_3000/product_plane/research/features/*`, `src/trading_advisor_3000/dagster_defs/phase2b_assets.py`, feature-layer unit/integration tests, manifest tests, and this task note.
- Out of Scope: Stage 5 strategy/vectorbt engine work, Stage 6 ranking/projection work, replacing the legacy `feature_snapshots` backtest bridge, and non-feature runtime changes.
- Constraints: keep canonical data as source-of-truth; do not reintroduce indicator logic into hot path backtests; keep MTF joins point-in-time safe; preserve legacy feature snapshot surfaces until later cutover.
- Done Evidence: green feature registry/unit tests, green feature materialization integration tests, green phase2b Dagster bootstrap integration, and green `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
- Priority Rule: truthful Stage 4 closure and point-in-time correctness beat diff minimization.

## Current Delta
- Session started and Stage 4 scope confirmed from the package technical spec, execution brief, and acceptance plan.
- Added a new versioned `research_feature_frames` materialization path with a feature profile registry and lineage metadata.
- Extended phase2b bootstrap orchestration to materialize feature frames after indicator frames.
- Added focused tests for feature contracts, MTF anti-lookahead behavior, and feature-layer reload/materialization.

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
- Decision Quality: correct_first_time
- Final Contexts: CTX-OPS, APP-PLANE, CTX-RESEARCH
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: test
- Improvement Artifact: tests/product-plane/integration/test_research_feature_materialization.py

## Blockers
- No blocker.

## Next Step
- Treat Phase 4 as closed and continue with Stage 5 strategy specs plus vectorbt backtests.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_research_feature_layer.py -q`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python -m pytest tests/product-plane/unit/test_phase2b_features.py -q`
- `python -m pytest tests/product-plane/unit/test_phase2b_manifests.py -q`
- `python -m pytest tests/product-plane/integration/test_research_feature_materialization.py -q --basetemp .pytest_tmp/research_feature_materialization`
- `python -m pytest tests/product-plane/integration/test_phase2b_dagster_bootstrap.py -q --basetemp .pytest_tmp/phase2b_feature_bootstrap`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
