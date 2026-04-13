# Task Note
Updated: 2026-04-13 12:07 UTC

## Goal
- Deliver: Make the vectorized research contour mandatory instead of optional and reassess remaining Phase 1 work

## Task Request Contract
- Objective: move the new vectorized research contour from optional to mandatory base-install status and re-evaluate the true remaining Phase 1 scope.
- In Scope: `pyproject.toml`, `src/trading_advisor_3000/product_plane/research/**`, targeted Phase 1 unit tests, and this task note.
- Out of Scope: dataset materialization, indicator execution, vectorbt backtest execution, Dagster rewiring, runtime cutover, and remediation of unrelated PR-gate infrastructure failures.
- Constraints: keep the migration product-plane only; preserve the scaffold shape already added for Phase 1; keep import-name compatibility for `pandas-ta-classic` versus `pandas-ta`; avoid claiming full phase closeout if external proof lanes still fail.
- Done Evidence: updated dependency contract in base dependencies, real import smoke for `vectorbt` + `pandas_ta_classic`, targeted unit pass, and loop gate with explicit markers.
- Priority Rule: correctness of dependency policy and truthful phase accounting over minimal diff size.

## Current Delta
- Session started for the dependency-policy correction.
- The current code still models the vectorized stack as optional, which conflicts with the requested Phase 1 direction.
- The environment already has `vectorbt` and `pandas_ta_classic`, so mandatory import smoke can be validated directly.
- Moved `vectorbt` and `pandas-ta-classic` into base project dependencies.
- Reframed the research dependency adapter from optional-fallback semantics to mandatory-base-install semantics, while preserving `pandas_ta_classic` versus `pandas_ta` import-name compatibility.
- Updated Phase 1 unit coverage to validate the base dependency contract and real imports from the current environment.

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
- Improvement Artifact: tests/product-plane/unit/test_phase1_vectorized_research_foundation.py

## Blockers
- No blocker.

## Next Step
- Treat the dependency policy as closed and use the remaining Phase 1 list to decide whether to stop at foundation closeout or continue into real execution slices.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_phase1_vectorized_research_foundation.py -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
