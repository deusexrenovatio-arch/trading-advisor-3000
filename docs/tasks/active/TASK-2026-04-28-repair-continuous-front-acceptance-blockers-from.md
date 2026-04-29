# Task Note
Updated: 2026-04-28 18:01 UTC

## Goal
- Deliver: Repair continuous_front acceptance blockers from review
- Change Surface: product-plane
- Solution Intent: target

## Scope
- In Scope: repair intraday roll handoff into research layer, policy-aware materialization identity/reuse, scheduled post-MOEX continuous_front defaults, stable non-oscillating roll selection, and Spark contour claim/implementation alignment.
- Out of Scope: live intraday execution, broker/runtime trading changes, `5m` activation, and overwriting `research/gold/current` during verification.
- Boundary: canonical data remains source truth; continuous_front remains downstream research/backfill truth with isolated verification output until accepted.

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Repaired intraday roll handoff by preserving per-bar active contracts in continuous-front research views.
- Repaired campaign reuse risk by including the full continuous-front policy in the materialization key and lock.
- Repaired scheduled post-MOEX chain by making the research data-prep sensor request continuous-front mode and policy explicitly.
- Repaired roll stability by adding no-rollback maturity rules and using a stricter default confirmation policy for flickering zero-OI slices.
- Repaired Spark contour alignment by deleting the Python continuous-front materializer route and writing continuous-front Delta outputs through the Spark job contour.
- Proved the repair on a clean real verification slice under `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260428-br-202203-repair`.

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
- Final Contexts: CTX-RESEARCH
- Route Match: matched
- Primary Rework Cause: requirements_gap
- Incident Signature: continuous-front acceptance blockers after real proof
- Improvement Action: test
- Improvement Artifact: continuous-front repair patch set plus verification output at `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260428-br-202203-repair`

## Blockers
- No blocker.

## Next Step
- Package for acceptance review or PR after final gate checks.

## Validation
- `py -3.11 -m pytest tests/product-plane/unit/test_research_dataset_layer.py tests/product-plane/unit/test_continuous_front.py tests/product-plane/unit/test_research_campaign_runner.py tests/product-plane/unit/test_continuous_front_spark_job.py tests/product-plane/integration/test_research_dagster_jobs.py::test_research_data_prep_can_source_indicators_from_continuous_front tests/product-plane/integration/test_research_dagster_jobs.py::test_research_definitions_expose_product_jobs_and_moex_success_sensor -q --basetemp=.tmp/pytest-continuous-front-repair`
- Real Delta proof: `continuous_front_bars=1119`, `research_bar_views=1119`, `research_indicator_frames=1119`, `research_derived_indicator_frames=1119`, `missing_front_to_research=0`, QC `PASS`, `_delta_log` present for front/research/indicator/derived tables.
- `py -3.11 scripts/validate_task_request_contract.py`
- `py -3.11 scripts/validate_session_handoff.py`
- `py -3.11 scripts/validate_solution_intent.py`
- `py -3.11 scripts/validate_critical_contour_closure.py`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
