# Task Note
Updated: 2026-04-26 18:08 UTC

## Goal
- Deliver: materialize full MOEX research indicator and derived indicator tables for 4-year baseline and wire post-canonicalization job

## Task Request Contract
- Objective: fully materialize reusable MOEX research indicator and derived-indicator Delta tables for the approved 4-year baseline, and keep the Dagster data-prep job wired to run after canonical MOEX baseline updates.
- Change Surface: `product-plane`.
- Primary Contexts: `CTX-DATA`, `CTX-RESEARCH`.
- In Scope: product-plane research data-prep assets, Delta writer behavior, derived-indicator materialization, focused tests, and active product-plane runbook/profile docs.
- Out of Scope: shell control-plane trading logic, live trading decisions, feature-layer recreation, broker/runtime execution behavior, and direct `main` publication.
- Constraints: use authoritative `D:/TA3000-data/trading-advisor-3000-nightly` baseline paths; keep `5m` out of the active promotion profile; preserve indicator and derived-indicator layer separation; use PR-oriented branch flow.
- Done Evidence: successful real campaign run `20260426T164735159067Z`, `_delta_log` presence for all materialized tables, row-count equality across bar/indicator/derived tables, focused tests, loop gate, and task-session validation.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Remote `main` was fast-forwarded before work began.
- Branch: `codex/materialize-indicators-derived-job`.
- Real materialization succeeded under `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current`.
- `research_bar_views`, `research_indicator_frames`, and `research_derived_indicator_frames` each have `2,789,911` rows across `13` instruments and `15m/1h/4h/1d`.
- `research_data_prep_after_moex_sensor` is `RUNNING`, monitors successful MOEX baseline update runs, and targets `research_data_prep_job`.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, synthetic upstream, sample artifact, scaffold-only
- Closure Evidence: real canonical dataset from `D:/TA3000-data`, downstream research Delta tables with `_delta_log`, integration test coverage for the Dagster job/sensor, and runtime-ready surface evidence through materialized row counts.
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
- Final Contexts: `CTX-DATA`, `CTX-RESEARCH`
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: focused tests and real run summary.

## Blockers
- No blocker.

## Next Step
- Run final validation and close task session.

## Validation
- `py -3.11 -m pytest tests/product-plane/unit/test_delta_runtime.py tests/product-plane/unit/test_research_derived_indicator_layer.py tests/product-plane/integration/test_research_dagster_jobs.py -q`
- `py -3.11 -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/moex_approved_universe_data_prep.yaml` with `PYTHONPATH=src`
- `py -3.11 scripts/validate_task_request_contract.py`
- `py -3.11 scripts/validate_session_handoff.py`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
