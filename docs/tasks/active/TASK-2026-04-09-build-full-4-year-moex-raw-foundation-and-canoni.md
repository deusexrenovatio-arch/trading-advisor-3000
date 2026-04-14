# Task Note
Updated: 2026-04-10 08:35 UTC

## Goal
- Deliver: Build full 4-year MOEX raw foundation and canonical backfill into the D:\TA3000-data nightly data-root layout

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Session started and baseline scope captured.

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
- Decision Quality: accepted baseline pinned from successful nightly run `20260409T162421Z`
- Final Contexts: 4-year MOEX raw + canonical baseline retained in the data-root layout:
  - raw: `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current`
  - canonical: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- Route Match: worker:phase-only
- Primary Rework Cause: MOEX rerun instability was isolated and baseline was pinned from the successful PASS run instead of from the later failed rerun
- Incident Signature: later rerun `20260409T180003Z` failed with `RemoteDisconnected`, so it was explicitly not accepted as baseline
- Improvement Action: data-root baseline layout and promotion runbook were added; failed/superseded runs are now removable without ambiguity
- Improvement Artifact: `scripts/pin_moex_baseline_storage.ps1`

## Blockers
- No blocker for baseline use. Remaining work is only future rerun-hardening against MOEX disconnects.

## Next Step
- Treat the pinned data-root baseline layout as the only authoritative downstream storage until a later successful refresh is explicitly re-pinned.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/product-plane/unit/test_moex_iss_client.py tests/product-plane/unit/test_moex_phase01_ingest_errors.py tests/product-plane/unit/test_moex_phase01_discovery.py tests/product-plane/integration/test_moex_phase01_ingest.py -q --basetemp .tmp/pytest_moex_full`
