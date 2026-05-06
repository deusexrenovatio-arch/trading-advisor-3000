# Task Note
Updated: 2026-03-31 05:55 UTC

## Goal
- Deliver: F1-A phase-01 execution remediation: rebind governed route from intake-normalization note, add canonical run_pr_gate closeout evidence, and continue to acceptance

## Task Request Contract
- Objective: unblock F1-A by restoring route/evidence integrity without widening scope to F1-B+.
- In Scope: `docs/session_handoff.md`; this F1-A execution note; F1-A phase/module pointer surfaces; phase-scoped orchestration evidence for the current run.
- Out of Scope: implementation or acceptance closure for F1-B/F1-C/F1-D/F1-E/F1-F; any trading/business logic; non-F1 module changes.
- Constraints: keep governed route canonical (`codex_governed_entry.py continue`); do not use prior F1 acceptance history as closure evidence; no silent skips/fallbacks/deferred critical checks.
- Done Evidence: active task/session pointer resolves to this F1-A execution note; worker/remediation evidence includes canonical `run_pr_gate`; current F1-A acceptance verdict is `PASS`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Canonical task session started for F1-A execution remediation and handoff pointer now targets this note.
- Route mismatch previously observed: durable state referenced the intake-normalization note instead of a dedicated F1-A execution note.
- Evidence mismatch previously observed: acceptance reran `run_pr_gate` green, but worker evidence package did not include canonical `run_pr_gate` closeout evidence.

## First-Time-Right Report
1. Confirmed coverage: remediation scope is limited to F1-A route/evidence closure.
2. Missing or risky scenarios: phase pointer drift and incomplete closeout evidence can silently re-open blocker loops.
3. Resource/time risks and chosen controls: enforce canonical F1-A pointer first, then run deterministic validation and gate evidence in phase scope.
4. Highest-priority fixes or follow-ups: lock F1-A acceptance first, then hand off to F1-B only after PASS.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: governed pointer durability, worker evidence contract completeness, and closeout gate integrity.
- Next Probe: rerun phase-scoped loop/pr gates and verify evidence payloads against acceptance requirements.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: remediation:phase-only (target)
- Primary Rework Cause: F1-A blocked by route/evidence integrity mismatch
- Incident Signature: none
- Improvement Action: make F1-A execution note and closeout gates explicit in durable route state.
- Improvement Artifact: this task note plus current F1-A run artifacts (`state.json`, `acceptance.json`, worker/remediation report).

## Blockers
- F1-A blocker B1: durable task/session state must point to F1-A execution note, not intake-normalization note.
- F1-A blocker B2: worker evidence package must include canonical `run_pr_gate` closeout evidence.

## Next Step
- Run governed `continue` for F1-A with updated route/evidence contract, drive acceptance to `PASS`, then unlock handoff to F1-B.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_stack_conformance.py`
- `python -m pytest tests/process/test_validate_stack_conformance.py -q`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
