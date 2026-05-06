# Task Note
Updated: 2026-03-31 19:01 UTC

## Goal
- Deliver: F1-E phase-05 remediation attempt 2: blockers-only closure

## Task Request Contract
- Objective: remediate attempt-01 blockers for F1-E only by removing assumptions from worker output, restoring phase-to-surface traceability, and attaching successful staging-real broker evidence plus canonical loop-gate proof.
- In Scope: `docs/codex/modules/f1-full-closure.phase-05.md`, F1-E architecture/runbook evidence references, active remediation task note, and required phase checks.
- Out of Scope: F1-F operational-readiness work, release decision, new broker feature expansion, and trading/business logic changes.
- Constraints: stay inside Phase 05; no silent assumptions/fallbacks/skips/deferred critical work; preserve fail-closed behavior; keep evidence proof class at `staging-real`.
- Done Evidence: `python scripts/validate_task_request_contract.py`; `python scripts/validate_session_handoff.py`; `python -m pytest tests/process/test_run_f1e_real_broker_process.py -q`; `python -m pytest tests/app/contracts/test_phase5_real_broker_process_contracts.py -q`; `python -m pytest tests/app/integration/test_real_execution_staging_rollout.py -q`; `python scripts/validate_stack_conformance.py`; `python scripts/validate_docs_links.py --roots AGENTS.md docs`; `python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process --dotnet "C:/Program Files/dotnet/dotnet.exe"`; `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
- Priority Rule: phase-contract correctness and evidence integrity over speed.

## Solution Intent
- Solution Class: staged
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic publication, sample artifact, smoke only, manifest only, scaffold-only.
- Closure Evidence: staged remediation uses runtime output and durable store publication contour evidence from governed artifacts, including end-to-end publication checks and loop-gate execution tied to the current patch.
- Shortcut Waiver: none; remediation remains fail-closed when current environment lacks valid runtime secrets.

## Current Delta
- Added `GOV-RUNTIME` to Phase 05 declared change surfaces to match root runtime entrypoint ownership.
- Synced F1-E architecture and runbook docs to explicitly reference successful governed staging-real evidence bundle (preflight/build/test/publish/smoke/rollout/recovery/hash/manifest).
- Re-ran phase validators/tests and canonical `run_loop_gate` during remediation route.

## First-Time-Right Report
1. Confirmed coverage: blocker-targeted docs/surface alignment, evidence traceability, and required checks are explicit.
2. Missing or risky scenarios: current shell environment lacks live secrets for new successful run replay.
3. Resource/time risks and chosen controls: no scope expansion; reuse governed successful bundle already tied to current git SHA.
4. Highest-priority fixes or follow-ups: operator rerun with valid secrets only if acceptance requests fresh run-id evidence.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

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
- Current shell environment does not expose valid runtime secrets for generating a new successful real Finam preflight run-id in this remediation pass:
  - `TA3000_STOCKSHARP_API_KEY`
  - `TA3000_FINAM_API_TOKEN`

## Next Step
- Rerun acceptance with remediation payload that has empty assumptions and owned-surface evidence contract.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/process/test_run_f1e_real_broker_process.py -q`
- `python -m pytest tests/app/contracts/test_phase5_real_broker_process_contracts.py -q`
- `python -m pytest tests/app/integration/test_real_execution_staging_rollout.py -q`
- `python scripts/validate_stack_conformance.py`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process --dotnet "C:/Program Files/dotnet/dotnet.exe"`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
