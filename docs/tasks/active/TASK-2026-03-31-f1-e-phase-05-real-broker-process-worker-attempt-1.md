# Task Note
Updated: 2026-03-31 19:18 UTC

## Goal
- Deliver: F1-E phase-05 worker attempt 1 for real broker process closure using governed staging-real proof only.

## Task Request Contract
- Objective: execute only F1-E and move `real_broker_process` from planned to implemented in the staging-real contour with fail-closed evidence.
- In Scope: governed F1-E proof entrypoint, connector profile + secrets contract versioning, disprover and recovery tests, and phase-scoped docs/registry/status updates.
- Out of Scope: F1-F operational-readiness or release-decision work, broad production rollout claims, and any trading/business logic.
- Constraints: no silent assumptions/fallbacks/skips/deferrals; keep route explicit (`worker:phase-only`); keep proof class at least `staging-real`.
- Done Evidence: `python scripts/validate_task_request_contract.py`; `python scripts/validate_session_handoff.py`; `python -m pytest tests/process/test_run_f1e_real_broker_process.py -q`; `python -m pytest tests/app/contracts/test_phase5_real_broker_process_contracts.py -q`; `python -m pytest tests/app/integration/test_real_execution_staging_rollout.py -q`; `python scripts/validate_stack_conformance.py`; `python scripts/validate_docs_links.py --roots AGENTS.md docs`; `python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process --dotnet "C:\\Program Files\\dotnet\\dotnet.exe"`; `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
- Priority Rule: result quality and fail-closed proof integrity over implementation speed.

## Solution Intent
- Solution Class: staged
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic publication, sample artifact, smoke only, manifest only, scaffold-only.
- Closure Evidence: staged broker contour run uses runtime output and durable store publication contour evidence from governed artifacts, including end-to-end publication checks and loop-gate execution tied to this phase patch.
- Shortcut Waiver: none

## Current Delta
- Revalidated phase-05 contract/tests/checks in this worker run.
- Re-executed governed phase-05 proof entrypoint and captured fail-closed blocker artifact for missing required secrets in current environment.
- Kept phase scope bounded to F1-E without opening phase-06 work.

## First-Time-Right Report
1. Confirmed coverage: process, contract, and integration checks for F1-E pass in current tree.
2. Missing or risky scenarios: missing required secrets block generation of a new successful real Finam replay.
3. Resource/time risks and chosen controls: real contour run is fail-closed in current environment and recorded via governed failure artifact.
4. Highest-priority fixes or follow-ups: bind required secrets and rerun governed phase-05 proof for replayable successful evidence.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same phase-05 blocker repeats after two focused attempts.
- Reset Action: stop scope expansion and route through remediation-only closure.
- New Search Space: secret binding availability and governed replayability for real contour evidence.
- Next Probe: rerun governed phase-05 proof when required bindings are present.

## Task Outcome
- Outcome Status: blocked
- Decision Quality: environment_blocked
- Final Contexts: broker_execution_contour
- Route Match: matched
- Primary Rework Cause: environment
- Incident Signature: none
- Improvement Action: env
- Improvement Artifact: artifacts/f1/phase05/real-broker-process/20260331T191638Z-7a1dc827e46e/failure.json

## Blockers
- Required secret bindings are absent in the current shell environment:
  - `TA3000_STOCKSHARP_API_KEY`
  - `TA3000_FINAM_API_TOKEN`
- Governed fail-closed artifact for this worker run:
  - `artifacts/f1/phase05/real-broker-process/20260331T191638Z-7a1dc827e46e/failure.json`

## Next Step
- Bind `TA3000_FINAM_API_BASE_URL=https://api.finam.ru` with valid `TA3000_FINAM_API_TOKEN` and `TA3000_STOCKSHARP_API_KEY`, rerun governed phase-05 proof, then return to acceptance without opening phase-06.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/process/test_run_f1e_real_broker_process.py -q`
- `python -m pytest tests/app/contracts/test_phase5_real_broker_process_contracts.py -q`
- `python -m pytest tests/app/integration/test_real_execution_staging_rollout.py -q`
- `python scripts/validate_stack_conformance.py`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process --dotnet "C:\\Program Files\\dotnet\\dotnet.exe"`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
