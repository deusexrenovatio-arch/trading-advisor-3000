# Task Note
Updated: 2026-03-31 21:27 UTC

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
- Re-scoped F1-E truth source back to blocked/planned until lifecycle crosses an external connector boundary.
- Removed documentation overclaims and corrected contract references to canonical schema paths under `src/trading_advisor_3000/app/contracts/schemas/`.
- Hardened runtime fail-closed behavior: Finam session mode no longer synthesizes submit/cancel/replace/updates/fills as accepted lifecycle closure.
- Added test coverage so rollout and process validation reject stub/synthetic connector bindings instead of passing them as closure evidence.
- Kept phase scope bounded to F1-E without opening phase-06 work.
- Replayed governed preflight (`run_id=20260331T212524Z-2cfa7ca773f5`) and observed explicit fail-closed rejection at `finam_session_preflight` because Finam session returned `readonly=true`.

## First-Time-Right Report
1. Confirmed coverage: process, contract, integration, stack-conformance, docs-links, loop-gate, and sidecar .NET tests pass; governed `run_f1e_real_broker_process.py` fails fail-closed on non-executable Finam session binding (`readonly=true`).
2. Missing or risky scenarios: secret rotation, transport miswire, or JWT expiry can invalidate contour proof and require governed replay before reuse.
3. Resource/time risks and chosen controls: kept route fail-closed with disprover coverage and commit-linked immutable artifact bundle.
4. Highest-priority fixes or follow-ups: submit this phase evidence to acceptance without expanding into phase-06 claims.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same phase-05 blocker repeats after two focused attempts.
- Reset Action: stop scope expansion and route through remediation-only closure.
- New Search Space: replayability after secret rotation and connector transport regression.
- Next Probe: acceptance review on this governed evidence bundle.

## Task Outcome
- Outcome Status: blocked
- Decision Quality: environment_blocked
- Final Contexts: broker_execution_contour
- Route Match: matched
- Primary Rework Cause: environment
- Incident Signature: none
- Improvement Action: architecture
- Improvement Artifact: artifacts/codex/orchestration/20260331T204848Z-f1-full-closure-phase-05/attempt-01/acceptance.json

## Blockers
- Real broker lifecycle transport boundary is still open: current governed path can validate Finam session but cannot close submit/replace/cancel/updates/fills on an external executable connector contour.
- Executable Finam account binding is required (`readonly=false` and non-empty `account_ids`) for closure evidence.

## Next Step
- Run governed continue again with updated fail-closed rules and refresh acceptance blockers for the remaining external lifecycle implementation scope only.

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
