# Task Note
Updated: 2026-03-31

## Goal
- Deliver: F1-D phase-04 worker attempt 1 for immutable, commit-linked sidecar evidence hardening without opening F1-E/F1-F scope.

## Task Request Contract
- Objective: execute only F1-D and convert sidecar proof from ad-hoc local runs to governed immutable evidence with hash integrity and disprovers.
- In Scope: sidecar evidence automation script, phase-matching process tests, sidecar/architecture/runbook documentation updates, and phase-route handoff/task-note state.
- Out of Scope: broker connector rollout, release-readiness decision, phase-05/phase-06 work, and any trading/business logic.
- Constraints: no silent assumptions/fallbacks/skips/deferrals; keep phase scope bounded; preserve pointer-shim handoff contract.
- Done Evidence: `python scripts/validate_task_request_contract.py`; `python scripts/validate_session_handoff.py`; `python -m pytest tests/process/test_run_f1d_sidecar_immutable_evidence.py -q`; `python -m pytest tests/app/unit/test_phase8_dotnet_sidecar_assets.py -q`; `python scripts/validate_docs_links.py --roots AGENTS.md docs`; `python scripts/run_f1d_sidecar_immutable_evidence.py --output-root artifacts/f1/phase04/sidecar-immutable --dotnet "C:\\Program Files\\dotnet\\dotnet.exe"`.
- Priority Rule: result quality and fail-closed evidence integrity over implementation speed.

## Current Delta
- Added governed phase-04 entrypoint `scripts/run_f1d_sidecar_immutable_evidence.py` for build/test/publish/smoke replay with commit-linked manifests and immutable hashes.
- Added disprover coverage for broken binary path, kill-switch readiness failure, and artifact-hash mismatch, including deterministic fail-closed checks.
- Added targeted process tests and refreshed sidecar/runbook/status docs so operational guidance matches the new immutable evidence route.

## First-Time-Right Report
1. Confirmed coverage: phase scope includes runtime automation, machine-verifiable artifacts, disprover logic, targeted tests, and phase-route documentation.
2. Missing or risky scenarios: environment without `.NET 8 SDK` cannot produce real compiled sidecar evidence and must fail closed.
3. Resource/time risks and chosen controls: bound changes to phase-04 surfaces; disprovers are implemented as deterministic checks that do not require destructive repo changes.
4. Highest-priority fixes or follow-ups: keep all future sidecar contract changes coupled with immutable hash verification and disprover replay.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: if acceptance reports the same phase-04 evidence blocker twice.
- Reset Action: stop scope growth, capture failing artifacts, and request explicit remediation-only scope.
- New Search Space: evidence manifest integrity, disprover determinism, and replay reproducibility.
- Next Probe: route this worker bundle to acceptance without opening phase-05.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: sidecar_immutable_evidence
- Route Match: worker:phase-only
- Primary Rework Cause: phase-04 evidence hardening for E1 ad-hoc debt removal.
- Incident Signature: none
- Improvement Action: keep hash-manifest verification and disprover replay mandatory for sidecar closure claims.
- Improvement Artifact: `scripts/run_f1d_sidecar_immutable_evidence.py`

## Blockers
- none

## Next Step
- Submit this phase-scoped worker evidence bundle to acceptance while keeping F1-E/F1-F locked.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/process/test_run_f1d_sidecar_immutable_evidence.py -q`
- `python -m pytest tests/app/unit/test_phase8_dotnet_sidecar_assets.py -q`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/run_f1d_sidecar_immutable_evidence.py --output-root artifacts/f1/phase04/sidecar-immutable --dotnet "C:\\Program Files\\dotnet\\dotnet.exe"`
