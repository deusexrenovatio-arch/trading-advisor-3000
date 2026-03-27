# Task Note
Updated: 2026-03-26 17:10 UTC

## Goal
- Deliver: reopen governed module phase 04 (D2 - Spark Execution Closure) and re-earn it on a stable Docker/Linux Spark-to-Delta proof profile.

## Task Request Contract
- Objective: replace the current `Spark compute -> Python Delta write` path with a real Spark-side Delta materialization flow that is reproducible in Docker/Linux.
- In Scope: `src/trading_advisor_3000/spark_jobs/*`, `scripts/run_phase2a_spark_proof.py`, Docker proof-environment files, phase-scoped Spark tests, and aligned architecture/runbook/checklist/registry updates for D2 only.
- Out of Scope: Dagster execution closure, runtime default closure, Telegram/sidecar phases, and any release-proof work.
- Constraints: no silent assumptions/skips/fallbacks/deferrals; keep the patch phase-scoped; Spark proof must no longer rely on host-side Python Delta writes after `collect()`.
- Done Evidence: Docker/Linux proof script succeeds, Spark itself writes Delta outputs, output contract checks pass, phase-scoped Spark tests and loop/PR gates are green, and docs/checklists explicitly describe the Docker/Linux proof profile.
- Priority Rule: honest and reproducible phase closure over speed.

## Current Delta
- Added a shared Linux proof image under `deployment/docker/phase-proofs/`.
- Updated the Spark proof script to run through Docker by default and capture host-visible proof artifacts.
- Reworked the Spark job so Delta outputs are written by Spark itself rather than by the host-side Python Delta writer after `collect()`.
- Updated phase docs, runbooks, checklist language, and stack-conformance registry to reflect the Docker/Linux proof profile.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: integration test evidence confirms Spark writes the canonical dataset to contract-valid Delta outputs, preserves a downstream research handoff, and keeps the same runtime-ready surface in the Linux/Docker proof profile.
- Shortcut Waiver: none
- Design Checkpoint: keep the proof profile narrow, deterministic, and phase-scoped; no implicit fallback back to Windows-local pseudo-closure.

## First-Time-Right Report
1. Confirmed coverage: the real blocker is proof honesty, not the existence of any Spark runtime.
2. Missing or risky scenarios: Docker image dependency drift and container/host path mapping issues.
3. Resource/time risks and chosen controls: one explicit Linux proof image, one script entrypoint, one phase-scoped evidence bundle.
4. Highest-priority fixes or follow-ups: close D2 honestly first, then move to D3 with the corrected upstream proof model.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: if Docker/Linux Spark Delta write still cannot produce contract-valid outputs after two focused fixes, stop and record a concrete environment blocker instead of rephrasing closure.
- Reset Action: capture the exact failing container/build/runtime command and tighten the proof contract before another attempt.
- New Search Space: Docker proof image, Spark Delta writer path, contract validation, and phase docs alignment.
- Next Probe: rerun the phase-scoped Spark tests, proof command, validators, and gates on the corrected D2 surface.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: CTX-DATA, GOV-RUNTIME, ARCH-DOCS
- Route Match: matched
- Route Signal: worker:phase-only
- Primary Rework Cause: proof_honesty_gap
- Incident Signature: d2-spark-delta-writer-gap
- Improvement Action: docker_linux_proof_profile
- Improvement Artifact: containerized Spark Delta proof path and updated D2 evidence bundle.

## Blockers
- No blocker.

## Next Step
- Rerun phase-04 through governed continuation on the corrected Docker/Linux proof path.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/app/integration/test_phase2a_spark_execution.py -q`
- `python -m pytest tests/app/integration/test_phase2a_data_plane.py -q`
- `python -m pytest tests/app/unit/test_phase2a_manifests.py -q`
- `python scripts/run_phase2a_spark_proof.py --output-json artifacts/phase2a-spark-proof.json`
- `python scripts/validate_stack_conformance.py`
- `python -m pytest tests/process/test_validate_stack_conformance.py -q`
- `python scripts/validate_docs_links.py --roots docs/architecture/app docs/runbooks/app docs/checklists/app`
- `python scripts/run_loop_gate.py --skip-session-check --changed-files <phase-04 scoped surface>`
- `python scripts/run_pr_gate.py --skip-session-check --changed-files <phase-04 scoped surface>`
- `python scripts/validate_docs_links.py --roots docs/architecture/app docs/runbooks/app docs/checklists/app`
- `python scripts/run_loop_gate.py --skip-session-check --changed-files <corrected phase-04 scope>`
- `python scripts/run_pr_gate.py --skip-session-check --changed-files <corrected phase-04 scope>`
