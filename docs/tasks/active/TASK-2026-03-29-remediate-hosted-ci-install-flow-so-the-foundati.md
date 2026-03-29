# Task Note
Updated: 2026-03-29 18:18 UTC

## Goal
- Deliver: Remediate hosted CI install flow so the foundation/data stack-conformance PR is mergeable with honest product-plane dependencies and single-contour evidence.

## Task Request Contract
- Objective: make PR `codex/stack-conformance-foundation-data -> main` mergeable on hosted CI without weakening the foundation/data contour or bypassing branch policy.
- In Scope: `pyproject.toml`, `.github/workflows/ci.yml`, `docs/session_handoff.md`, active task note, and any process metadata required for single-contour validation.
- Out of Scope: runtime/sidecar contour changes, new product features, direct push to `main`, and any shortcut that hides missing app dependencies.
- Constraints: keep the diff aligned to `data-integration-closure`, preserve fail-closed truth-source behavior, avoid emergency main push, and prefer one reproducible install path for local plus hosted CI.
- Done Evidence: branch-local validators pass, hosted CI install path is declared in code rather than assumed, PR checks can rerun from the updated branch, and the branch remains pushable against `origin/main`.
- Priority Rule: choose the more reproducible dependency and CI contract even if it is a slightly larger fix.

## Current Delta
- Hosted `pr-lane` failed during test collection because the workflow installed only `pytest` and `pyyaml`, while the foundation/data contour imports `pyarrow` through the declared Delta runtime path.
- The remediation direction is to align declared project dependencies and GitHub Actions install steps so the same install contract is used locally and in hosted CI.

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
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: pending
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Implement focused patch and rerun loop gate.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream, scaffold-only, sample artifact
- Closure Evidence: integration test capable hosted CI install path, canonical dataset importability through declared dependencies, downstream research compatibility, and runtime-ready surface alignment limited to the foundation/data contour
- Shortcut Waiver: none

Design checkpoint:
- chosen path: declare the missing Delta dependency in `pyproject.toml` and switch hosted CI from ad hoc `pytest` installs to editable project installation
- why it is not a shortcut: the workflow will exercise the same dependency graph the branch claims in code, instead of a lighter CI-only environment
- what future shape is preserved: after the data contour lands in `main`, the runtime/sidecar follow-up can rebase on the same reproducible install contract without downgrading data surfaces

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
