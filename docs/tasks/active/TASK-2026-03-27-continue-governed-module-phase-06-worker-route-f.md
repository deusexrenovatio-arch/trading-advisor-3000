# Task Note
Updated: 2026-03-27 06:00 UTC

## Goal
- Deliver: execute governed worker route for module phase-06 R1 (Durable Runtime Default and Service Closure) without opening phase-07+ scope.

## Task Request Contract
- Objective: prove phase-06 durable runtime default and real FastAPI service closure on the worker route with fail-closed behavior.
- In Scope: runtime bootstrap defaults for `staging`/`production`, durable Postgres enforcement, restart/recovery proof path, FastAPI ASGI smoke (`/health`, `/ready`), phase-06 stack/docs conformance evidence.
- Out of Scope: Telegram adapter closure (phase-07), .NET sidecar closure (phase-08), replaceable-stack ADR phase (phase-09), full re-acceptance (phase-10).
- Constraints: phase-only patch; no silent assumptions/skips/fallbacks/deferrals; no in-memory fallback for `staging`/`production`; keep evidence black-box.
- Done Evidence: task/session validators, solution-intent validator, phase-scoped loop gate, phase-06 runtime/API tests, stack-conformance validator, docs-link validation for app architecture/runbooks.
- Priority Rule: route integrity and fail-closed runtime semantics over speed.

## Current Delta
- Session pointer and active task note are aligned to phase-06 worker route.
- Durable runtime bootstrap is enforced for non-test defaults (`staging`/`production`) with explicit Postgres requirement.
- FastAPI ASGI surface is booted through the same runtime bootstrap path and covered by smoke tests.

## Solution Intent
- Solution Class: target
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic publication, sample artifact, smoke only, manifest only, scaffold-only
- Closure Evidence: runtime output is persisted in a durable store under staging or production bootstrap, publication contour state is recovered after restart, and end-to-end publication behavior is verified through runtime integration and FastAPI runtime smoke.
- Shortcut Waiver: none
- Design Checkpoint: keep patch and checks phase-scoped to R1.

## First-Time-Right Report
1. Confirmed coverage: phase-06 objective is covered by runtime bootstrap behavior, API smoke, restart/recovery integration, and stack/docs validators.
2. Missing or risky scenarios: full `--from-git` contour checks on a long-lived mixed worktree can pull unrelated contours into this phase.
3. Resource/time risks and chosen controls: run phase-scoped changed-files gate commands plus explicit phase runtime/API checks.
4. Highest-priority fixes or follow-ups: hand this worker evidence to acceptance; do not start phase-07 before verdict.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: repeated failure on the same phase-06 route/check path.
- Reset Action: capture failing command output, repair only failing phase-06 surface, rerun required checks.
- New Search Space: runtime bootstrap enforcement, FastAPI bootstrap path, stack-conformance/doc drift.
- Next Probe: acceptance rerun on phase-06 evidence set.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: CTX-ORCHESTRATION, CTX-API-UI, ARCH-DOCS, GOV-DOCS
- Route Match: matched
- Route Signal: worker:phase-only
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: none
- Improvement Artifact: none

## Blockers
- Remediation blockers source: none (`Remediation Blockers: NONE`).
- Active blockers in scope: none.

## Next Step
- Complete phase-06 worker checks and hand off to acceptance.

## Executed Evidence
- `python scripts/validate_task_request_contract.py` -> OK
- `python scripts/validate_session_handoff.py` -> OK
- `python scripts/validate_solution_intent.py --changed-files pyproject.toml src/trading_advisor_3000/app/runtime/bootstrap.py src/trading_advisor_3000/app/runtime/__init__.py src/trading_advisor_3000/app/interfaces/asgi.py tests/app/unit/test_phase6_runtime_durable_bootstrap.py tests/app/unit/test_phase6_fastapi_smoke.py tests/app/unit/test_phase6_runtime_profile_ops.py tests/app/integration/test_phase2c_runtime_postgres_store.py docs/architecture/app/STATUS.md docs/architecture/app/stack-conformance-baseline.md docs/runbooks/app/bootstrap.md registry/stack_conformance.yaml docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` -> OK
- `python scripts/run_loop_gate.py --changed-files pyproject.toml src/trading_advisor_3000/app/runtime/bootstrap.py src/trading_advisor_3000/app/runtime/__init__.py src/trading_advisor_3000/app/interfaces/asgi.py tests/app/unit/test_phase6_runtime_durable_bootstrap.py tests/app/unit/test_phase6_fastapi_smoke.py tests/app/unit/test_phase6_runtime_profile_ops.py tests/app/integration/test_phase2c_runtime_postgres_store.py docs/architecture/app/STATUS.md docs/architecture/app/stack-conformance-baseline.md docs/runbooks/app/bootstrap.md registry/stack_conformance.yaml docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` -> OK
- `python -m pytest tests/app/unit/test_phase6_runtime_durable_bootstrap.py -q` -> 6 passed
- `python -m pytest tests/app/unit/test_phase6_fastapi_smoke.py -q` -> 2 passed
- `python -m pytest tests/app/unit/test_phase6_runtime_profile_ops.py -q` -> 4 passed
- `python -m pytest tests/app/integration/test_phase2c_runtime_postgres_store.py -q` -> 1 passed
- `python scripts/validate_stack_conformance.py` -> OK
- `python scripts/validate_docs_links.py --roots docs/architecture/app docs/runbooks/app` -> OK

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_solution_intent.py --changed-files pyproject.toml src/trading_advisor_3000/app/runtime/bootstrap.py src/trading_advisor_3000/app/runtime/__init__.py src/trading_advisor_3000/app/interfaces/asgi.py tests/app/unit/test_phase6_runtime_durable_bootstrap.py tests/app/unit/test_phase6_fastapi_smoke.py tests/app/unit/test_phase6_runtime_profile_ops.py tests/app/integration/test_phase2c_runtime_postgres_store.py docs/architecture/app/STATUS.md docs/architecture/app/stack-conformance-baseline.md docs/runbooks/app/bootstrap.md registry/stack_conformance.yaml docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md`
- `python scripts/run_loop_gate.py --changed-files pyproject.toml src/trading_advisor_3000/app/runtime/bootstrap.py src/trading_advisor_3000/app/runtime/__init__.py src/trading_advisor_3000/app/interfaces/asgi.py tests/app/unit/test_phase6_runtime_durable_bootstrap.py tests/app/unit/test_phase6_fastapi_smoke.py tests/app/unit/test_phase6_runtime_profile_ops.py tests/app/integration/test_phase2c_runtime_postgres_store.py docs/architecture/app/STATUS.md docs/architecture/app/stack-conformance-baseline.md docs/runbooks/app/bootstrap.md registry/stack_conformance.yaml docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md`
- `python -m pytest tests/app/unit/test_phase6_runtime_durable_bootstrap.py -q`
- `python -m pytest tests/app/unit/test_phase6_fastapi_smoke.py -q`
- `python -m pytest tests/app/unit/test_phase6_runtime_profile_ops.py -q`
- `python -m pytest tests/app/integration/test_phase2c_runtime_postgres_store.py -q`
- `python scripts/validate_stack_conformance.py`
- `python scripts/validate_docs_links.py --roots docs/architecture/app docs/runbooks/app`
