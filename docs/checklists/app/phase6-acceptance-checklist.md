# Phase 6 Acceptance Checklist

Date: 2026-03-18

## Acceptance Disposition
- [x] Phase 6 baseline evidence is retained for runtime durability hardening and profile-aware service bootstrap.
- [x] Failure recovery plus retry/idempotency behavior remains evidenced for the bounded runtime contour.
- [x] Production readiness remains not accepted in `docs/architecture/app/STATUS.md`.

## Deliverables
- [x] Bridge retry policy and retry-exhaustion contract added
- [x] Live secrets policy enforcement and redaction added
- [x] Controlled-live idempotent submit reuse added
- [x] Recovery playbook with deterministic actions added
- [x] Runtime operational profile endpoint (`/health`, `/ready`, `/metrics`) added
- [x] Production-like compose profile and Prometheus scrape config added
- [x] Unit + integration tests for non-happy execution paths added
- [x] Phase 6 runbook with DR notes added

## Acceptance Criteria
- [x] Failure scenarios replayed in tests (transient + exhausted retries, reconciliation drift)
- [x] Live submit is blocked when secrets policy is enforced and secrets are missing
- [x] Idempotent replay does not duplicate live submit side-effects
- [x] Recovery plan surfaces actionable steps with escalation signal
- [x] PR gate green

## Evidence Commands
- [x] `python -m pytest tests/app/unit/test_phase6_live_bridge_hardening.py -q`
- [x] `python -m pytest tests/app/unit/test_phase6_recovery_and_idempotency.py -q`
- [x] `python -m pytest tests/app/unit/test_phase6_runtime_profile_ops.py -q`
- [x] `python -m pytest tests/app/unit/test_phase6_production_profile_deployment.py -q`
- [x] `python -m pytest tests/app/integration/test_phase6_operational_hardening.py -q`
- [x] `python -m pytest tests/app -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## F1 Re-Acceptance Snapshot (2026-03-27)
- [x] Checklist wording is normalized to truth-source constrained language.
- [x] Registry mapping remains explicit: `durable_runtime_state` and `service_api_runtime_surface` are implemented for bounded runtime/service contours.
- [x] Negative proof expectations remain fail-closed (`staging/production` must not silently fall back to in-memory runtime state).
- [x] This checklist does not claim full operational closure beyond the evidenced contour.
