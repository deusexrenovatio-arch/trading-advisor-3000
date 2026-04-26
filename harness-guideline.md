# Harness Guideline

## Purpose
Map governance principles to enforceable checks, owner surfaces, and CI lanes.

## Principle Mapping

| Principle | Enforcement | Owner surface | Lane |
| --- | --- | --- | --- |
| PR-only main | `.githooks/pre-push` + `python scripts/validate_pr_only_policy.py` | root governance | loop / pre-push |
| Task request contract | `python scripts/validate_task_request_contract.py` | process governance | loop / pr |
| Session handoff pointer shim | `python scripts/validate_session_handoff.py` | process governance | loop / pr |
| Durable state contract | `python scripts/validate_plans.py` + `python scripts/validate_agent_memory.py` + `python scripts/validate_task_outcomes.py` | plans + memory | loop / pr / nightly |
| Process regression controls | `python scripts/validate_process_regressions.py` | outcomes telemetry | nightly |
| Skills runtime governance | `python scripts/sync_skills_catalog.py --check` + `python scripts/validate_skills.py --strict` + `python scripts/skill_precommit_gate.py` | `.codex/skills`, docs/agent skills, legacy `.cursor/skills` cleanup | loop / pr |
| Context coverage | `python scripts/validate_agent_contexts.py` + context tests | docs/agent-contexts + router | loop / pr / nightly |
| Ownership routing coverage | `python scripts/validate_codeowners.py` | `CODEOWNERS` + configs | pr / nightly |
| Architecture boundary policy | `python scripts/validate_architecture_policy.py` + architecture tests | docs/architecture + tests/architecture | loop / pr / nightly |
| Nightly root hygiene | `python scripts/nightly_root_hygiene.py` | repository root hygiene | nightly |
| Dashboard pack generation | `python scripts/build_governance_dashboard.py` + report scripts | artifacts/reporting | dashboard-refresh / nightly |
| Hosted CI opt-in semantics | `AI_SHELL_ENABLE_HOSTED_CI=1` workflow guards | `.github/workflows/ci.yml` | all hosted lanes |

## Lane Contract
1. `loop-lane`: fast deterministic validation for changed surfaces.
2. `pr-lane`: strict closeout checks, ownership/context/skill enforcement, full QA matrix.
3. `nightly-lane`: deep hygiene and longitudinal governance checks.
4. `dashboard-refresh`: deterministic dashboard/report build pack.

## Hosted CI Semantics
- Hosted lane execution is opt-in.
- Canonical switch: repository variable `AI_SHELL_ENABLE_HOSTED_CI=1`.
- When disabled, local gate replay is required for acceptance evidence.
