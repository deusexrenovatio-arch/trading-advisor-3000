# Runtime Entrypoints

## Rule
Process automation is driven by canonical Python entrypoints and documented hooks.
Do not reintroduce shell-only wrapper flows as primary control paths.

## Lifecycle
- `python scripts/task_session.py begin --request "<request>"`
- `python scripts/task_session.py status`
- `python scripts/task_session.py end`

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_task_outcomes.py`
- `python scripts/validate_codeowners.py`
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`

## Gate entrypoints
- `python scripts/run_loop_gate.py ...`
- `python scripts/run_pr_gate.py ...`
- `python scripts/run_nightly_gate.py ...`
- `python scripts/compute_change_surface.py ...`
- `python scripts/sync_skills_catalog.py ...`

## Hook runtime policy
- Main protection is implemented in `.githooks/pre-push`.
- Emergency override uses neutral variables:
  - `AI_SHELL_EMERGENCY_MAIN_PUSH`
  - `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON`

## Legacy policy
- Legacy gate aliases are not allowed.
