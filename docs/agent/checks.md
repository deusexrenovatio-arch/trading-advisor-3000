# Checks Matrix

## Fast local checks

| Check | Command | Purpose |
| --- | --- | --- |
| Hook bootstrap (dry run) | `python scripts/install_git_hooks.py --dry-run --allow-no-git` | verify operational bootstrap entrypoint |
| Session handoff contract | `python scripts/validate_session_handoff.py` | keep pointer-shim and context budget valid |
| Task request contract | `python scripts/validate_task_request_contract.py` | enforce objective/scope/repetition controls |
| Docs links | `python scripts/validate_docs_links.py --roots AGENTS.md docs` | prevent broken markdown references |

## Gate lanes

| Lane | Canonical command | Scope |
| --- | --- | --- |
| Loop gate | `python scripts/run_loop_gate.py --from-git --git-ref HEAD` | fast surface-aware checks |
| PR gate | `python scripts/run_pr_gate.py --from-git --git-ref HEAD` | closeout superset checks |
| Nightly gate | `python scripts/run_nightly_gate.py --from-git --git-ref HEAD` | deep hygiene and reporting |
| Dashboard refresh | `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md` | dashboard/report regeneration lane |

## Durable-state checks
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_task_outcomes.py`
- `python scripts/validate_process_regressions.py`

## QA matrix core
- `python -m pytest tests/process/test_compute_change_surface.py -q`
- `python -m pytest tests/process/test_context_router.py -q`
- `python -m pytest tests/process/test_gate_scope_routing.py -q`
- `python -m pytest tests/process/test_harness_contracts.py -q`
- `python -m pytest tests/process/test_runtime_harness.py -q`
- `python -m pytest tests/process/test_measure_dev_loop.py -q`
- `python -m pytest tests/process/test_agent_process_telemetry.py -q`
- `python -m pytest tests/process/test_process_reports.py -q`
- `python -m pytest tests/process/test_nightly_root_hygiene.py -q`
- `python -m pytest tests/process/test_validate_plans_contract.py -q`
- `python -m pytest tests/process/test_validate_task_request_contract.py -q`
- `python -m pytest tests/architecture/test_context_coverage.py -q`
- `python -m pytest tests/architecture/test_codeowners_coverage.py -q`
- `python -m pytest tests/architecture/test_crosslayer_boundaries.py -q`

## Policy
1. Fast checks must stay deterministic.
2. If a check fails twice, stop feature expansion and remediate process first.
3. No silent bypass for failing policy checks.
