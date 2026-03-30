# Checks Matrix

## Fast local checks

| Check | Command | Purpose |
| --- | --- | --- |
| Hook bootstrap (dry run) | `python scripts/install_git_hooks.py --dry-run --allow-no-git` | verify operational bootstrap entrypoint |
| Session handoff contract | `python scripts/validate_session_handoff.py` | keep pointer-shim and context budget valid |
| Task request contract | `python scripts/validate_task_request_contract.py` | enforce objective/scope/repetition controls |
| Phase planning contract | `python scripts/validate_phase_planning_contract.py` | block lazy phase slicing between TZ, execution contract, and phase briefs |
| Solution intent contract | `python scripts/validate_solution_intent.py --from-git --git-ref HEAD` | require explicit `target|staged|fallback` on critical contours |
| Critical contour closure | `python scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD` | block scaffold/sample/synthetic closure claims on pilot contours |
| Stack conformance | `python scripts/validate_stack_conformance.py` | fail-closed stack claim drift between registry, docs, and runtime proof |
| Skills catalog drift | `python scripts/sync_skills_catalog.py --check` | ensure generated catalog matches runtime skills |
| CODEOWNERS coverage | `python scripts/validate_codeowners.py` | ensure ownership routing remains complete |
| Docs links | `python scripts/validate_docs_links.py --roots AGENTS.md docs` | prevent broken markdown references |

## Gate lanes

| Lane | Canonical command | Scope |
| --- | --- | --- |
| Loop gate | `python scripts/run_loop_gate.py --from-git --git-ref HEAD` | fast surface-aware checks |
| PR gate | `python scripts/run_pr_gate.py --from-git --git-ref HEAD` | closeout superset checks |
| Nightly gate | `python scripts/run_nightly_gate.py --from-git --git-ref HEAD` | deep hygiene and reporting |
| Dashboard refresh | `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md` | dashboard/report regeneration lane |
| Phase 8 proving | `python scripts/run_phase8_operational_proving.py --from-git --git-ref HEAD --output artifacts/phase8-operational-proving.json` | consolidated lane proof with fail-closed evidence |

Hosted CI note:
- GitHub-hosted lane execution is enabled only when `AI_SHELL_ENABLE_HOSTED_CI=1`.
- Default-off mode prevents infrastructure-side billing errors from producing false-red checks.

## Durable-state checks
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_task_outcomes.py`
- `python scripts/validate_process_regressions.py`
- `python scripts/validate_codeowners.py`
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`

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
- `python -m pytest tests/process/test_sync_skills_catalog.py -q`
- `python -m pytest tests/process/test_validate_skills.py -q`
- `python -m pytest tests/process/test_skill_update_decision.py -q`
- `python -m pytest tests/process/test_skill_precommit_gate.py -q`
- `python -m pytest tests/process/test_validate_codeowners.py -q`
- `python -m pytest tests/process/test_validate_plans_contract.py -q`
- `python -m pytest tests/process/test_validate_task_request_contract.py -q`
- `python -m pytest tests/architecture/test_context_coverage.py -q`
- `python -m pytest tests/architecture/test_codeowners_coverage.py -q`
- `python -m pytest tests/architecture/test_crosslayer_boundaries.py -q`

## Policy
1. Fast checks must stay deterministic.
2. If a check fails twice, stop feature expansion and remediate process first.
3. No silent bypass for failing policy checks.
