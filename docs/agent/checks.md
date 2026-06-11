# Checks Matrix

## Fast local checks

| Check | Command | Purpose |
| --- | --- | --- |
| Boring checks quick ratchet | `python scripts/run_boring_checks.py --profile quick --scope changed` | parse `pyproject.toml`, run ruff format/check, compile changed Python, and run fast process/architecture tests |
| Hook bootstrap (dry run) | `python scripts/install_git_hooks.py --dry-run --allow-no-git` | verify operational bootstrap entrypoint |
| Solution intent contract | `python scripts/validate_solution_intent.py --from-git --git-ref HEAD` | require explicit `target|staged|fallback` on critical contours |
| Critical contour closure | `python scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD` | block scaffold/sample/synthetic closure claims on pilot contours |
| Stack conformance | `python scripts/validate_stack_conformance.py` | fail-closed stack claim drift between registry, docs, and runtime proof |
| Truth recomposition validator | `python scripts/truth_recomposition.py validate --report <path>` | fail closed when stacked follow-up recomposition still carries temporary downgrade surfaces or out-of-contract deltas |
| PR surface matrix plan | `python scripts/run_surface_pr_matrix.py --plan-only --from-git --git-ref HEAD --output-json artifacts/ci/pr-surface-plan.json --summary-file artifacts/ci/pr-surface-plan.md` | resolve contour-aware profile/check plan and emit CI-visible evidence |
| PR size gate | `python scripts/validate_pr_size.py --from-git --git-ref HEAD` | fail closed when reviewable PR size exceeds 100 files or 3000 line changes; delete-only cold/generated/lifecycle state is excluded |
| Skills catalog drift | `python scripts/sync_skills_catalog.py --check` | ensure generated catalog matches runtime skills |
| CODEOWNERS coverage | `python scripts/validate_codeowners.py` | ensure ownership routing remains complete |
| Docs links | `python scripts/validate_docs_links.py --roots AGENTS.md docs` | prevent broken markdown references |
| Project map contract | `python scripts/validate_project_map.py` | keep Obsidian project-map nodes tied to DFD/source/proof refs |
| Project map item sync | `python scripts/sync_project_map_items.py --check` | ensure generated attention items match current project-map node signals |
| Project cockpit freshness | `python scripts/build_project_cockpit.py --check` | ensure generated HTML stays aligned with project-map notes |
| Legacy namespace growth | `python scripts/validate_legacy_namespace_growth.py` | fail closed when changed files introduce new legacy rename tokens outside migration allowlist |
| Product surface naming | `python scripts/validate_product_surface_naming.py` | fail closed when active product-facing names reintroduce numbered delivery labels |
| Product-plane module import inventory | `python scripts/report_product_plane_module_imports.py --format markdown` | report-only inventory of current imports against product-plane module charters |

Quality baseline note:
- `run_boring_checks.py --profile quick --scope changed` is the mandatory
  changed-file ratchet for current work.
- Full-repository `ruff` and `mypy` remain historical cleanup work until the
  existing baseline debt is removed.

## Gate lanes

| Lane | Canonical command | Scope |
| --- | --- | --- |
| Branch diagnostic lane | `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` | push/manual feedback with branch-local diff semantics; not a merge-required GitHub context |
| Loop gate | `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` | local fast surface-aware checks with explicit markers |
| PR gate | `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` | single PR closeout gate; includes loop gate, PR size gate, and PR-only checks |
| Nightly gate | `python scripts/run_nightly_gate.py --from-git --git-ref HEAD` | deep hygiene and reporting |
| Dashboard refresh | `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md` | dashboard/report regeneration lane |
| Shell delivery proving | `python scripts/run_shell_delivery_operational_proving.py --from-git --git-ref HEAD --output artifacts/shell-delivery-operational-proving.json` | consolidated lane proof with fail-closed evidence |

Hosted CI note:
- GitHub-hosted lane execution is enabled only when `AI_SHELL_ENABLE_HOSTED_CI=1`.
- Default-off mode prevents infrastructure-side billing errors from producing false-red checks.
- Required GitHub statuses are `pr-lane` and `CodeRabbit`; branch push feedback uses `branch-lane`.

## Durable-state checks
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
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
- `python -m pytest tests/architecture/test_context_coverage.py -q`
- `python -m pytest tests/architecture/test_codeowners_coverage.py -q`
- `python -m pytest tests/architecture/test_crosslayer_boundaries.py -q`

## Policy
1. Fast checks must stay deterministic.
2. If a check fails twice, stop feature expansion and remediate process first.
3. No silent bypass for failing policy checks.
