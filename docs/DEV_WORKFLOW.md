# Development Workflow (Dual-Surface Delivery)

## Objective
Provide a predictable and enforceable workflow for governance-first delivery in a repository that contains both shell and isolated product-plane surfaces.

## Canonical ordinary loop
1. Read hot docs (`docs/agent/*`).
2. Confirm the change surface (`shell`, `product-plane`, or `mixed`), keep domain logic out of shell control-plane files, and keep the same surface declaration in PR metadata.
3. For non-trivial code changes or new code inside an existing subsystem, start with Serena for code discovery, local pattern learning, impact analysis, and reference checks before broad scans, whole-file reads, or implementation.
4. For architecture-heavy, cross-module, ownership-sensitive, or concept-location uncertain code tasks, follow Architecture Orientation Routing in `docs/agent/skills-routing.md`.
5. Use the minimal skill/context route that owns the current artifact or risk.
6. If the diff matches a critical contour, keep the claim explicit: target, staged, or fallback, with matching evidence in the PR.
7. Run loop gate: `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
8. Run PR gate before closeout.

For product-plane tasks, read `docs/architecture/product-plane/STATUS.md` before treating older phase-closure language as current truth.

## Worktree setup
New worktrees run `scripts/serena_worktree_bootstrap.py` from the tracked `post-checkout` hook when the target commit contains the hook. If a worktree was created from an older commit or the hook was skipped, run:

`py -3.11 scripts/serena_worktree_bootstrap.py --worktree "<worktree-root>"`

## Gate order
Default order: `loop gate -> pr gate -> nightly gate -> dashboard refresh`.

## CI lane model
1. `branch-lane` lives in the branch-diagnostic workflow for push/manual diagnostics and is not merge-required.
2. `pr-lane` is the only repo-owned merge-required PR lane. It runs `run_pr_gate.py`, which includes loop gate, PR size, and PR-closeout checks.
   - planner: `python scripts/run_surface_pr_matrix.py --plan-only ...`
   - executor: `python scripts/run_surface_pr_matrix.py ...`
3. `nightly-lane` lives in the nightly workflow for hygiene, telemetry, and report generation.
4. `dashboard-refresh` lives in the dashboard workflow for deterministic report/dashboard rebuild.
5. Hosted lane execution is opt-in via repository variable:
   - `AI_SHELL_ENABLE_HOSTED_CI=1`
   - default is disabled to avoid false-red checks when hosted runners are unavailable.

Main merge requirement:
- GitHub protection for `main` must require successful `pr-lane` and `CodeRabbit` before merge.
- `pr-lane` is reserved for pull request events so push-range failures cannot shadow a green PR-range check.
- PR size is enforced inside `pr-lane` by `python scripts/validate_pr_size.py`.
- `CodeRabbit` is an external required review status; automatic integration must wait for it.
- `nightly-lane` and `dashboard-refresh` are non-merge lanes and remain post-PR hygiene/report lanes.

## Hosted CI fallback
If hosted runners are unavailable, replay lanes locally for acceptance evidence:
1. `python scripts/run_surface_pr_matrix.py --plan-only --from-git --git-ref HEAD --output-json artifacts/ci/pr-surface-plan.json --summary-file artifacts/ci/pr-surface-plan.md`
2. `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile runtime-api --summary-file artifacts/ci/pr-gate-summary.md`
3. `python scripts/run_nightly_gate.py --from-git --git-ref HEAD`
4. `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md`

## TA3000 Production Workflow
The production nightly contour is a runtime contour, not a development contour.

- Runtime branch: `ta3000-production`.
- Runtime checkout: `D:/TA3000-production`.
- Runtime data root: `D:/TA3000-data/trading-advisor-3000-nightly`.
- Production launcher: `C:/Users/Admin/run_ta3000_production_nightly.cmd`.
- Product staging bootstrap: `scripts/run_ta3000_product_staging_bootstrap.cmd`.
- Production log: `D:/TA3000-data/logs/ta3000-production-nightly.log`.

Runtime surfaces:

- Production staging is `moex_product_staging`. It is the long-lived product
  runtime surface backed by `D:/TA3000-production` for code and
  `D:/TA3000-data/trading-advisor-3000-nightly` for data. Its containers expose
  the production checkout as `/workspace` and the product data root as
  `/ta3000-data/moex-historical`.
- Production staging runs the supported MOEX updater through Dagster:
  `moex_baseline_daily_update_schedule` owns the nightly tick and
  `moex_baseline_update_job` owns the update work. Windows Task Scheduler only
  bootstraps or refreshes this container runtime; it must not run the updater
  directly with host Python.
- Test staging is `moex_test_staging_on_demand`. Use it for verification runs,
  route changes, seeded smoke tests, and risky data-plane checks before touching
  production staging. It writes under
  `D:/TA3000-data/trading-advisor-3000-verification` and must not mutate the
  production data root.
- Runtime instance names, paths, mounts, and launch defaults are declared in the
  MOEX runtime registry.

Concrete references:

- Product staging registry entry: `moex_product_staging` in
  [deployment/runtime-instances/moex-runtime-instances.v1.yaml](../deployment/runtime-instances/moex-runtime-instances.v1.yaml).
- Test staging registry entry: `moex_test_staging_on_demand` in
  [deployment/runtime-instances/moex-runtime-instances.v1.yaml](../deployment/runtime-instances/moex-runtime-instances.v1.yaml).
- Product staging base compose:
  [deployment/docker/dagster-staging/docker-compose.dagster-staging.yml](../deployment/docker/dagster-staging/docker-compose.dagster-staging.yml).
- Product staging production-checkout bind:
  [deployment/docker/dagster-staging/docker-compose.dagster-product-main-bind.yml](../deployment/docker/dagster-staging/docker-compose.dagster-product-main-bind.yml).
- Product staging bootstrap:
  [scripts/run_ta3000_product_staging_bootstrap.cmd](../scripts/run_ta3000_product_staging_bootstrap.cmd).
- Production nightly runbook:
  [docs/runbooks/app/ta3000-production-nightly.md](docs/runbooks/app/ta3000-production-nightly.md).
- Runtime contract test:
  [tests/process/test_ta3000_product_staging_nightly_contract.py](../tests/process/test_ta3000_product_staging_nightly_contract.py).

`ta3000-production` is not used for feature work, fixes, experiments, or review
branches. Changes reach it only by promoting already-verified `main`. The
promotion cadence is intentionally manual until a separate operating decision
sets a stronger rule.

To make a code change reach production staging, merging to `main` is not enough.
After the change is verified on `main`, promote `main` into the
`ta3000-production` branch and refresh the product staging containers. Until
that branch is updated, production staging continues to run the previous
`ta3000-production` code.

The production checkout and data root must stay separate. The scheduler may
create ordinary runtime/cache traces inside the checkout, but it must not use
the data root as a git checkout and must not clean these data-root folders:
`raw`, `canonical`, `research`, `staging`, `verification`, and
`moex-baseline-update`.

The production launcher pulls `origin/ta3000-production`, not `origin/main`, then
delegates to `scripts/run_ta3000_product_staging_bootstrap.cmd`. The launcher
must not run the MOEX baseline update with host Python. Dagster daemon inside
product staging owns the nightly data job through
`moex_baseline_daily_update_schedule`.

The retired `C:/Users/Admin/run_moex_nightly_backfill.cmd` launcher is kept for
forensic reference only. The target scheduler state is a direct action pointing
at `C:/Users/Admin/run_ta3000_production_nightly.cmd`; if local permissions
block that update, the old path may exist only as a compatibility shim that
delegates to the production launcher and does not run the retired route.

Operator details live in
`docs/runbooks/app/ta3000-production-nightly.md`.

## Guardrails
1. No direct main pushes by default.
2. No legacy gate aliases.
3. No manual bypass when enforced scripts exist.
4. No domain-specific logic inside shell control-plane files.
5. Product-plane work must stay inside isolated app paths unless a governance change is explicit.
6. Critical contour tasks must not claim target closure with scaffold, sample, smoke, or synthetic evidence.
7. Every PR must declare `shell`, `product-plane`, or `mixed`, with boundary rationale when `mixed`.
8. Semantic navigation should reduce context and risk. Serena is the mandatory first route for non-trivial code discovery, but not a heavy CI gate; fallback must be narrow and explicitly explained.
9. Broad architecture context belongs to Architecture Orientation Routing, not the general coding loop.

## Change management policy
1. One patch set equals one major governance concept.
2. High-risk surfaces follow ordered series: `contracts -> code -> docs`.
3. Repeated failure rule:
   - First failure: fix and retry.
   - Second failure: stop scope expansion and remediate process.

## Emergency policy
Direct-main emergency requires both:
- `AI_SHELL_EMERGENCY_MAIN_PUSH=1`
- `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON=<non-empty>`

Emergency path is for incident containment only.
