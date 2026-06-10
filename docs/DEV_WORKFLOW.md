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
