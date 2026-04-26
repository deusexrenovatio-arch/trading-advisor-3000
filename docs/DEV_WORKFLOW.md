# Development Workflow (Dual-Surface Delivery)

## Objective
Provide a predictable and enforceable workflow for governance-first delivery in a repository that contains both shell and isolated product-plane surfaces.

## Canonical loop
1. Read hot docs (`docs/agent/*`).
2. Confirm the change surface (`shell`, `product-plane`, or `mixed`), keep domain logic out of shell control-plane files, and keep the same surface declaration in task note + PR.
3. For non-trivial code changes or new code inside an existing subsystem, start with Serena for code discovery, local pattern learning, impact analysis, and reference checks before broad scans, whole-file reads, or implementation.
4. For architecture-heavy, cross-module, ownership-sensitive, or concept-location uncertain code tasks, follow Architecture Orientation Routing in `docs/agent/skills-routing.md`.
5. Start task session: `python scripts/task_session.py begin --request "<request>"`.
6. Keep task contract current in active task note.
7. If the diff matches a critical contour, add `## Solution Intent` before coding and keep the declared class aligned with contour evidence.
8. Run loop gate: `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.
9. Run PR gate before closeout.
10. Close lifecycle with `python scripts/task_session.py end` when outcome is terminal.

For product-plane tasks, read `docs/architecture/product-plane/STATUS.md` before treating older phase-closure language as current truth.

## Gate order
`begin -> task note -> task contract validation -> loop gate -> pr gate -> nightly gate -> dashboard refresh -> end`

## CI lane model
1. `loop-lane` for fast surface-aware feedback.
2. `pr-lane` for closeout confidence with contour-aware dependency/test selection.
   - planner: `python scripts/run_surface_pr_matrix.py --plan-only ...`
   - executor: `python scripts/run_surface_pr_matrix.py ...`
3. `nightly-lane` for hygiene, telemetry, and report generation.
4. `dashboard-refresh` for deterministic report/dashboard rebuild.
5. Hosted lane execution is opt-in via repository variable:
   - `AI_SHELL_ENABLE_HOSTED_CI=1`
   - default is disabled to avoid false-red checks when hosted runners are unavailable.

Main merge requirement:
- GitHub protection for `main` must require successful `loop-lane` and `pr-lane` before merge.
- `nightly-lane` and `dashboard-refresh` are non-merge lanes and remain post-PR hygiene/report lanes.

## Hosted CI fallback
If hosted runners are unavailable, replay lanes locally for acceptance evidence:
1. `python scripts/run_loop_gate.py --skip-session-check --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
2. `python scripts/run_surface_pr_matrix.py --plan-only --from-git --git-ref HEAD --output-json artifacts/ci/pr-surface-plan.json --summary-file artifacts/ci/pr-surface-plan.md`
3. `python scripts/run_pr_gate.py --skip-session-check --from-git --git-ref HEAD --snapshot-mode changed-files --profile runtime-api --summary-file artifacts/ci/pr-gate-summary.md`
4. `python scripts/run_nightly_gate.py --from-git --git-ref HEAD`
5. `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md`

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
