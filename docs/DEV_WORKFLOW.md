# Development Workflow (AI Delivery Shell)

## Objective
Provide a predictable and enforceable workflow for governance-first delivery in a repository that contains both shell and isolated product-plane surfaces.

## Canonical loop
1. Read hot docs (`docs/agent/*`).
2. Confirm the change surface (`shell`, `product-plane`, or mixed) and keep domain logic out of shell control-plane files.
3. Start task session: `python scripts/task_session.py begin --request "<request>"`.
4. Keep task contract current in active task note.
5. If the diff matches a critical contour, add `## Solution Intent` before coding and keep the declared class aligned with contour evidence.
6. Run loop gate: `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
7. Run PR gate before closeout.
8. Close lifecycle with `python scripts/task_session.py end` when outcome is terminal.

For product-plane tasks, read `docs/architecture/app/STATUS.md` before treating older phase-closure language as current truth.

## Gate order
`begin -> task note -> task contract validation -> loop gate -> pr gate -> nightly gate -> dashboard refresh -> end`

## CI lane model
1. `loop-lane` for fast surface-aware feedback.
2. `pr-lane` for closeout confidence and full QA matrix.
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
1. `python scripts/run_loop_gate.py --skip-session-check --from-git --git-ref HEAD`
2. `python scripts/run_pr_gate.py --skip-session-check --from-git --git-ref HEAD`
3. `python scripts/run_nightly_gate.py --from-git --git-ref HEAD`
4. `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md`

## Guardrails
1. No direct main pushes by default.
2. No legacy gate aliases.
3. No manual bypass when enforced scripts exist.
4. No domain-specific logic inside shell control-plane files.
5. Product-plane work must stay inside isolated app paths unless a governance change is explicit.
6. Critical contour tasks must not claim target closure with scaffold, sample, smoke, or synthetic evidence.

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
