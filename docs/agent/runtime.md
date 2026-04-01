# Runtime Entrypoints

## Rule
Process automation is driven by canonical Python entrypoints and documented hooks.
Do not reintroduce shell-only wrapper flows as primary control paths.

## Lifecycle
- `python scripts/task_session.py begin --request "<request>"`
- `python scripts/task_session.py status`
- `python scripts/task_session.py end`
- `python scripts/codex_governed_bootstrap.py --request "<request>" ...`

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

## Governed Codex entrypoints
- `python scripts/codex_governed_bootstrap.py --request "<request>" --route auto`
- `python scripts/codex_governed_entry.py auto`
- `python scripts/codex_governed_entry.py package --package-path <zip>`
- `python scripts/codex_governed_entry.py continue --execution-contract <path> --parent-brief <path>`
- `python scripts/codex_governed_entry.py --route stacked-followup --execution-contract <path> --parent-brief <path> --predecessor-ref <merged-ref> --source-branch <split-branch> --new-base-ref origin/main --carry-surface <surface>`

## Governed route policy
- If the operator asks to take a package, continue a current module, or resume a governed phase, use `scripts/codex_governed_entry.py` first.
- A plain chat response without the governed launcher does not count as a valid governed run.
- For `continue` and `stacked-followup`, the valid path is the orchestrator route:
  - `worker`
  - `acceptance`
  - `remediation` when blocked
  - `unlock` only after `PASS`

## Route vocabulary contract
- Canonical declaration: `docs/codex/orchestration/route-snapshot-profile-contract.md`
- H0 status: contract terms are declared for route, snapshot, profile, and session lifecycle boundaries.
- H0 limitation: this declaration does not switch runtime behavior or validator enforcement yet.

## Dual-Mode Route/Session Semantics (H1)
- Governed entry now accepts dual-mode route/session metadata while keeping legacy invocation paths operational.
- Route mode:
  - `legacy` / `legacy-wrapper` keeps compatibility wrappers (including positional `auto|package|continue|stacked-followup` aliases).
  - `explicit` / `explicit-dual-mode` requires flag-based route invocation (`--route <value>`) and fails closed on positional alias use.
- Session mode:
  - `legacy-full` keeps existing lifecycle behavior.
  - `tracked_session` is explicit mode for the same governed lifecycle boundary.
  - Compatibility aliases `full` and `legacy` are accepted only at input boundaries and must be normalized to `legacy-full` in durable/session outputs.
  - `codex_governed_bootstrap.py` must fail closed when an active reused task session has a different mode than the requested `--session-mode`; no silent mode drift is allowed between bootstrap state, route state, and runtime output.
- Snapshot mode marker:
  - `route-report` (default) and explicit alternatives (`changed-files`, `phase-state`, `contract-only`) are persisted in governed route state.

Example:
- `python scripts/codex_governed_entry.py --route continue --route-mode explicit --session-mode tracked_session --snapshot-mode route-report --execution-contract <path> --parent-brief <path>`

## Loop/PR Metadata (H1)
- `run_loop_gate.py` and `run_pr_gate.py` now emit explicit `snapshot_mode` and `profile` markers in gate output.
- Optional profile marker:
  - `python scripts/run_loop_gate.py --from-git --git-ref HEAD --profile ops`
  - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --profile ops`

## CI / Proof Binding (H2)
- Hosted PR lane resolves contour-aware profile/check plan via:
  - `python scripts/run_surface_pr_matrix.py --plan-only ...`
- PR gate app contours run through:
  - `python scripts/run_surface_pr_matrix.py ...`
- Dependency installation is profile-aware:
  - runtime contour: `runtime-api` + `dev-test`
  - data contour: `runtime-api` + `data-proof` + `proof-docker` + `dev-test`
  - mixed contour: integration profile (runtime + data bundles)
- Docker-based governed proof scripts must use shared runtime contract utilities in `scripts/proof_runtime_contract.py` for path normalization, runtime-root validation, and writable-output fail-closed checks.

## Stacked Follow-Up / Recomposition (H3)
- Governed entry supports `--route stacked-followup` with explicit merge context and base contract fields:
  - `--predecessor-ref`
  - `--source-branch`
  - `--new-base-ref`
  - `--carry-surface`
  - optional `--temporary-downgrade-surface`
- Multi-module continuation now supports:
  - explicit `--module-slug <slug>`;
  - explicit `--module-priority phase-order|slug-lexical`;
  - machine-readable ambiguity report artifact when unresolved: `.runlogs/codex-governed-entry/module-ambiguity-report.json`.
- Stacked follow-up route emits continuation contract artifact by default:
  - `.runlogs/codex-governed-entry/stacked-followup-contract.json`
- When `--route stacked-followup` executes (not dry-run), the continuation contract is forwarded into orchestrator preflight/worker flow and persisted in orchestration state for route traceability.
- Truth recomposition helper/validator:
  - `python scripts/truth_recomposition.py build --followup-contract <path> --merged-surface <surface> --candidate-surface <surface> --output <path>`
  - `python scripts/truth_recomposition.py validate --report <path>`

## Hook runtime policy
- Main protection is implemented in `.githooks/pre-push`.
- Emergency override uses neutral variables:
  - `AI_SHELL_EMERGENCY_MAIN_PUSH`
  - `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON`

## Legacy policy
- Legacy gate aliases are not allowed.
