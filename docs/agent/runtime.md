# Runtime Entrypoints

## Rule
Process automation is driven by canonical Python entrypoints and documented hooks.
Do not reintroduce shell-only wrapper flows as primary control paths.

## Product Plane Delta Input Rule
For Product Plane analytical inputs, especially research and backtest inputs, Python is only the orchestration layer.
Do not load Delta-backed bars, indicators, derived indicators, or campaign result tables through Python row-object/list scans as the active data path.

The accepted read path is Delta-native first:
- apply Delta predicates before data reaches Python;
- project only the columns required by the strategy family or operator inspection;
- convert to Arrow/Pandas/vectorbt matrices only after Delta has filtered the table.

Allowed engines are the native Delta/Arrow/Spark read paths used by the Product Plane. Row-list helpers may exist for small metadata, tests, or historical compatibility, but they are not a fallback for the battle research/backtest contour.

## Validation
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_codeowners.py`
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`

## Gate entrypoints
- `python scripts/run_loop_gate.py ...`
- `python scripts/run_pr_gate.py ...`
- `python scripts/run_nightly_gate.py ...`
- `python scripts/compute_change_surface.py ...`
- `python scripts/sync_skills_catalog.py ...`

## Local MCP profile policy
- `.codex/config.toml` is ignored machine-local state, not a CI input.
- If local project MCP config exists, `profiles.base.servers` must be exactly
  `["serena"]`.
- GitHub, OpenAI docs, data stores, Dagster, graph, browser, office, plugin-eval,
  multi-agent, and memory MCP routes are explicit opt-in profiles or local
  enables only; they are not ordinary-chat defaults.

## Loop/PR Metadata
- `run_loop_gate.py` and `run_pr_gate.py` emit explicit `snapshot_mode` and `profile` markers in gate output.
- On policy-critical validation paths, missing markers now fail closed.
- Canonical explicit invocation:
  - `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
  - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- Profile override remains supported:
  - `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile ops`
  - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile ops`

## CI / Proof Binding
- Hosted PR lane resolves contour-aware profile/check plan via:
  - `python scripts/run_surface_pr_matrix.py --plan-only ...`
- PR gate app contours run through:
  - `python scripts/run_surface_pr_matrix.py ...`
- Dependency installation is profile-aware:
  - runtime contour: `runtime-api` + `dev-test`
  - data contour: `runtime-api` + `data-proof` + `proof-docker` + `dev-test`
  - mixed contour: integration profile (runtime + data bundles)
- Docker-based proof scripts must use shared runtime contract utilities in `scripts/proof_runtime_contract.py` for path normalization, runtime-root validation, and writable-output fail-closed checks.

## Recomposition
- Truth recomposition helper/validator:
  - `python scripts/truth_recomposition.py build --followup-contract <path> --merged-surface <surface> --candidate-surface <surface> --output <path>`
  - `python scripts/truth_recomposition.py validate --report <path>`

## Hook runtime policy
- Main protection is implemented in `.githooks/pre-push`.
- Codex app-level hooks are not part of the ordinary TA3000 route. Keep local
  Codex hook automation disabled unless a task explicitly restores a scoped
  hook with a matching test/invariant.
- Emergency override uses neutral variables:
  - `AI_SHELL_EMERGENCY_MAIN_PUSH`
  - `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON`

## Legacy policy
- Legacy gate aliases are not allowed.
