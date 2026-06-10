# Governance Remediation Guide

Use this runbook when a governance gate fails.

## `python scripts/run_loop_gate.py --snapshot-mode changed-files --profile none`
- Read reported `primary_surface` and failing command.
- On policy-critical surfaces, pass explicit marker contract:
  - `--snapshot-mode changed-files`
  - `--profile none` (or explicit profile id)
- Re-run `python scripts/compute_change_surface.py --from-git --git-ref HEAD --format text`.
- Fix scoped validator/test and rerun loop gate.

## `python scripts/run_pr_gate.py --snapshot-mode changed-files --profile none`
- Ensure loop gate passes first.
- Keep explicit marker contract in PR closeout:
  - `--snapshot-mode changed-files`
  - `--profile none` (or explicit profile id)
- Fix PR-only command failures and rerun.

## `python scripts/run_nightly_gate.py`
- Ensure PR gate passes first.
- Fix nightly-only drift/hygiene failures and rerun.

## `python scripts/nightly_root_hygiene.py`
- Validate root layout, required files, and no legacy gate aliases.
- Fix repository hygiene before retrying nightly lane.

## `python scripts/measure_dev_loop.py`
- Regenerate dev-loop baseline when outcome ledger changed.
- Use markdown output for human review and JSON output for dashboards.

## `python scripts/harness_baseline_metrics.py`
- Rebuild baseline inventory for plans/memory/process rollups.
- Keep artifact path stable for dashboard refresh lane.

## `python scripts/build_governance_dashboard.py`
- Regenerate dashboard JSON and markdown.
- Use after nightly lane or when governance ledgers changed.

## `python scripts/validate_solution_intent.py`
- If a diff matches `configs/critical_contours.yaml`, declare Solution Intent before coding.
- Declare `Solution Class`, `Critical Contour`, `Forbidden Shortcuts`, `Closure Evidence`, and `Shortcut Waiver`.
- Split the patch if multiple critical contours are triggered at once.

## `python scripts/validate_critical_contour_closure.py`
- Keep target and staged claims backed by contour-specific evidence.
- Do not use fixture paths, sample artifacts, smoke-only proofs, or synthetic publication paths as closure evidence.
- If the patch is intentionally non-target, downgrade to `fallback` and state the waiver explicitly.

## `python scripts/validate_pr_only_policy.py`
- Keep direct push to `main` blocked by default.
- Keep emergency override variables neutral:
  - `AI_SHELL_EMERGENCY_MAIN_PUSH`
  - `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON`
- Keep GitHub server-side protection active for `main`.
- Required GitHub merge checks for `main` are:
  - `pr-lane`
  - `CodeRabbit`
- `pr-lane` is a PR-only context. Branch pushes and manual diagnostic runs use `branch-lane` so push-range failures do not block a green PR-range merge gate.
- `CodeRabbit` is a required external review status so automatic integration cannot skip it.
- `pr-lane` must include `python scripts/validate_pr_size.py` through `run_pr_gate.py`.
- Public GitHub repositories can validate rules anonymously; private repositories require `GH_TOKEN` or `GITHUB_TOKEN`.

## `python scripts/validate_plans.py`
- Keep `plans/items/` canonical and structurally valid.
- Regenerate compatibility output: `plans/PLANS.yaml`.

## `python scripts/validate_agent_memory.py`
- Keep memory entries typed by section and unique by id.
- Ensure dates are valid ISO format.

## `python scripts/validate_process_regressions.py`
- If the rolling process gate fails, inspect `memory/task_outcomes.yaml` first.
- Use the process report to identify recent non-first-time tasks before changing thresholds.
- Keep `burn_in_min_completed_tasks` explicit when the ledger has not reached a stable sample size.
- Do not rewrite task outcomes just to satisfy the metric; fix the process or add a tracked remediation task.

## `python scripts/validate_architecture_policy.py`
- Ensure required architecture package files exist.
- Keep architecture map in mermaid format and ADR baseline present.

## `python scripts/skill_precommit_gate.py`
- Use when changing `.codex/skills/*` or removing legacy `.cursor/skills/*`.
- Update skill catalog/routing docs if gate reports `update_required`.

## Hosted CI not starting (billing/spending disabled)
- Keep PR CI defined in `.github/workflows/ci.yml`; branch diagnostics, nightly, and dashboard refresh live in separate workflow files.
- Enable hosted execution only with repository variable:
  - `AI_SHELL_ENABLE_HOSTED_CI=1`
- If hosted runners are unavailable, collect lane evidence locally:
  - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
  - `python scripts/run_nightly_gate.py --from-git --git-ref HEAD`
  - `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md`
