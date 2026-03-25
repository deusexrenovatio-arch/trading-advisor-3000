# Dashboard Refresh Workflow

## Purpose
Regenerate process reports and governance dashboard outside the hot loop.

Hosted CI execution of this lane is gated by `AI_SHELL_ENABLE_HOSTED_CI=1`.
When hosted runners are unavailable, run this workflow locally.

## Inputs
- `plans/PLANS.yaml`
- `memory/agent_memory.yaml`
- `memory/task_outcomes.yaml`

## Commands
1. `python scripts/measure_dev_loop.py --format markdown --output artifacts/dev-loop-baseline.md`
2. `python scripts/harness_baseline_metrics.py --output artifacts/harness-baseline-metrics.json`
3. `python scripts/process_improvement_report.py --output artifacts/process-improvement-report.md`
4. `python scripts/autonomy_kpi_report.py --output artifacts/autonomy-kpi-report.md`
5. `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md`

## Outputs
- `artifacts/dev-loop-baseline.md`
- `artifacts/harness-baseline-metrics.json`
- `artifacts/process-improvement-report.md`
- `artifacts/autonomy-kpi-report.md`
- `artifacts/governance-dashboard.json`
- `artifacts/governance-dashboard.md`

## Pilot Observation Counters
- The governance dashboard includes a lightweight `pilot_observation` section sourced from task notes under `docs/tasks/`.
- Counters are intentionally limited to:
  - `critical tasks with explicit solution class`
  - `blocked shortcut claims`
  - `staged-vs-target declarations`
- Counter definitions are explicit and deterministic:
  - `critical tasks with explicit solution class`: task notes that declare both a valid `Solution Class` (`target|staged|fallback`) and a non-`none` `Critical Contour` in `## Solution Intent`.
  - `blocked shortcut claims`: the subset of those critical task notes where `Task Outcome` is `blocked` and the note text explicitly references `shortcut`.
  - `staged-vs-target declarations`: counts of `staged` and `target` classes among the same critical task notes.
- Pilot window guardrails are fail-closed:
  - `observation_window_start` is always the configured pilot start date.
  - notes dated before the configured pilot start are excluded from pilot-window counters.
- This stays a pilot aid and must not evolve into a standalone analytics subsystem.
