# Dashboard Refresh Workflow

## Purpose
Regenerate process reports and governance dashboard outside the hot loop.

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

