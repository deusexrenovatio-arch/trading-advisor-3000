# Layers v2

## L1 - Governance Contract Layer
Defines rules, ownership, and hot/warm/cold source-of-truth boundaries.

### Assets
- `AGENTS.md`
- `CODEOWNERS`
- `docs/agent/*`
- `docs/checklists/*`
- `docs/workflows/*`
- `docs/runbooks/*`

### Allowed dependencies
- May call L2 entrypoints only through documented scripts.

## L2 - Runtime Orchestration Layer
Executes task lifecycle, context routing, and gate orchestration.

### Assets
- `scripts/task_session.py`
- `scripts/context_router.py`
- `scripts/compute_change_surface.py`
- `scripts/run_loop_gate.py`
- `scripts/run_pr_gate.py`
- `scripts/run_nightly_gate.py`

### Allowed dependencies
- Reads L1 policy.
- Writes to L3 state and L5 reporting outputs.

## L3 - Durable State Layer
Stores canonical process records and compatibility outputs.

### Assets
- `plans/items/*`
- `plans/PLANS.yaml`
- `memory/decisions/*`
- `memory/incidents/*`
- `memory/patterns/*`
- `memory/agent_memory.yaml`
- `memory/task_outcomes.yaml`

### Allowed dependencies
- Read/write from L2 orchestration and L4 validators.

## L4 - Validation Layer
Contains validators and tests that protect shell contracts.

### Assets
- `scripts/validate_*.py`
- `tests/process/*`
- `tests/architecture/*`
- `tests/product-plane/*`

### Allowed dependencies
- Reads L1-L3 definitions and verifies consistency.

## L5 - Reporting and Governance Analytics Layer
Builds measurable evidence for process health and regressions.

### Assets
- `scripts/measure_dev_loop.py`
- `scripts/agent_process_telemetry.py`
- `scripts/harness_baseline_metrics.py`
- `scripts/process_improvement_report.py`
- `scripts/autonomy_kpi_report.py`
- `scripts/build_governance_dashboard.py`
- `artifacts/*`

### Allowed dependencies
- Aggregates data from L3 and signals from L4.

## L6 - Application Plane Layer
Holds isolated application/product-plane code, contracts, and app-facing runtime surfaces.

### Assets
- `src/trading_advisor_3000/*`

### Allowed dependencies
- Must not import governance runtime modules directly.

