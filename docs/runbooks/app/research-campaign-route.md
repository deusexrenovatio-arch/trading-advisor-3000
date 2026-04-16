# Research Campaign Route

## Purpose
This runbook defines the only supported user-facing route for Product Plane research runs.

Official command:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>
```

The runner is a thin Product Plane front door.
It does not compute indicators, features, or backtests by itself.
It validates a machine-readable campaign config, writes immutable run artifacts, and dispatches only into the Dagster `phase2b` contour.

## Contracts

Input contract:
- `src/trading_advisor_3000/product_plane/contracts/schemas/research_campaign.v1.json`

Run output contract:
- `src/trading_advisor_3000/product_plane/contracts/schemas/research_run_summary.v1.json`

## Storage Model

Recommended committed configs should point to external-first storage under `D:/TA3000-data`.

Storage split:
- reusable materialized layer: `<materialized_root>/<materialization_key>/`
- immutable per-run artifacts: `<runs_root>/<campaign_name>/<run_id>/`

The same compatible campaign can reuse a materialized layer.
Each execution still gets a fresh `run_id` and a fresh immutable run folder.

## Stage Selection

The route is selected strictly by `target_stage` in the campaign config:
- `bootstrap` -> materialize reusable dataset, indicator, and feature layers
- `backtest` -> reuse or rebuild bootstrap layer, then run backtests and rankings
- `projection` -> run the full route through candidate projection

## Run Artifacts

Each run folder contains:
- `campaign.lock.json`
- `status.json`
- `run-summary.json`
- `artifacts-index.json`
- `logs/stdout.log`
- `logs/stderr.log`

Status transitions are:
- `queued`
- `running`
- terminal `success | failed | blocked`

## Internal / Debug Only

These entrypoints remain in the repository for diagnostics and development, but they are internal/debug-only and not the supported operational route:
- `python -m trading_advisor_3000.product_plane.research.jobs.bootstrap`
- `python -m trading_advisor_3000.product_plane.research.jobs.backtest`
- `python -m trading_advisor_3000.product_plane.research.jobs.project_candidates`
- `python -m trading_advisor_3000.product_plane.research.jobs.benchmark`
- `trading_advisor_3000.product_plane.research.run_research_from_bars(...)`

Use them only when debugging or working on implementation details.
Operational research campaigns should start from `run_campaign`.
